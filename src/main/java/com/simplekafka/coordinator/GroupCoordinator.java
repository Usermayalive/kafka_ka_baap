package com.simplekafka.coordinator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Broker-side consumer group coordination with cooperative (incremental) rebalancing and
 * broker-computed assignments, in the spirit of Kafka's KIP-848 consumer protocol.
 *
 * <ul>
 *   <li>Membership changes (join, leave, session expiry, subscription change) bump the
 *       generation and recompute a sticky, balanced <em>target</em> assignment.</li>
 *   <li>Cooperative handoff: a member is only granted a partition once no other member reports
 *       owning it. Members keep untouched partitions through a rebalance; only moved partitions
 *       pause, and only between the old owner's revoke and the new owner's next sync.</li>
 *   <li>Static membership (default in the client): a member rejoining with the same instance
 *       ID within its session timeout takes over the previous member's identity and assignment
 *       without a rebalance, so rolling restarts don't reshuffle partitions.</li>
 * </ul>
 *
 * Client loop: JOIN once, then HEARTBEAT every interval reporting owned partitions; when the
 * heartbeat says {@code needsSync}, SYNC to get the currently grantable assignment, revoke what
 * is no longer assigned (committing offsets first), and start what is newly granted.
 */
public class GroupCoordinator {
    private static final Logger LOGGER = Logger.getLogger(GroupCoordinator.class.getName());

    public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {
        @Override
        public int compareTo(TopicPartition o) {
            int c = topic.compareTo(o.topic);
            return c != 0 ? c : Integer.compare(partition, o.partition);
        }
    }

    public record JoinResult(String memberId, int generation) {}

    public record SyncResult(int generation, List<TopicPartition> assignment) {}

    public record HeartbeatResult(int generation, boolean needsSync) {}

    public static class UnknownMemberException extends Exception {
        public UnknownMemberException(String message) {
            super(message);
        }
    }

    private static final class Member {
        String memberId;
        final String instanceId; // null for dynamic members
        Set<String> subscription;
        final long sessionTimeoutMs;
        volatile long lastSeenNanos;
        Set<TopicPartition> owned = new TreeSet<>();

        Member(String memberId, String instanceId, Set<String> subscription, long sessionTimeoutMs) {
            this.memberId = memberId;
            this.instanceId = instanceId;
            this.subscription = subscription;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.lastSeenNanos = System.nanoTime();
        }
    }

    private static final class Group {
        final String groupId;
        final Map<String, Member> members = new LinkedHashMap<>();
        int generation;
        Map<TopicPartition, String> target = new TreeMap<>(); // partition -> memberId

        Group(String groupId) {
            this.groupId = groupId;
        }
    }

    /** Partition count per topic, from cluster metadata (0 or absent if the topic doesn't exist). */
    private final Function<String, Integer> partitionCount;
    private final Map<String, Group> groups = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "group-session-reaper");
        t.setDaemon(true);
        return t;
    });

    /** Receives lifecycle events (event name, details), e.g. for the audit log. */
    public interface GroupEventListener {
        void onEvent(String event, Map<String, Object> details);
    }

    private final GroupEventListener listener;

    public GroupCoordinator(Function<String, Integer> partitionCount) {
        this(partitionCount, (event, details) -> {});
    }

    public GroupCoordinator(Function<String, Integer> partitionCount, GroupEventListener listener) {
        this.partitionCount = partitionCount;
        this.listener = listener;
        reaper.scheduleWithFixedDelay(this::expireSessions, 100, 100, TimeUnit.MILLISECONDS);
    }

    public JoinResult join(String groupId, String memberId, String instanceId, Set<String> topics, long sessionTimeoutMs) {
        Group group = groups.computeIfAbsent(groupId, Group::new);
        synchronized (group) {
            Member existing = memberId == null || memberId.isEmpty() ? null : group.members.get(memberId);
            if (existing == null && instanceId != null && !instanceId.isEmpty()) {
                existing = findByInstance(group, instanceId);
                if (existing != null) {
                    // Static member restarted: new member ID, same slot and assignment, no rebalance
                    String newId = instanceId + "-" + UUID.randomUUID();
                    group.members.remove(existing.memberId);
                    replaceInTarget(group, existing.memberId, newId);
                    existing.memberId = newId;
                    existing.owned = new TreeSet<>(); // the restarted process owns nothing yet
                    group.members.put(newId, existing);
                    LOGGER.info("Group " + groupId + ": static member " + instanceId + " rejoined as " + newId);
                    listener.onEvent("GROUP_STATIC_MEMBER_REJOINED", Map.of("group", groupId, "instance", instanceId,
                            "member", newId, "generation", group.generation));
                }
            }
            if (existing != null) {
                existing.lastSeenNanos = System.nanoTime();
                if (!existing.subscription.equals(topics)) {
                    existing.subscription = new TreeSet<>(topics);
                    rebalance(group, "subscription change by " + existing.memberId);
                }
                return new JoinResult(existing.memberId, group.generation);
            }
            String newId = (instanceId != null && !instanceId.isEmpty() ? instanceId : "member") + "-" + UUID.randomUUID();
            group.members.put(newId, new Member(newId, emptyToNull(instanceId), new TreeSet<>(topics), sessionTimeoutMs));
            listener.onEvent("GROUP_MEMBER_JOINED", Map.of("group", groupId, "member", newId, "topics", List.copyOf(topics)));
            rebalance(group, newId + " joined");
            return new JoinResult(newId, group.generation);
        }
    }

    /** Returns the member's target partitions that are free to take (no other member owns them). */
    public SyncResult sync(String groupId, String memberId, Set<TopicPartition> owned) throws UnknownMemberException {
        Group group = groupOf(groupId, memberId);
        synchronized (group) {
            Member member = memberOf(group, memberId);
            member.lastSeenNanos = System.nanoTime();
            member.owned = new TreeSet<>(owned);
            refreshTargetIfTopicsChanged(group);
            return new SyncResult(group.generation, grantable(group, member));
        }
    }

    public HeartbeatResult heartbeat(String groupId, String memberId, Set<TopicPartition> owned) throws UnknownMemberException {
        Group group = groupOf(groupId, memberId);
        synchronized (group) {
            Member member = memberOf(group, memberId);
            member.lastSeenNanos = System.nanoTime();
            member.owned = new TreeSet<>(owned);
            refreshTargetIfTopicsChanged(group);
            // Sync when the member holds something it shouldn't, or could take something it should
            boolean needsSync = !new TreeSet<>(grantable(group, member)).equals(member.owned);
            return new HeartbeatResult(group.generation, needsSync);
        }
    }

    public void leave(String groupId, String memberId) {
        Group group = groups.get(groupId);
        if (group == null) {
            return;
        }
        synchronized (group) {
            if (group.members.remove(memberId) != null) {
                listener.onEvent("GROUP_MEMBER_LEFT", Map.of("group", groupId, "member", memberId));
                rebalance(group, memberId + " left");
            }
        }
    }

    /** Current target assignment (for admin/observability). */
    public Map<String, List<TopicPartition>> describe(String groupId) {
        Group group = groups.get(groupId);
        Map<String, List<TopicPartition>> result = new TreeMap<>();
        if (group == null) {
            return result;
        }
        synchronized (group) {
            for (Member m : group.members.values()) {
                result.put(m.memberId, new ArrayList<>());
            }
            group.target.forEach((tp, owner) -> result.computeIfAbsent(owner, k -> new ArrayList<>()).add(tp));
        }
        return result;
    }

    public Set<String> groupIds() {
        return new TreeSet<>(groups.keySet());
    }

    public int generation(String groupId) {
        Group group = groups.get(groupId);
        return group == null ? 0 : group.generation;
    }

    // ------------------------------------------------------------------------------------

    private void expireSessions() {
        long now = System.nanoTime();
        for (Group group : groups.values()) {
            synchronized (group) {
                List<String> expired = new ArrayList<>();
                for (Member m : group.members.values()) {
                    if (now - m.lastSeenNanos > TimeUnit.MILLISECONDS.toNanos(m.sessionTimeoutMs)) {
                        expired.add(m.memberId);
                    }
                }
                if (!expired.isEmpty()) {
                    expired.forEach(group.members::remove);
                    listener.onEvent("GROUP_MEMBERS_EXPIRED", Map.of("group", group.groupId, "members", expired));
                    rebalance(group, "session timeout of " + expired);
                }
            }
        }
    }

    private void rebalance(Group group, String reason) {
        group.generation++;
        group.target = computeTarget(group);
        listener.onEvent("GROUP_REBALANCED", Map.of("group", group.groupId, "generation", group.generation,
                "reason", reason, "members", group.members.size(), "partitions", group.target.size()));
        LOGGER.info("Group " + group.groupId + " generation " + group.generation + " (" + reason + "): " + group.target);
    }

    /** Topics can gain partitions or be created after members subscribed. */
    private void refreshTargetIfTopicsChanged(Group group) {
        Set<TopicPartition> current = allPartitions(group);
        if (!current.equals(group.target.keySet())) {
            rebalance(group, "topic metadata changed");
        }
    }

    private Set<TopicPartition> allPartitions(Group group) {
        Set<String> topics = new TreeSet<>();
        group.members.values().forEach(m -> topics.addAll(m.subscription));
        Set<TopicPartition> partitions = new TreeSet<>();
        for (String topic : topics) {
            Integer count = partitionCount.apply(topic);
            for (int p = 0; count != null && p < count; p++) {
                partitions.add(new TopicPartition(topic, p));
            }
        }
        return partitions;
    }

    /**
     * Sticky balanced assignment: keep valid previous owners, give orphans to the least-loaded
     * eligible member, then move partitions from the most- to the least-loaded member until no
     * eligible move narrows the gap.
     */
    private Map<TopicPartition, String> computeTarget(Group group) {
        Map<String, TreeSet<TopicPartition>> load = new LinkedHashMap<>();
        group.members.keySet().stream().sorted().forEach(id -> load.put(id, new TreeSet<>()));
        Map<TopicPartition, String> target = new TreeMap<>();
        for (TopicPartition tp : allPartitions(group)) {
            String previous = group.target.get(tp);
            if (previous != null && eligible(group, previous, tp)) {
                target.put(tp, previous);
                load.get(previous).add(tp);
            }
        }
        for (TopicPartition tp : allPartitions(group)) {
            if (!target.containsKey(tp)) {
                String least = leastLoaded(group, load, tp);
                if (least != null) {
                    target.put(tp, least);
                    load.get(least).add(tp);
                }
            }
        }
        boolean moved = true;
        while (moved) {
            moved = false;
            for (String from : sortedByLoad(load, true)) {
                for (TopicPartition tp : new ArrayList<>(load.get(from))) {
                    String to = leastLoaded(group, load, tp);
                    if (to != null && load.get(from).size() - load.get(to).size() > 1) {
                        load.get(from).remove(tp);
                        load.get(to).add(tp);
                        target.put(tp, to);
                        moved = true;
                        break;
                    }
                }
                if (moved) {
                    break;
                }
            }
        }
        return target;
    }

    private static List<String> sortedByLoad(Map<String, TreeSet<TopicPartition>> load, boolean descending) {
        List<String> ids = new ArrayList<>(load.keySet());
        Comparator<String> bySize = Comparator.comparingInt(id -> load.get(id).size());
        ids.sort(descending ? bySize.reversed().thenComparing(Comparator.naturalOrder()) : bySize.thenComparing(Comparator.naturalOrder()));
        return ids;
    }

    private String leastLoaded(Group group, Map<String, TreeSet<TopicPartition>> load, TopicPartition tp) {
        for (String id : sortedByLoad(load, false)) {
            if (eligible(group, id, tp)) {
                return id;
            }
        }
        return null;
    }

    private static boolean eligible(Group group, String memberId, TopicPartition tp) {
        Member m = group.members.get(memberId);
        return m != null && m.subscription.contains(tp.topic());
    }

    private static List<TopicPartition> grantable(Group group, Member member) {
        List<TopicPartition> result = new ArrayList<>();
        for (Map.Entry<TopicPartition, String> e : group.target.entrySet()) {
            if (!e.getValue().equals(member.memberId)) {
                continue;
            }
            boolean ownedByOther = false;
            for (Member other : group.members.values()) {
                if (other != member && other.owned.contains(e.getKey())) {
                    ownedByOther = true;
                    break;
                }
            }
            if (!ownedByOther) {
                result.add(e.getKey());
            }
        }
        return result;
    }

    private static void replaceInTarget(Group group, String oldId, String newId) {
        group.target.replaceAll((tp, owner) -> owner.equals(oldId) ? newId : owner);
    }

    private static Member findByInstance(Group group, String instanceId) {
        for (Member m : group.members.values()) {
            if (instanceId.equals(m.instanceId)) {
                return m;
            }
        }
        return null;
    }

    private Group groupOf(String groupId, String memberId) throws UnknownMemberException {
        Group group = groups.get(groupId);
        if (group == null) {
            throw new UnknownMemberException("Unknown group " + groupId + " (member " + memberId + ")");
        }
        return group;
    }

    private static Member memberOf(Group group, String memberId) throws UnknownMemberException {
        Member member = group.members.get(memberId);
        if (member == null) {
            throw new UnknownMemberException("Member " + memberId + " is not in group " + group.groupId
                    + " (expired or replaced); rejoin");
        }
        return member;
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    public void close() {
        reaper.shutdownNow();
    }
}

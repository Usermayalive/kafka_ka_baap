#!/usr/bin/env bash
# End-to-end demo: a 3-broker StormLog cluster with an embedded Raft metadata quorum.
# Creates a topic, produces and consumes, kill -9's the controller, shows failover and the
# offline partition, restarts the broker and shows its data back online. No Docker needed.
#
#   ./scripts/demo-cluster.sh            (builds the jar if missing)
set -euo pipefail
cd "$(dirname "$0")/.."

JAR=target-cli/simple-kafka-1.0-SNAPSHOT.jar
DEMO_DIR=${DEMO_DIR:-demo-data}
HOST=127.0.0.1
BROKER_PORTS=(19092 19093 19094)
RAFT_PORTS=(19191 19192 19193)
QUORUM="1@$HOST:${RAFT_PORTS[0]},2@$HOST:${RAFT_PORTS[1]},3@$HOST:${RAFT_PORTS[2]}"
PIDS=()

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
cli() { java -cp "$JAR" com.simplekafka.cli.StormLogCli --bootstrap "$HOST:$1" "${@:2}" 2>/dev/null; }
admin() { curl -s "http://$HOST:$(( $1 + 1000 ))$2"; }

start_broker() {
  local id=$1 idx=$(( $1 - 1 ))
  STORMLOG_QUORUM="$QUORUM" java --enable-native-access=ALL-UNNAMED -Dstormlog.data.dir="$DEMO_DIR" \
    -jar "$JAR" "$id" "$HOST" "${BROKER_PORTS[$idx]}" "${RAFT_PORTS[$idx]}" > "$DEMO_DIR/broker-$id.log" 2>&1 &
  PIDS[$idx]=$!
}

cleanup() { for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

wait_for() { # wait_for DESCRIPTION SECONDS COMMAND...
  local what=$1 secs=$2; shift 2
  for _ in $(seq 1 $(( secs * 4 ))); do "$@" >/dev/null 2>&1 && return 0; sleep 0.25; done
  echo "Timed out waiting for: $what" >&2; exit 1
}
quorum_has_leader() { admin "$1" /admin/quorum | grep -q '"leader":[1-9]'; }
controller_id() { admin "$1" /admin/quorum | sed -E 's/.*"leader":([0-9-]+).*/\1/'; }

[ -f "$JAR" ] || { step "Building $JAR"; ./mvnw -q -o package -Dbuild.dir=target-cli -DskipTests; }
rm -rf "$DEMO_DIR" && mkdir -p "$DEMO_DIR"

step "Starting 3 brokers (Raft quorum $QUORUM)"
for id in 1 2 3; do start_broker "$id"; done
wait_for "metadata leader" 60 quorum_has_leader "${BROKER_PORTS[0]}"
admin "${BROKER_PORTS[0]}" /admin/quorum; echo

step "Creating topic 'orders' with 3 partitions (one per broker)"
cli "${BROKER_PORTS[0]}" create-topic orders 3

step "Producing 30 messages"
cli "${BROKER_PORTS[0]}" produce-batch orders 30
cli "${BROKER_PORTS[0]}" consume orders 0 | head -3; echo "  ..."

step "Consumer group 'billing' reads everything and commits"
cli "${BROKER_PORTS[0]}" consume-group billing orders 3 | tail -1

CONTROLLER=$(controller_id "${BROKER_PORTS[0]}")
SURVIVOR_PORT=${BROKER_PORTS[0]}; [ "$CONTROLLER" = "1" ] && SURVIVOR_PORT=${BROKER_PORTS[1]}
step "kill -9 broker $CONTROLLER (the controller AND a partition leader)"
kill -9 "${PIDS[$(( CONTROLLER - 1 ))]}"
PIDS[$(( CONTROLLER - 1 ))]=""
wait_for "new controller" 30 bash -c "curl -s http://$HOST:$(( SURVIVOR_PORT + 1000 ))/admin/quorum | grep -q '\"leader\":[1-9]' && ! curl -s http://$HOST:$(( SURVIVOR_PORT + 1000 ))/admin/quorum | grep -q '\"leader\":$CONTROLLER,'"
admin "$SURVIVOR_PORT" /admin/quorum; echo
sleep 4 # session timeout (3s) -> the dead broker is unregistered and its partition marked offline
cli "$SURVIVOR_PORT" describe

step "Producing 9 more messages: the offline partition fails, the others keep working"
cli "$SURVIVOR_PORT" produce-batch orders 9 || true

step "Restarting broker $CONTROLLER: it re-registers and is re-elected leader of its partition"
start_broker "$CONTROLLER"
sleep 8
cli "$SURVIVOR_PORT" describe
for p in 0 1 2; do
  echo "  orders-$p: $(cli "$SURVIVOR_PORT" consume orders $p | wc -l | tr -d ' ') messages"
done

step "Observability: committed-offset lag (reported by the group's coordinator, for partitions it hosts)"
for port in "${BROKER_PORTS[@]}"; do
  lag=$(admin "$port" "/admin/lag?group=billing" | tr -d '\n ')
  [ "$lag" != "[]" ] && [ -n "$lag" ] && echo "broker@$port: $lag"
done
step "Metrics on broker at port $SURVIVOR_PORT"
admin "$SURVIVOR_PORT" /metrics | grep -E '^stormlog_(topic_records_in_total|request_latency_seconds_count\{api="produce")' | head -5

step "Done. Broker logs: $DEMO_DIR/broker-*.log"

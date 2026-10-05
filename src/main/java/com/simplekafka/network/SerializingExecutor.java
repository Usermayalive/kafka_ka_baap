package com.simplekafka.network;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ensures strict FIFO sequential execution of tasks for a given connection/channel
 * while offloading the actual execution to a shared worker pool (requestExecutionPool).
 *
 * GUARANTEES:
 * 1. Pipelined requests on a single channel are processed in exact arrival order.
 * 2. Event-loop threads never block on disk I/O.
 * 3. Different channels execute concurrently in parallel across the shared worker pool threads.
 * 4. A task whose execute() throws RejectedExecutionException never runs, so the caller can
 *    answer it with an error without the task also producing a response later.
 *
 * The shared pool holds at most one runner per channel, so its queue depth does not reflect a
 * single connection's backlog; callers apply per-connection backpressure via getQueueDepth().
 */
public class SerializingExecutor implements Executor {
    private final Executor delegate;
    private final Queue<ClaimableTask> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    // Tasks queued but not yet started; ConcurrentLinkedQueue.size() is O(n)
    private final AtomicInteger pending = new AtomicInteger();

    public SerializingExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    /** Runs at most once: either a runner claims it to execute, or a rejected execute() claims it to withdraw. */
    private static final class ClaimableTask {
        private final Runnable task;
        private final AtomicBoolean claimed = new AtomicBoolean(false);

        ClaimableTask(Runnable task) {
            this.task = task;
        }

        boolean claim() {
            return claimed.compareAndSet(false, true);
        }
    }

    @Override
    public void execute(Runnable task) {
        ClaimableTask entry = new ClaimableTask(task);
        pending.incrementAndGet();
        queue.add(entry);
        try {
            scheduleNext();
        } catch (RejectedExecutionException e) {
            // A concurrently finishing runner may already have picked the task up; if so it
            // will respond, and reporting the rejection too would send a second response.
            if (entry.claim()) {
                queue.remove(entry);
                pending.decrementAndGet();
                throw e;
            }
        }
    }

    private void scheduleNext() {
        if (!isRunning.get() && isRunning.compareAndSet(false, true)) {
            try {
                delegate.execute(this::runTasks);
            } catch (RejectedExecutionException e) {
                isRunning.set(false);
                throw e;
            }
        }
    }

    private void runTasks() {
        while (true) {
            ClaimableTask entry;
            while ((entry = queue.poll()) != null) {
                if (!entry.claim()) {
                    continue; // withdrawn by a rejected execute()
                }
                pending.decrementAndGet();
                try {
                    entry.task.run();
                } catch (Throwable t) {
                    // Task errors are handled internally by task.run()
                }
            }

            isRunning.set(false);
            if (queue.isEmpty() || !isRunning.compareAndSet(false, true)) {
                return;
            }
            try {
                // Yield this worker so other channels get a turn
                delegate.execute(this::runTasks);
                return;
            } catch (RejectedExecutionException e) {
                // Pool saturated: keep draining on this thread rather than stranding queued tasks
            }
        }
    }

    public int getQueueDepth() {
        return pending.get();
    }
}

package com.simplekafka.network;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ensures strict FIFO sequential execution of tasks for a given connection/channel
 * while offloading the actual execution to a shared worker pool (requestExecutionPool).
 *
 * GUARANTEES:
 * 1. Pipelined requests on a single channel are processed in exact arrival order.
 * 2. Event-loop threads never block on disk I/O.
 * 3. Different channels execute concurrently in parallel across the shared worker pool threads.
 */
public class SerializingExecutor implements Executor {
    private final Executor delegate;
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public SerializingExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Runnable task) {
        queue.add(task);
        scheduleNext();
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
        try {
            Runnable task;
            while ((task = queue.poll()) != null) {
                try {
                    task.run();
                } catch (Throwable t) {
                    // Task errors are handled internally by task.run()
                }
            }
        } finally {
            isRunning.set(false);
            if (!queue.isEmpty()) {
                scheduleNext();
            }
        }
    }

    public int getQueueDepth() {
        return queue.size();
    }
}

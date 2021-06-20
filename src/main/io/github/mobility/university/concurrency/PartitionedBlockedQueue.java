package io.github.mobility.university.concurrency;

import net.jcip.annotations.ThreadSafe;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This queue allows to process multi threaded streams in order for each partition.
 * This allows to execute timely-expansive operations between acquire and release.
 * Partitioning is based on Key::hashcode.
 *
 * @param <Key> the key which should be used for partitioning
 * @param <Value> the value which should be queued
 */
@ThreadSafe
public class PartitionedBlockedQueue<Key, Value> {
    private final int numberOfPartitions;
    private final List<Semaphore> semaphores;
    private final List<Deque<Value>> queues;

    public PartitionedBlockedQueue(int numberOfPartitions) {
        assert numberOfPartitions > 0 : "need to have at least a single partition";

        this.numberOfPartitions = numberOfPartitions;
        this.semaphores = IntStream.range(0, numberOfPartitions).boxed().map(i -> new Semaphore(1)).collect(Collectors.toUnmodifiableList());
        this.queues = IntStream.range(0, numberOfPartitions).boxed().map(i -> new LinkedList<Value>()).collect(Collectors.toUnmodifiableList());
    }

    public Value acquire(Key key, Value value) throws InterruptedException {
        var idx = toHash(key);
        var semaphore = semaphores.get(idx);
        var queue = queues.get(idx);
        synchronized (semaphore) {
            queue.add(value);
        }
        semaphore.acquire();
        synchronized (semaphore) {
            assert !queue.isEmpty() : "forgot to add before acquire";

            return queue.poll();
        }
    }

    public void release(Key key) {
        with(semaphores.get(toHash(key)), semaphore -> {
            synchronized (semaphore) {
                semaphore.release();
            }
        });
    }

    private int toHash(Key key) {
        return Math.abs(key.hashCode()) % this.numberOfPartitions;
    }

    private static <T> void with(T obj, Consumer<T> c) {
        c.accept(obj);
    }
}

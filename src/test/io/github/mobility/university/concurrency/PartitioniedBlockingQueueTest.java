package io.github.mobility.university.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitioniedBlockingQueueTest {

    @Test()
    public void constructs() {
        // execute & verify
        assertNotNull(new PartitionedBlockingQueue(1));
    }

    @Test()
    public void acquiresValue() throws InterruptedException {
        // setup
        var queue = new PartitionedBlockingQueue<String,String>(1);
        // execute
        var actual = queue.acquire("key", "value");
        // verify
        assertEquals("value", actual);
    }

    @Test()
    public void acquiresParallelForDifferentPartitions() throws InterruptedException {
        // setup
        var queue = new PartitionedBlockingQueue<SpecificHash,String>(2);

        assertEquals(queue.acquire(new SpecificHash(0), "first value"), "first value");
        // execute
        var actual = queue.acquire(new SpecificHash(1), "second value");
        // verify
        assertEquals("second value", actual);
    }

    @Test()
    public void releasesAcquiredPartition() throws InterruptedException {
        // setup
        var queue = new PartitionedBlockingQueue<String,String>(1);
        queue.acquire("key", "value");
        // execute & verify
        queue.release("key");
    }

    @Test()
    public void acquiresAgainAfterRelease() throws InterruptedException {
        // setup
        var queue = new PartitionedBlockingQueue<String,String>(1);

        assertEquals(queue.acquire("key", "value"), "value");
        queue.release("key");
        // execute & verify
        var actual = queue.acquire("key", "second value");

        // verify
        assertEquals("second value", actual);
    }
    
    @Test()
    public void blocksParallelAcquireToSamePartition() throws InterruptedException{
        // setup
        var queue = new PartitionedBlockingQueue<String,String>(1);
        var eaters = Executors.newFixedThreadPool(1);
        assertEquals(queue.acquire("key", "value"), "value");
        var counter = new CountDownLatch(1);

        // exercise
        eaters.submit(() -> {
            try {
                var value = queue.acquire("key", "value");
            } catch (InterruptedException e) {
                throw new RuntimeException("not expected");
            }
            counter.countDown();
        });
        counter.await(1, TimeUnit.SECONDS);
        // verify
        assertEquals(counter.getCount(), 1);
    }

    @Test()
    public void providesAllValues() throws InterruptedException{
        // setup
        var queue = new PartitionedBlockingQueue<String,String>(10);
        var workers = Executors.newFixedThreadPool(30);
        var numberOfTasks = 1_000;
        var expectedValues = IntStream.range(0, numberOfTasks).boxed().map(task -> String.format("value %s", task)).collect(Collectors.toList());
        var actualValues = new LinkedBlockingQueue<String>();

        // exercise
        IntStream.range(0, numberOfTasks).forEach(
                task -> {
            workers.submit(() -> {
                try {
                    actualValues.put(queue.acquire(String.format("key %s", task), String.format("value %s", task)));
                    timelyExpansiveOperation();
                    queue.release(String.format("key %s", task));
                } catch (InterruptedException e) {
                    throw new RuntimeException("not expected");
                }
            });
        });
        // verify
        while (!expectedValues.isEmpty()) {
            var actual = actualValues.take();
            assertTrue(expectedValues.contains(actual));
            expectedValues.remove(actual);
        }
    }

    @Test()
    public void keepsOrder() throws InterruptedException{
        // setup
        var queue = new PartitionedBlockingQueue<String,String>(1);
        var workers = Executors.newFixedThreadPool(30);
        var numberOfTasks = 100;

        var inputs = new LinkedBlockingQueue<String>();
        IntStream.range(0, numberOfTasks).forEach(
                task -> {inputs.add(String.format("value %s", task));}
        );
        var outputs = new LinkedBlockingQueue<String>();
        var ingestionSemaphore = new Semaphore(1);

        // exercise
        IntStream.range(0, numberOfTasks).forEach(
                task -> {
                    workers.submit(() -> {
                        try {
                            ingestionSemaphore.acquire();
                            outputs.put(queue.acquire(String.format("key %s", task), inputs.take()));
                            ingestionSemaphore.release();
                            timelyExpansiveOperation();
                            queue.release(String.format("key %s", task));
                        } catch (InterruptedException e) {
                            throw new RuntimeException("not expected");
                        }
                    });
                });
        // verify
        IntStream.range(0, numberOfTasks).forEach(task -> {
            try {
                assertEquals(String.format("value %s", task), outputs.take());
            } catch (InterruptedException e) {
                throw new RuntimeException("not expected");
            }
        });
        assertTrue(outputs.isEmpty());
    }

    /**
     * allows to specify a specific hash
     */
    private class SpecificHash {
        private final int hash;

        public SpecificHash(int hash) {
            this.hash = hash;
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

    }

    private void timelyExpansiveOperation() throws InterruptedException {
        Thread.sleep(500);
    }
}

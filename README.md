# partitioned-blocking-queue

a Blocking Queue to be used in a multi threaded environment which keeps the order within the partition.

## Usage

```!java
@ThreadSafe
class Processor {
    privage PartitionedBlockingQueue<YourPartitionKey, YourValue> queue;

    public Processor() {
        int numberOfPartitions = 10;
        this.queue = new PartitionedBlockingQueue(numberOfPartitions);
    }

    public void work(YourValue value) {
        YourPartitionKey key = toPartitionKey(value);
        YourValue toProcess = this.queue.acquire(key, value);
        try {
            // here your timely expansive operation with "toProcess"
        } finally {
            // release the partition to be able that the next thing is processed
            this.queue.release(key);
        }
    }

    private YourPartitionKey toPartitionKey(YourValue value) {
        return // your code
    }
}
```

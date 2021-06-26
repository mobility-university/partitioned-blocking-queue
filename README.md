[![Build](https://github.com/mobility-university/partitioned-blocking-queue/actions/workflows/maven.yml/badge.svg)](https://github.com/mobility-university/partitioned-blocking-queue/actions/workflows/maven.yml)

# PartitionedBlockingQueue

a Blocking Queue to be used in a multi threaded environment which keeps the order within the partition.

## Motivation

when reading from one and writing to another queue in a multi threaded java programm, then there was no simple solution to achieve this when the data needed to be kept in the order for some partitions.

## Usage

```java

@ThreadSafe
class Processor {
    private PartitionedBlockingQueue<YourPartitionKey, YourValue> queue;

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
        return null; // your code
    }
}
```

## Artifact

```xml
<dependency>
  <groupId>io.github.mobility-university</groupId>
  <artifactId>partitioned-blocking-queue</artifactId>
  <version>0.1</version>
</dependency>
```


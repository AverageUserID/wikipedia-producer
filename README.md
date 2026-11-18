# Wikipedia Producer

Streams Wikipedia recent-change events from the Wikimedia SSE feed into the `wikipedia-edits` Kafka topic.

## Run locally against Redpanda

Build the image and run it on the `stream_network` Docker network so it can reach the Redpanda brokers by container name:

```bash
docker build -t wikipedia-producer .

docker run --rm --name wikipedia-producer \
  --network stream_network \
  -e KAFKA_BROKER=redpanda-0:9092,redpanda-1:9092,redpanda-2:9092 \
  -e MAX_EVENTS=100 \
  wikipedia-producer
```

`MAX_EVENTS` stops the producer after that many events; omit it (or set `0`) to run indefinitely.

"""
Unit tests run without a broker. Integration tests (marked `kafka`) need a
reachable Kafka API at KAFKA_BROKER, e.g. from compose.test.yaml.
"""

import json
import time

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from conftest import BROKER


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, value))


class FakeMsg:
    def key(self):
        return b"k"


def sample_event(i):
    return {"id": i, "title": f"Page {i}", "timestamp": int(time.time()), "type": "edit"}


# ---------- unit ----------

def test_publish_serializes_and_counts(producer_module):
    streamer = producer_module.WikipediaStreamer()
    fake = FakeProducer()

    streamer.publish(fake, sample_event(42))

    assert streamer.produced == 1
    topic, key, value = fake.messages[0]
    assert topic == "wikipedia-edits"
    assert key in ("42", b"42")
    assert json.loads(value)["title"] == "Page 42"


def test_on_delivery_counts_success_and_failure(producer_module):
    streamer = producer_module.WikipediaStreamer()
    streamer.on_delivery(None, FakeMsg())
    streamer.on_delivery("broker down", FakeMsg())
    assert (streamer.delivered, streamer.failed) == (1, 1)


# ---------- integration ----------

@pytest.mark.kafka
def test_round_trip_through_broker(producer_module, topic_name):
    admin = AdminClient({"bootstrap.servers": BROKER})
    admin.create_topics([NewTopic(topic_name, num_partitions=3, replication_factor=1)])[topic_name].result(timeout=15)

    streamer = producer_module.WikipediaStreamer()
    streamer.topic = streamer.app.topic(name=topic_name, value_serializer="json")

    n = 25
    with streamer.app.get_producer() as producer:
        for i in range(n):
            streamer.publish(producer, sample_event(i))
    # Exiting the context flushes, so every delivery callback has fired
    assert streamer.delivered == n
    assert streamer.failed == 0

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": f"{topic_name}-reader",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic_name])
    ids, deadline = set(), time.time() + 20
    while len(ids) < n and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            ids.add(json.loads(msg.value())["id"])
    consumer.close()
    admin.delete_topics([topic_name])

    assert ids == set(range(n))

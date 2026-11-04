"""At-least-once publication; deterministic observation IDs make replay idempotent."""

import os

from confluent_kafka import Producer

from urbanlake.contracts import Observation

TOPIC = "urbanlake.observations.v1"


class Publisher:
    def __init__(self, bootstrap: str | None = None, topic: str = TOPIC):
        self.topic = topic
        self.errors = []
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap or os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 60000,
                "compression.type": "zstd",
                "linger.ms": 20,
                "batch.size": 262144,
            }
        )

    def delivered(self, error, message):
        if error:
            self.errors.append(str(error))

    def send(self, observation: Observation):
        for _ in range(100):
            try:
                self.producer.produce(
                    self.topic,
                    key=f"{observation.kind}:{observation.source_key}",
                    value=observation.model_dump_json(),
                    on_delivery=self.delivered,
                )
                self.producer.poll(0)
                return
            except BufferError:
                self.producer.poll(0.1)
        raise RuntimeError("Kafka producer buffer remained full")

    def flush(self):
        remaining = self.producer.flush(65)
        if remaining or self.errors:
            raise RuntimeError(
                f"Kafka delivery failed: remaining={remaining}, errors={self.errors}"
            )

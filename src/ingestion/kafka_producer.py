"""Kafka producer for simulated IoT sensor data.

Produces synthetic IoT sensor events to Kafka topics for
downstream processing by the Spark Structured Streaming consumer.
"""

from __future__ import annotations

import json
import time

import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import get_settings
from src.generators.iot_simulator import IoTSimulator

logger = structlog.get_logger(__name__)


class SensorDataProducer:
    """Produces IoT sensor events to Kafka topics.

    Attributes:
        producer: KafkaProducer instance.
        topic: Target Kafka topic name.
        simulator: IoT data simulator.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
    ) -> None:
        """Initialize the Kafka producer.

        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated).
            topic: Target Kafka topic for IoT events.
        """
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.topic = topic or settings.kafka.topic_iot
        self.simulator = IoTSimulator()
        self._producer: KafkaProducer | None = None

        logger.info(
            "producer_initialized",
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic,
        )

    @property
    def producer(self) -> KafkaProducer:
        """Lazy-initialize Kafka producer connection."""
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                compression_type="gzip",
                acks="all",
                retries=3,
                linger_ms=50,
            )
        return self._producer

    def produce_event(self, event: dict) -> bool:
        """Send a single IoT event to Kafka.

        Args:
            event: IoT event dictionary.

        Returns:
            True if the event was sent successfully.
        """
        try:
            future = self.producer.send(
                self.topic,
                key=event.get("sensor_id", "unknown"),
                value=event,
            )
            future.get(timeout=10)
            logger.debug("event_produced", event_id=event.get("event_id"), topic=self.topic)
            return True
        except KafkaError as exc:
            logger.error("event_produce_failed", error=str(exc), event_id=event.get("event_id"))
            return False

    def produce_batch(self, batch_size: int = 100) -> int:
        """Generate and send a batch of IoT events.

        Args:
            batch_size: Number of events to generate and send.

        Returns:
            Number of successfully sent events.
        """
        events = self.simulator.generate_batch(batch_size)
        success_count = 0

        for event in events:
            if self.produce_event(event):
                success_count += 1

        self.producer.flush()
        logger.info("batch_produced", total=batch_size, success=success_count, topic=self.topic)
        return success_count

    def run_continuous(self, interval_seconds: float = 5.0, batch_size: int = 10) -> None:
        """Continuously produce IoT events at a fixed interval.

        Args:
            interval_seconds: Seconds between each batch.
            batch_size: Number of events per batch.
        """
        logger.info(
            "continuous_producer_started",
            interval=interval_seconds,
            batch_size=batch_size,
        )
        try:
            while True:
                self.produce_batch(batch_size)
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("continuous_producer_stopped")
        finally:
            self.close()

    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logger.info("producer_closed")


def main() -> None:
    """Entry point for the Kafka producer."""
    producer = SensorDataProducer()
    producer.run_continuous(interval_seconds=5.0, batch_size=10)


if __name__ == "__main__":
    main()

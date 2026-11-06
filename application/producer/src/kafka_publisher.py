"""Kafka publisher for Equipment, SSR and Fare reference events."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError


logger = logging.getLogger(__name__)


TOPIC_BY_JOB = {
    "equipment-agregate": "2l.bridge.equipment",
    "ssr-agregate": "2l.bridge.ssr",
    "fare-agregate": "2l.bridge.fare",
}


class KafkaPublisherError(RuntimeError):
    """Raised when an event cannot be published to Kafka."""


class KafkaPublisher:
    """Publishes versioned JSON events to their corresponding Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        request_timeout_ms: int = 30000,
    ) -> None:
        servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.producer = KafkaProducer(
            bootstrap_servers=servers.split(","),
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
            acks="all",
            retries=10,
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=request_timeout_ms,
        )

        logger.info("Kafka producer initialized: %s", servers)

    def publish(self, job_name: str, event: dict[str, Any]) -> dict:
        """Publish one event and wait for the broker acknowledgement."""

        topic = TOPIC_BY_JOB.get(job_name)

        if topic is None:
            raise ValueError(f"Unsupported job name: {job_name}")

        if not isinstance(event, dict):
            raise TypeError("Event must be a dictionary")

        event_id = event.get("eventId")
        version = event.get("version")
        aggregate_id = event.get("aggregateId")

        if not event_id or version is None or not aggregate_id:
            raise ValueError(
                "Event must contain eventId, version and aggregateId"
            )

        try:
            future = self.producer.send(
                topic=topic,
                key=aggregate_id,
                value=event,
            )

            metadata = future.get(timeout=30)

            logger.info(
                "Published event: topic=%s partition=%s offset=%s "
                "eventId=%s version=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
                event_id,
                version,
            )

            return {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
                "eventId": event_id,
                "version": version,
            }

        except (KafkaError, TimeoutError) as exc:
            logger.exception(
                "Kafka publication failed: job=%s eventId=%s",
                job_name,
                event_id,
            )
            raise KafkaPublisherError(
                f"Could not publish event {event_id} to {topic}"
            ) from exc

    def publish_batch(self, events: dict[str, dict]) -> list[dict]:
        """Publish events for multiple jobs."""

        results = []

        for job_name, event in events.items():
            results.append(self.publish(job_name, event))

        return results

    def close(self) -> None:
        """Flush pending messages and close the producer."""

        if self.producer is not None:
            self.producer.flush(timeout=30)
            self.producer.close(timeout=30)
            logger.info("Kafka producer closed")

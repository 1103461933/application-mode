"""Build versioned Kafka events for Equipment, SSR and Fare."""

from __future__ import annotations

import copy
from typing import Any, Callable

from version_store import VersionStore, utc_now_iso


JOB_DEFINITIONS = {
    "equipment-agregate": {
        "domain": "equipment",
        "record_key_field": "aircraftType",
        "required": ("eventId", "tenant", "aircraftType"),
    },
    "ssr-agregate": {
        "domain": "ssr",
        "record_key_field": "ssrCode",
        "required": ("eventId", "tenant", "ssrCode"),
    },
    "fare-agregate": {
        "domain": "fare",
        "record_key_field": "fareId",
        "required": ("eventId", "tenant", "fareId", "classification"),
    },
}


def normalize_record_key(value: Any) -> str:
    """Normalize a business record key."""
    if value is None:
        raise ValueError("Business record key cannot be null")

    normalized = str(value).strip().upper()

    if not normalized:
        raise ValueError("Business record key cannot be empty")

    return normalized


def build_versioned_event(
    job_name: str,
    payload: dict,
    version_store: VersionStore,
) -> dict:
    """Build an event with a persistent version and UTC timestamp."""

    if job_name not in JOB_DEFINITIONS:
        raise ValueError(f"Unsupported Flink Job: {job_name}")

    if not isinstance(payload, dict):
        raise TypeError("Event payload must be a dictionary")

    definition = JOB_DEFINITIONS[job_name]

    missing = [
        field
        for field in definition["required"]
        if payload.get(field) is None or str(payload[field]).strip() == ""
    ]

    if missing:
        raise ValueError(
            f"{job_name} event missing required fields: {missing}"
        )

    event = copy.deepcopy(payload)

    tenant = str(event["tenant"]).strip().lower()
    domain = definition["domain"]

    record_key = normalize_record_key(
        event[definition["record_key_field"]]
    )

    event_id = str(event["eventId"]).strip()

    # Allocate or recover the version for this logical event.
    allocation = version_store.allocate(
        tenant=tenant,
        domain=domain,
        record_key=record_key,
        event_id=event_id,
    )

    # Normalize the business identity consistently.
    event["tenant"] = tenant
    event[definition["record_key_field"]] = record_key

    event["eventId"] = event_id
    event["eventTimestamp"] = event.get("eventTimestamp") or utc_now_iso()
    event["version"] = allocation["version"]

    # Stable identifier shared with the Flink processor and Couchbase Sink.
    event["aggregateId"] = f"{tenant}:{domain}:{record_key}"

    return event


def build_sample_events(
    version_store: VersionStore,
    sample_events_provider: Callable[[], dict],
) -> dict:
    """Build versioned events using the existing synthetic event generator."""

    samples = sample_events_provider()

    return {
        job_name: build_versioned_event(
            job_name=job_name,
            payload=payload,
            version_store=version_store,
        )
        for job_name, payload in samples.items()
    }


def build_external_event(
    job_name: str,
    json_payload: dict,
    version_store: VersionStore,
) -> dict:
    """Build a versioned event from an externally supplied JSON object."""

    return build_versioned_event(
        job_name=job_name,
        payload=json_payload,
        version_store=version_store,
    )

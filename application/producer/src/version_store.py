
"""Persistent, per-record event version allocation using Couchbase transactions."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from couchbase.exceptions import DocumentNotFoundException


class VersionStoreError(RuntimeError):
    """Raised when a persistent event version cannot be allocated."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _document_key(prefix: str, value: str) -> str:
    """Create a deterministic Couchbase-safe document key."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{prefix}::{digest}"


class VersionStore:
    """Allocates monotonically increasing versions independently per record.

    The counter update and event-idempotency record are committed in one
    Couchbase transaction.
    """

    def __init__(self, cluster: Any, bucket_name: str, scope_name: str,
                 collection_name: str = "producer_versions") -> None:
        self.cluster = cluster
        self.collection = (
            cluster.bucket(bucket_name)
            .scope(scope_name)
            .collection(collection_name)
        )

    def allocate(
        self,
        *,
        tenant: str,
        domain: str,
        record_key: str,
        event_id: str,
    ) -> dict:
        tenant = tenant.strip()
        domain = domain.strip().lower()
        record_key = record_key.strip()
        event_id = event_id.strip()

        if not all((tenant, domain, record_key, event_id)):
            raise ValueError(
                "tenant, domain, record_key and event_id are required"
            )

        record_id = f"{tenant}:{domain}:{record_key}"

        counter_key = _document_key("counter", record_id)
        event_key = _document_key("event", f"{record_id}:{event_id}")

        result: dict = {}

        def transaction_logic(ctx) -> None:
            # An existing event must always receive its original version.
            try:
                existing_event = ctx.get(self.collection, event_key)
                result.update(existing_event.content_as[dict])
                return
            except DocumentNotFoundException:
                pass

            # Read and increment the counter for this business record.
            try:
                counter_doc = ctx.get(self.collection, counter_key)
                counter = counter_doc.content_as[dict]
                next_version = int(counter["version"]) + 1

                updated_counter = {
                    **counter,
                    "version": next_version,
                    "updatedAt": utc_now_iso(),
                }
                ctx.replace(counter_doc, updated_counter)

            except DocumentNotFoundException:
                next_version = 1

                ctx.insert(
                    self.collection,
                    counter_key,
                    {
                        "recordId": record_id,
                        "tenant": tenant,
                        "domain": domain,
                        "recordKey": record_key,
                        "version": next_version,
                        "updatedAt": utc_now_iso(),
                    },
                )

            # Persist the event identity and assigned version atomically.
            event_record = {
                "recordId": record_id,
                "eventId": event_id,
                "tenant": tenant,
                "domain": domain,
                "recordKey": record_key,
                "version": next_version,
                "assignedAt": utc_now_iso(),
            }

            ctx.insert(self.collection, event_key, event_record)
            result.update(event_record)

        self.cluster.transactions.run(transaction_logic)

        if not result:
            raise VersionStoreError(
                f"No version was returned for event {event_id}"
            )

        return result

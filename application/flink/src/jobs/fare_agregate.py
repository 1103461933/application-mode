"""Synthetic fare reference-data aggregation contract."""

def process_event(event: dict) -> dict:
    required = ("eventId", "tenant", "fareId", "classification")
    missing = [k for k in required if not event.get(k)]

    if missing:
        raise ValueError(
            f"fare event missing fields: {missing}"
        )

    tenant = str(event["tenant"]).strip()
    fare_id = str(event["fareId"]).strip().upper()

    refs = event.get("associatedFareRefs", [])

    if not isinstance(refs, list):
        raise ValueError(
            "associatedFareRefs must be a list"
        )

    return {
        "tenant": tenant,
        "aggregateId": f"{tenant}:fare:{fare_id}",
        "fareId": fare_id,
        "classification": str(
            event["classification"]
        ).upper(),
        "associatedFareRefs": sorted({
            str(r).upper() for r in refs
        }),
        "attributes": event.get("attributes", {}),
        "recordType": "fare-agregate",
    }
"""Synthetic fare reference-data aggregation contract."""
def process_event(event: dict) -> dict:
    required = ("eventId", "tenant", "fareId", "classification")
    missing = [k for k in required if not event.get(k)]
    if missing:
        raise ValueError(f"fare event missing fields: {missing}")
    refs = event.get("associatedFareRefs", [])
    if not isinstance(refs, list):
        raise ValueError("associatedFareRefs must be a list")
    return {
        "tenant": event["tenant"],
        "aggregateId": f'{event["tenant"]}:fare:{event["fareId"]}',
        "fareId": str(event["fareId"]).upper(),
        "classification": str(event["classification"]).upper(),
        "associatedFareRefs": sorted({str(r).upper() for r in refs}),
        "attributes": event.get("attributes", {}),
        "recordType": "fare-agregate",
    }

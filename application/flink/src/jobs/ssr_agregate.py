"""Synthetic special-service reference-data aggregation contract."""
def process_event(event: dict) -> dict:
    required = ("eventId", "tenant", "ssrCode")
    missing = [k for k in required if not event.get(k)]
    if missing:
        raise ValueError(f"SSR event missing fields: {missing}")
    categories = event.get("categories", [])
    if not isinstance(categories, list):
        raise ValueError("categories must be a list")
    # This is reference data only: intentionally no passenger/reservation ID.
    return {
        "tenant": event["tenant"],
        "aggregateId": f'{event["tenant"]}:ssr:{str(event["ssrCode"]).upper()}',
        "ssrCode": str(event["ssrCode"]).upper(),
        "description": event.get("description", ""),
        "categories": sorted({str(c).lower() for c in categories}),
        "active": bool(event.get("active", True)),
        "recordType": "ssr-agregate",
    }

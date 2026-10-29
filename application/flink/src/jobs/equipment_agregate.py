"""Synthetic equipment reference-data aggregation contract."""
def process_event(event: dict) -> dict:
    required = ("eventId", "tenant", "aircraftType")
    missing = [k for k in required if not event.get(k)]
    if missing:
        raise ValueError(f"equipment event missing fields: {missing}")
    compartments = event.get("compartments", [])
    if not isinstance(compartments, list):
        raise ValueError("compartments must be a list")
    normalized = []
    for compartment in compartments:
        if not isinstance(compartment, dict) or not compartment.get("compartmentId"):
            raise ValueError("each compartment must have compartmentId")
        units = compartment.get("units", [])
        if not isinstance(units, list):
            raise ValueError("compartment units must be a list")
        normalized.append({
            **compartment,
            "units": units,
            "unitCount": len(units),
        })
    return {
        "tenant": event["tenant"],
        "aggregateId": f'{event["tenant"]}:equipment:{event["aircraftType"]}',
        "aircraftType": str(event["aircraftType"]).upper(),
        "compartments": normalized,
        "compartmentCount": len(normalized),
        "unitCount": sum(c["unitCount"] for c in normalized),
        "recordType": "equipment-agregate",
    }

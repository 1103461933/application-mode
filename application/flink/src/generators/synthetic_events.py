"""Repeatable synthetic input events for local transformation tests."""
def sample_events() -> dict:
    return {
        "equipment-agregate": {
            "eventId": "evt-eqp-001", "tenant": "bridge-2l", "aircraftType": "a350",
            "compartments": [{"compartmentId": "CABIN-1", "compartmentType": "PASSENGER",
                              "units": [{"unitId": "SEAT-1A", "unitType": "SEAT", "capacity": 1}]}],
        },
        "ssr-agregate": {
            "eventId": "evt-ssr-001", "tenant": "bridge-2l", "ssrCode": "wchr",
            "description": "Wheelchair assistance", "categories": ["Assistance", "wheelchair"], "active": True,
        },
        "fare-agregate": {
            "eventId": "evt-fare-001", "tenant": "bridge-2l", "fareId": "y26flex",
            "classification": "economy_flex", "associatedFareRefs": ["y26basic", "y26plus"],
            "attributes": {"refundable": True, "changeable": True},
        },
    }


"""Repeatable synthetic input events for local transformation tests.

Each business record has its own monotonically increasing version.
The real producer must assign the next version whenever that record changes.
"""

SAMPLE_EVENT_TIMESTAMP = "2026-11-06T15:30:00Z"


def sample_events() -> dict:
    return {
        "equipment-agregate": {
            "eventId": "evt-eqp-001",
            "eventTimestamp": SAMPLE_EVENT_TIMESTAMP,
            "version": 1,
            "tenant": "2l",
            "aircraftType": "a350",
            "compartments": [
                {
                    "compartmentId": "CABIN-1",
                    "compartmentType": "PASSENGER",
                    "units": [
                        {
                            "unitId": "SEAT-1A",
                            "unitType": "SEAT",
                            "capacity": 1,
                        }
                    ],
                }
            ],
        },
        "ssr-agregate": {
            "eventId": "evt-ssr-001",
            "eventTimestamp": SAMPLE_EVENT_TIMESTAMP,
            "version": 1,
            "tenant": "2l",
            "ssrCode": "wchr",
            "description": "Wheelchair assistance",
            "categories": ["Assistance", "wheelchair"],
            "active": True,
        },
        "fare-agregate": {
            "eventId": "evt-fare-001",
            "eventTimestamp": SAMPLE_EVENT_TIMESTAMP,
            "version": 1,
            "tenant": "2l",
            "fareId": "y26flex",
            "classification": "economy_flex",
            "associatedFareRefs": ["y26basic", "y26plus"],
            "attributes": {
                "refundable": True,
                "changeable": True,
            },
        },
    }

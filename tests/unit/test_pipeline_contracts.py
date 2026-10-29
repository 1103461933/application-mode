import json
import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC = os.path.join(ROOT, "application", "flink", "src")
sys.path.insert(0, SRC)

from jobs.equipment_agregate import process_event as equipment
from jobs.ssr_agregate import process_event as ssr
from jobs.fare_agregate import process_event as fare
from event_utils import normalize_json_event

class PipelineContractTests(unittest.TestCase):
    def test_equipment_aggregate_counts_units(self):
        out = equipment({
            "eventId": "e1", "tenant": "2L", "aircraftType": "a320",
            "compartments": [{"compartmentId": "C1", "units": ["1A", "1B"]}]
        })
        self.assertEqual(out["aircraftType"], "A320")
        self.assertEqual(out["compartmentCount"], 1)
        self.assertEqual(out["unitCount"], 2)

    def test_ssr_is_reference_data_without_passenger_id(self):
        out = ssr({"eventId": "e2", "tenant": "2L", "ssrCode": "wchr",
                   "categories": ["Mobility", "mobility"]})
        self.assertEqual(out["ssrCode"], "WCHR")
        self.assertEqual(out["categories"], ["mobility"])
        self.assertNotIn("passengerId", out)

    def test_fare_normalizes_references(self):
        out = fare({"eventId": "e3", "tenant": "2L", "fareId": "y1",
                    "classification": "economy", "associatedFareRefs": ["b", "A", "a"]})
        self.assertEqual(out["fareId"], "Y1")
        self.assertEqual(out["associatedFareRefs"], ["A", "B"])

    def test_json_event_must_be_object(self):
        self.assertEqual(normalize_json_event('{"eventId":"e1"}'), {"eventId": "e1"})
        with self.assertRaises(ValueError):
            normalize_json_event("[]")

if __name__ == "__main__":
    unittest.main()

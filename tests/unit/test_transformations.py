import sys
from pathlib import Path
SRC = Path(__file__).parents[2] / "application" / "flink" / "src"
sys.path.insert(0, str(SRC))
from generators.synthetic_events import sample_events
from jobs.equipment_agregate import process_event as equipment
from jobs.ssr_agregate import process_event as ssr
from jobs.fare_agregate import process_event as fare

def test_equipment_compartments_and_units():
    result = equipment(sample_events()["equipment-agregate"])
    assert result["aircraftType"] == "A350"
    assert result["compartmentCount"] == 1
    assert result["unitCount"] == 1

def test_ssr_is_reference_data_without_passenger():
    result = ssr(sample_events()["ssr-agregate"])
    assert result["ssrCode"] == "WCHR"
    assert "passengerId" not in result
    assert "bookingId" not in result

def test_fare_references():
    result = fare(sample_events()["fare-agregate"])
    assert result["fareId"] == "Y26FLEX"
    assert result["associatedFareRefs"] == ["Y26BASIC", "Y26PLUS"]

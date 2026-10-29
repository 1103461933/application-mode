"""Run the synthetic transformation contracts locally; no Kafka/Flink/Couchbase required."""
import json
from generators.synthetic_events import sample_events
from jobs.equipment_agregate import process_event as equipment
from jobs.ssr_agregate import process_event as ssr
from jobs.fare_agregate import process_event as fare

def main():
    events = sample_events()
    results = {
        "equipment-agregate": equipment(events["equipment-agregate"]),
        "ssr-agregate": ssr(events["ssr-agregate"]),
        "fare-agregate": fare(events["fare-agregate"]),
    }
    print(json.dumps(results, indent=2))
if __name__ == "__main__":
    main()

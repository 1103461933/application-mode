"""Pure Python input validation helpers; intentionally independent of PyFlink."""
import json
from typing import Any, Dict

def normalize_json_event(raw: str) -> Dict[str, Any]:
    if not isinstance(raw, str):
        raise TypeError("Kafka message value must be a string")
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("Kafka JSON payload must be an object")
    return event

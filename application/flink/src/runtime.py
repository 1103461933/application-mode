"""Runtime helpers and Java-backed Couchbase sink for Flink 2.2."""
import json
from typing import Any, Dict

from pyflink.datastream.connectors.base import Sink
from pyflink.java_gateway import get_gateway


def normalize_json_event(raw: str) -> Dict[str, Any]:
    if not isinstance(raw, str):
        raise TypeError("Kafka message value must be a string")

    event = json.loads(raw)

    if not isinstance(event, dict):
        raise ValueError("Kafka JSON payload must be an object")

    return event


def create_couchbase_sink(
    connection_string: str,
    username: str,
    password: str,
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    document_id_field: str = "aggregateId",
) -> Sink:
    """Create a PyFlink Sink adapter around the Java Couchbase sink."""

    gateway = get_gateway()

    java_sink = gateway.jvm.com.example.flink.CouchbaseSink(
        connection_string,
        username,
        password,
        bucket_name,
        scope_name,
        collection_name,
        document_id_field,
    )

    return Sink(java_sink)
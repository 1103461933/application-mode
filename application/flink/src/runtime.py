"""Runtime helpers and Couchbase sink for the Flink application."""
import json
from typing import Any, Dict

from pyflink.datastream.functions import SinkFunction

def normalize_json_event(raw: str) -> Dict[str, Any]:
    if not isinstance(raw, str):
        raise TypeError("Kafka message value must be a string")
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("Kafka JSON payload must be an object")
    return event

class CouchbaseJsonSink(SinkFunction):
    """Upsert aggregate documents to Couchbase using the Python SDK.

    The connection is created per sink subtask. Document IDs are derived from
    aggregateId, making repeated upserts idempotent for the same aggregate key.
    Checkpoint-aligned exactly-once transactions are not claimed by this sink.
    """
    def __init__(self, connection_string, username, password, bucket_name,
                 scope_name, collection_name, document_id_field="aggregateId"):
        self.connection_string = connection_string
        self.username = username
        self.password = password
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.collection_name = collection_name
        self.document_id_field = document_id_field
        self.cluster = None
        self.collection = None

    def open(self, runtime_context):
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions
        from couchbase.durability import DurabilityLevel
        self._durability = DurabilityLevel.NONE
        auth = PasswordAuthenticator(self.username, self.password)
        self.cluster = Cluster.connect(
            self.connection_string, ClusterOptions(auth)
        )
        self.cluster.wait_until_ready(__import__(
            "datetime"
        ).timedelta(seconds=15))
        bucket = self.cluster.bucket(self.bucket_name)
        self.collection = bucket.scope(self.scope_name).collection(self.collection_name)

    def invoke(self, value, context):
        document = json.loads(value) if isinstance(value, str) else value
        doc_id = document.get(self.document_id_field)
        if not doc_id:
            raise ValueError(f"Output document missing {self.document_id_field}")
        self.collection.upsert(str(doc_id), document)

    def close(self):
        if self.cluster is not None:
            self.cluster.close()
            self.cluster = None

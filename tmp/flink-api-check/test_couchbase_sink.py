from pathlib import Path

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import Sink
from pyflink.java_gateway import get_gateway

jar_path = Path("/tmp/flink-jars/couchbase-sink-all.jar")

if not jar_path.is_file():
    raise FileNotFoundError(f"JAR not found: {jar_path}")

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(jar_path.as_uri())

gateway = get_gateway()

java_sink = gateway.jvm.com.example.flink.CouchbaseSink(
    "couchbase://localhost",
    "test-user",
    "test-password",
    "flight-reservations",
    "reservations",
    "equipment",
    "id",
)

sink = Sink(java_sink)

stream = env.from_collection(
    ["test-document"],
    type_info=Types.STRING(),
)

stream.sink_to(sink).name("couchbase-sink-registration-test")

print("SUCCESS: CouchbaseSink loaded and registered with sink_to().")
print("No Couchbase connection has been attempted yet.")

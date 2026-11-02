from pathlib import Path

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import Sink
from pyflink.java_gateway import get_gateway

jar_path = "/tmp/flink-jars/flink-api-check-1.0-SNAPSHOT.jar"

if not Path(jar_path).is_file():
    raise FileNotFoundError(f"JAR not found: {jar_path}")

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(Path(jar_path).as_uri())

gateway = get_gateway()
java_sink = gateway.jvm.com.example.flink.LoggingSink()
sink = Sink(java_sink)

stream = env.from_collection(
    ["test-1", "test-2", "test-3"],
    type_info=Types.STRING()
)

stream.sink_to(sink).name("logging-sink-test")

print("SUCCESS: Java LoggingSink registered with sink_to()")
env.execute("logging-sink-integration-test")

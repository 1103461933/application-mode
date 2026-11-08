from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, Encoder
from pyflink.datastream.connectors.file_system import FileSink

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = env.from_collection(
    ["flink-azure-plugin-test"],
    type_info=Types.STRING()
)

sink = FileSink.for_row_format(
    "abfss://application-mode-checkpoints@flinkstorage911.dfs.core.windows.net/plugin-test",
    Encoder.simple_string_encoder()
).build()

data.sink_to(sink)

print("Submitting Azure filesystem plugin test")
env.execute("azure-plugin-classloader-test")

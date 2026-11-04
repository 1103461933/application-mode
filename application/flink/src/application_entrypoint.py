"""Flink 2.2 PyFlink application entrypoint.

One Flink application graph contains three independent logical pipelines:
equipment-agregate, ssr-agregate and fare-agregate.

Each pipeline consumes JSON events from Kafka, processes them in Python,
and writes normalized JSON documents to Couchbase using the Java sink.
"""

import json
import logging
import os
from pathlib import Path

import yaml
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer

from event_utils import normalize_json_event
from jobs.equipment_agregate import process_event as process_equipment
from jobs.fare_agregate import process_event as process_fare
from jobs.ssr_agregate import process_event as process_ssr
from runtime import create_couchbase_sink
from pyflink.java_gateway import get_gateway
from pyflink.common.typeinfo import Types

def committed_offsets_initializer():
    """
    Starts from the earliest available Kafka offset when no
    committed offset exists for a partition.
    """
    jvm = get_gateway().jvm

    java_strategy = (
        jvm.org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
    )

    java_initializer = (
        jvm.org.apache.flink.connector.kafka.source.enumerator.initializer
        .OffsetsInitializer.committedOffsets(java_strategy)
    )

    return KafkaOffsetsInitializer(java_initializer)


LOG = logging.getLogger("flink-reference-jobs")

COUCHBASE_SINK_JAR = Path(
    "/opt/flink/usrlib/couchbase-sink-all.jar"
)

PROCESSORS = {
    "equipment-agregate": process_equipment,
    "ssr-agregate": process_ssr,
    "fare-agregate": process_fare,
}


def load_config():
    """Load and validate the pipeline configuration."""

    config_path = os.getenv(
        "PIPELINE_CONFIG",
        "/opt/flink/usrlib/application/config/jobs.yaml",
    )

    with open(config_path, "r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError("Pipeline configuration must be a YAML mapping")

    spec = config.get("spec", {})
    kafka = spec.get("kafka", {})
    couchbase = spec.get("couchbase", {})
    metadata = spec.get("metadata", {})

    if not kafka.get("bootstrapServers"):
        raise ValueError(
            "spec.kafka.bootstrapServers must be configured"
        )

    for key in (
        "connectionString",
        "usernameSecretEnv",
        "passwordSecretEnv",
        "bucket",
        "scope",
    ):
        if not couchbase.get(key):
            raise ValueError(
                f"spec.couchbase.{key} must be configured"
            )

    for key in ("domain", "tenant"):
        if not metadata.get(key):
            raise ValueError(
                f"spec.metadata.{key} must be configured"
            )

    jobs = spec.get("jobs", [])

    if not isinstance(jobs, list):
        raise ValueError("spec.jobs must be a list")

    for job in jobs:
        if not job.get("enabled", True):
            continue

        name = job.get("name")

        if name not in PROCESSORS:
            raise ValueError(
                f"Unsupported pipeline: {name}"
            )

        if not job.get("inputTopic") or (
            "REPLACE_ME" in job["inputTopic"]
        ):
            raise ValueError(
                f"Valid inputTopic required for {name}"
            )

        if not job.get("outputCollection"):
            raise ValueError(
                f"outputCollection required for {name}"
            )

    return config


def build_pipeline(env, config, job):
    """Build one Kafka-to-Couchbase pipeline."""

    spec = config["spec"]
    kafka_cfg = spec["kafka"]
    couch_cfg = spec["couchbase"]

    name = job["name"]
    topic = job["inputTopic"]

    group_id = job.get(
        "groupId",
        f'{spec["metadata"]["tenant"]}-{name}',
    )

    # Configure the Kafka source.
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(
            kafka_cfg["bootstrapServers"]
        )
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(
            committed_offsets_initializer()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        f"kafka-{name}",
    )

    # Select the processor for this logical pipeline.
    processor = PROCESSORS[name]

    # Parse, process, and serialize the event.
    # Invalid JSON or invalid events fail visibly.
    normalized = stream.map(
    lambda raw: json.dumps(
        processor(normalize_json_event(raw)),
        separators=(",", ":"),
        sort_keys=True,
    ),
    output_type=Types.STRING(),
    )

    # Read Couchbase credentials from environment variables.
    username_env = couch_cfg["usernameSecretEnv"]
    password_env = couch_cfg["passwordSecretEnv"]

    username = os.environ.get(username_env)
    password = os.environ.get(password_env)

    if not username:
        raise ValueError(
            f"Required environment variable {username_env} is missing"
        )

    if not password:
        raise ValueError(
            f"Required environment variable {password_env} is missing"
        )

    # Create the Java-backed Couchbase sink.
    couchbase_sink = create_couchbase_sink(
        connection_string=couch_cfg["connectionString"],
        username=username,
        password=password,
        bucket_name=couch_cfg["bucket"],
        scope_name=couch_cfg["scope"],
        collection_name=job["outputCollection"],
        document_id_field="aggregateId",
    )

    # Register the sink using the modern Sink API.
    normalized.sink_to(
        couchbase_sink
    ).name(f"couchbase-{name}")

    LOG.info(
        "Configured pipeline=%s inputTopic=%s outputCollection=%s",
        name,
        topic,
        job["outputCollection"],
    )


def main():
    """Build and execute the Flink application."""

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    config = load_config()
    spec = config["spec"]

    enabled_jobs = [
        job
        for job in spec.get("jobs", [])
        if job.get("enabled", True)
    ]

    if not enabled_jobs:
        raise ValueError(
            "At least one pipeline must be enabled"
        )

    # Verify that the Java sink JAR is available.
    if not COUCHBASE_SINK_JAR.is_file():
        raise FileNotFoundError(
            f"Couchbase sink JAR not found: {COUCHBASE_SINK_JAR}"
        )

    # Create the Flink execution environment.
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add the Java sink and its bundled dependencies to the job.
    env.add_jars(
        COUCHBASE_SINK_JAR.as_uri()
    )

    env.set_parallelism(
        int(spec.get("parallelism", 1))
    )

    checkpoint_ms = int(
        spec.get("checkpointIntervalMs", 30000)
    )

    env.enable_checkpointing(checkpoint_ms)

    # Add all enabled pipelines to the same Flink job graph.
    for job in enabled_jobs:
        build_pipeline(env, config, job)

    application_name = (
        f'{spec["metadata"]["domain"]}-'
        f'{spec["metadata"]["tenant"]}-reference-pipelines'
    )

    LOG.info(
        "Starting Flink application %s with %d pipelines",
        application_name,
        len(enabled_jobs),
    )

    env.execute(application_name)


if __name__ == "__main__":
    main()


"""Flink 2.2 PyFlink application entrypoint.

Submits three independent streaming jobs in one Flink Application Mode
cluster:
    - equipment-agregate
    - ssr-agregate
    - fare-agregate

Each job has its own execution graph, checkpoint directory and savepoint
directory. The jobs share the Application Mode cluster resources.
"""

import json
import logging
import os
from pathlib import Path

import yaml

from pyflink.common import Configuration
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.java_gateway import get_gateway

from event_utils import normalize_json_event
from jobs.equipment_agregate import process_event as process_equipment
from jobs.fare_agregate import process_event as process_fare
from jobs.ssr_agregate import process_event as process_ssr
from runtime import create_couchbase_sink


LOG = logging.getLogger("flink-reference-jobs")

COUCHBASE_SINK_JAR = Path(
    "/opt/flink/usrlib/couchbase-sink-all.jar"
)

PROCESSORS = {
    "equipment-agregate": process_equipment,
    "ssr-agregate": process_ssr,
    "fare-agregate": process_fare,
}


def committed_offsets_initializer():
    """
    Start from committed Kafka offsets.

    If no committed offset exists for a partition, start from EARLIEST.
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


def load_config():
    """Load and validate the tenant-specific jobs configuration."""

    config_path = os.getenv(
        "PIPELINE_CONFIG",
        "/opt/flink/usrlib/application/config/jobs.yaml",
    )

    with open(config_path, "r", encoding="utf-8-sig") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError(
            "Pipeline configuration must be a YAML mapping"
        )

    spec = config.get("spec", {})
    kafka = spec.get("kafka", {})
    couchbase = spec.get("couchbase", {})
    metadata = spec.get("metadata", {})
    state = spec.get("state", {})

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

    if state.get("backend", "rocksdb") not in ("rocksdb", "hashmap", "forst"):
        raise ValueError(
            "spec.state.backend must be rocksdb, hashmap or forst"
        )

    if state.get("checkpointStorage", "filesystem") != "filesystem":
        raise ValueError(
            "This application currently expects filesystem checkpoint storage"
        )

    jobs = spec.get("jobs", [])

    if not isinstance(jobs, list):
        raise ValueError(
            "spec.jobs must be a list"
        )

    enabled_names = []

    for job in jobs:
        if not isinstance(job, dict):
            raise ValueError(
                "Each item in spec.jobs must be a mapping"
            )

        if not job.get("enabled", True):
            continue

        name = job.get("name")

        if name not in PROCESSORS:
            raise ValueError(
                f"Unsupported Flink job: {name}"
            )

        if not job.get("inputTopic"):
            raise ValueError(
                f"inputTopic is required for {name}"
            )

        if not job.get("outputCollection"):
            raise ValueError(
                f"outputCollection is required for {name}"
            )

        if not job.get("checkpointDir"):
            raise ValueError(
                f"checkpointDir is required for {name}"
            )

        if not job.get("savepointDir"):
            raise ValueError(
                f"savepointDir is required for {name}"
            )

        enabled_names.append(name)

    if len(enabled_names) != len(set(enabled_names)):
        raise ValueError(
            "Enabled Flink job names must be unique"
        )

    return config


def configure_job_environment(env, spec, job):
    """
    Configure checkpointing and state independently for one job.

    The configuration is applied before building and submitting the
    corresponding StreamGraph.
    """

    state = spec.get("state", {})

    checkpoint_interval = int(
        job.get(
            "checkpointIntervalMs",
            spec.get("checkpointIntervalMs", 30000),
        )
    )

    if checkpoint_interval <= 0:
        raise ValueError(
            f"Invalid checkpoint interval for {job['name']}"
        )

    job_configuration = Configuration()

    # State backend.
    job_configuration.set_string(
        "state.backend.type",
        state.get("backend", "rocksdb"),
    )

    # Checkpoint storage and job-specific checkpoint directory.
    job_configuration.set_string(
        "execution.checkpointing.storage",
        state.get("checkpointStorage", "filesystem"),
    )

    job_configuration.set_string(
        "execution.checkpointing.dir",
        job["checkpointDir"],
    )

    # Incremental checkpoints.
    job_configuration.set_string(
        "execution.checkpointing.incremental",
        str(
            state.get("incrementalCheckpoints", True)
        ).lower(),
    )

    # Exactly-once checkpointing.
    job_configuration.set_string(
        "execution.checkpointing.mode",
        "EXACTLY_ONCE",
    )

    # Apply the configuration to the execution environment.
    env.configure(job_configuration)

    # Job-specific savepoint directory.
    env.set_default_savepoint_directory(
        job["savepointDir"]
    )

    # Configure the checkpoint interval for this job.
    env.enable_checkpointing(
        checkpoint_interval,
        CheckpointingMode.EXACTLY_ONCE,
    )

    # Configure job parallelism.
    env.set_parallelism(
        int(
            job.get(
                "parallelism",
                spec.get("parallelism", 1),
            )
        )
    )

    LOG.info(
        "Configured state for job=%s checkpointDir=%s savepointDir=%s",
        job["name"],
        job["checkpointDir"],
        job["savepointDir"],
    )


def build_pipeline(env, config, job):
    """Build exactly one Kafka-to-Couchbase processing flow."""

    spec = config["spec"]
    kafka_cfg = spec["kafka"]
    couch_cfg = spec["couchbase"]

    name = job["name"]
    topic = job["inputTopic"]

    tenant = spec["metadata"]["tenant"]

    group_id = job.get(
        "groupId",
        f"{tenant}-{name}",
    )

    # Kafka source.
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

    # Select the transformation for this job.
    processor = PROCESSORS[name]

    normalized = stream.map(
        lambda raw: json.dumps(
            processor(
                normalize_json_event(raw)
            ),
            separators=(",", ":"),
            sort_keys=True,
        ),
        output_type=Types.STRING(),
    )

    # Resolve Couchbase credentials from Kubernetes Secret environment vars.
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

    # Couchbase Java sink.
    couchbase_sink = create_couchbase_sink(
        connection_string=couch_cfg["connectionString"],
        username=username,
        password=password,
        bucket_name=couch_cfg["bucket"],
        scope_name=couch_cfg["scope"],
        collection_name=job["outputCollection"],
        document_id_field="aggregateId",
    )

    normalized.sink_to(
        couchbase_sink
    ).name(
        f"couchbase-{name}"
    )

    LOG.info(
        "Built job=%s tenant=%s inputTopic=%s outputCollection=%s",
        name,
        tenant,
        topic,
        job["outputCollection"],
    )


def main():
    """Submit all enabled jobs asynchronously in Application Mode."""

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
            "At least one Flink job must be enabled"
        )

    if not COUCHBASE_SINK_JAR.is_file():
        raise FileNotFoundError(
            f"Couchbase sink JAR not found: {COUCHBASE_SINK_JAR}"
        )

    # One Application Mode execution environment.
    # execute_async() generates and submits a separate StreamGraph
    # and clears the registered transformations after each submission.
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add the Couchbase Java sink JAR to the application.
    env.add_jars(
        COUCHBASE_SINK_JAR.as_uri()
    )

    tenant = spec["metadata"]["tenant"]

    submitted_jobs = []

    for job in enabled_jobs:
        job_name = job["name"]
        application_job_name = f"{tenant}-{job_name}"

        # Apply this job's checkpoint/savepoint configuration.
        configure_job_environment(
            env,
            spec,
            job,
        )

        # Register only this job's transformations.
        build_pipeline(
            env,
            config,
            job,
        )

        LOG.info(
            "Submitting independent Flink job=%s",
            application_job_name,
        )

        # Non-blocking submission.
        job_client = env.execute_async(
            application_job_name
        )

        submitted_jobs.append(
            {
                "name": application_job_name,
                "client": job_client,
            }
        )

        LOG.info(
            "Submission accepted for job=%s jobId=%s",
            application_job_name,
            job_client.get_job_id(),
        )

    LOG.info(
        "Submitted %d independent streaming jobs for tenant=%s",
        len(submitted_jobs),
        tenant,
    )

    for submitted in submitted_jobs:
        LOG.info(
            "Submitted job=%s",
            submitted["name"],
        )


if __name__ == "__main__":
    main()

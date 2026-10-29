"""Flink 2.2 PyFlink application entrypoint.

One Flink application graph contains three independent logical pipelines:
equipment-agregate, ssr-agregate and fare-agregate. Each consumes JSON
events from its configured Kafka topic and writes normalized JSON documents
to Couchbase.
"""
import json
import logging
import os
from pathlib import Path

import yaml
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema

from jobs.equipment_agregate import process_event as process_equipment
from jobs.ssr_agregate import process_event as process_ssr
from jobs.fare_agregate import process_event as process_fare
from runtime import CouchbaseJsonSink
from event_utils import normalize_json_event

LOG = logging.getLogger("flink-reference-jobs")

PROCESSORS = {
    "equipment-agregate": process_equipment,
    "ssr-agregate": process_ssr,
    "fare-agregate": process_fare,
}

def load_config():
    config_path = os.getenv("PIPELINE_CONFIG", "/opt/flink/usrlib/application/config/jobs.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    spec = config.get("spec", {})
    kafka = spec.get("kafka", {})
    couchbase = spec.get("couchbase", {})
    if not kafka.get("bootstrapServers"):
        raise ValueError("spec.kafka.bootstrapServers must be configured")
    for key in ("connectionString", "usernameSecretEnv", "passwordSecretEnv", "bucket", "scope"):
        if not couchbase.get(key):
            raise ValueError(f"spec.couchbase.{key} must be configured")
    for job in spec.get("jobs", []):
        if job.get("enabled", True):
            if job.get("name") not in PROCESSORS:
                raise ValueError(f"Unsupported pipeline: {job.get('name')}")
            if not job.get("inputTopic") or "REPLACE_ME" in job["inputTopic"]:
                raise ValueError(f"Valid inputTopic required for {job.get('name')}")
    return config

def build_pipeline(env, config, job):
    spec = config["spec"]
    kafka_cfg = spec["kafka"]
    couch_cfg = spec["couchbase"]
    name = job["name"]
    topic = job["inputTopic"]
    group_id = job.get("groupId", f'{spec["metadata"]["tenant"]}-{name}')
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_cfg["bootstrapServers"])
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(__import__(
            "pyflink.datastream.connectors.kafka", fromlist=["KafkaOffsetsInitializer"]
        ).KafkaOffsetsInitializer.committed_offsets())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), f"kafka-{name}"
    )
    processor = PROCESSORS[name]
    # Validate and normalize JSON in the Python operator; malformed events fail
    # visibly rather than being silently dropped.
    normalized = stream.map(
        lambda raw: json.dumps(
            processor(normalize_json_event(raw)), separators=(",", ":"), sort_keys=True
        )
    )
    normalized.add_sink(
        CouchbaseJsonSink(
            connection_string=couch_cfg["connectionString"],
            username=os.environ[couch_cfg["usernameSecretEnv"]],
            password=os.environ[couch_cfg["passwordSecretEnv"]],
            bucket_name=couch_cfg["bucket"],
            scope_name=couch_cfg["scope"],
            collection_name=job["outputCollection"],
            document_id_field="aggregateId",
        )
    ).name(f"couchbase-{name}")
    LOG.info("Configured pipeline=%s inputTopic=%s outputCollection=%s",
             name, topic, job["outputCollection"])

def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    config = load_config()
    spec = config["spec"]
    enabled_jobs = [j for j in spec.get("jobs", []) if j.get("enabled", True)]
    if not enabled_jobs:
        raise ValueError("At least one pipeline must be enabled")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(spec.get("parallelism", 1)))
    checkpoint_ms = int(spec.get("checkpointIntervalMs", 30000))
    env.enable_checkpointing(checkpoint_ms)

    for job in enabled_jobs:
        build_pipeline(env, config, job)

    # One application and one Flink job graph with three logical pipelines.
    env.execute(f'{spec["metadata"]["domain"]}-{spec["metadata"]["tenant"]}-reference-pipelines')

if __name__ == "__main__":
    main()

# Implementation status — next step

## Implemented in this revision
- One PyFlink application entrypoint defines three logical Kafka-to-Couchbase pipelines.
- Each pipeline reads JSON messages from its configured Kafka topic.
- The existing transformation functions are wired into the DataStream graph.
- Output documents are upserted to the configured Couchbase collection using aggregateId.
- Tenant configuration examples exist for bridge-2l and bridge-3l.
- The Docker build is intended to run from repository root and includes application code plus jobs configuration.

## Not yet validated end-to-end
- Building the Docker image against the selected official Flink 2.2.0 image.
- Successful download/loading of the Kafka connector JAR compatible with Flink 2.2.0.
- PyFlink SinkFunction API compatibility and Couchbase SDK behavior.
- Native Kubernetes Application Mode startup command and JobManager lifecycle.
- Kafka topic existence, security and serialization contracts.
- Couchbase bucket/scope/collection existence and permissions.
- Checkpoint restore, savepoint migration, duplicate handling and rollback.

Do not apply to a production or shared cluster until the above checks pass.

# Flink reference pipelines

This application defines three logical DataStream pipelines in one Flink
application graph: equipment-agregate, ssr-agregate and fare-agregate.

## Local syntax checks

From this directory:
```bash
python -m compileall -q src
python -m unittest discover -s ../../tests/unit -v
```

## Required configuration

Edit `config/jobs.yaml` before building the image. Set the Kafka bootstrap
address, the Couchbase endpoint, and valid output collections. Supply
`COUCHBASE_USERNAME` and `COUCHBASE_PASSWORD` through Kubernetes Secrets.
Topic names must match the topics provisioned in the shared Kafka cluster.

## Build context

Build from the repository root so Docker can copy the shared configuration:
```bash
docker build -f application/flink/Dockerfile -t flink-reference-jobs:2.2.0-dev .
```

## Important validation status

The source is an implementation draft and has not been run against an actual
Flink 2.2.0 cluster, Kafka, or Couchbase in this environment. In particular,
validate the official image's Python runtime, the PyFlink custom sink API,
Kafka connector JAR availability, Couchbase SDK behavior, and native
Kubernetes Application Mode command before deployment. The Couchbase sink
uses idempotent upserts but does not promise end-to-end exactly-once writes.

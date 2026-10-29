# Migration sequence

1. Inventory existing Session cluster, actual topic names, schemas, consumer groups, Couchbase target and current scripts.
2. Establish baseline using current Session Mode.
3. Build a Flink 2.2.0-compatible image with these three simulated aggregators.
4. Validate transformations locally, then validate Kafka source and Couchbase upsert adapters in a non-production environment.
5. Deploy the private namespace and mini-cluster side by side using isolated test topics or controlled shadow consumption.
6. Rehearse savepoint/restore and verify source offsets and idempotent writes.
7. Cut over with approved stop/savepoint/start scripts; keep rollback available.

The current code is a local simulation only. It does not yet instantiate a Flink job or connect to Kafka/Couchbase. Do not deploy the deny-all NetworkPolicy until required egress/ingress rules are reviewed.

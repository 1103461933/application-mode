# Rollback

Stop the Application Mode workload using the approved procedure, record logs/state/offsets, and restart the Session Mode jobs from the verified prior deployment or compatible savepoints. Verify source offsets and sink idempotency. Do not delete the Session Mode namespace during migration. Do not remove shared Kafka, Couchbase, Argo CD, Argo Workflows or storage services.

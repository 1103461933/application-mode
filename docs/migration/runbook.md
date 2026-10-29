# Runbook: Session first, then Application

1. Verify current AKS context and shared service endpoints.
2. Deploy the Session Mode namespace and validated Flink 2.2.0 cluster manifests.
3. Submit the three jobs using the approved Flink CLI and record job IDs/status, lag, checkpoints and sink counts.
4. Build an image containing the same business transformations for Application Mode.
5. Review `application-mode` security controls and add necessary network allow rules.
6. Deploy target in parallel using isolated test topics or controlled shadow reads.
7. Rehearse savepoint/restore and rollback.
8. Schedule cutover, stop old jobs with approved scripts, save final state and start target.
9. Validate offsets, records, duplicates and errors. Retain old resources until sign-off.

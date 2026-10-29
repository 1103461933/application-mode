# Migration gate checklist

- [ ] Confirm kubectl context and cluster identity.
- [ ] Deploy Session Mode first in `session-mode`.
- [ ] Confirm all three simulated jobs are running and record baseline.
- [ ] Confirm Kafka topics/ACLs and Couchbase endpoints from live configuration.
- [ ] Build and scan the Flink 2.2.0 image.
- [ ] Review Application Mode command and connector compatibility.
- [ ] Review RBAC, ResourceQuota, LimitRange and NetworkPolicy allow rules.
- [ ] Test savepoint/restore and sink idempotency.
- [ ] Side-by-side test with isolated topics or controlled consumer strategy.
- [ ] Approve cutover and rollback.
- [ ] Keep Session Mode resources until acceptance.

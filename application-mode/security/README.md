# Security controls

The namespace represents Product Management / bridge-2l. Quota and LimitRange are provisional. RBAC is intentionally incomplete pending verification of the chosen deployment method. The deny-all NetworkPolicy blocks traffic until reviewed allow rules are added; do not apply to a live namespace before validating required flows and CNI support. Kafka remains shared, so Kafka ACLs/credentials must restrict this tenant to its own topics.

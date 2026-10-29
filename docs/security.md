# Security review before deployment

- Keep credentials in Kubernetes Secrets or workload identity; never commit them.
- Namespace RBAC must follow least privilege.
- Review ResourceQuota/LimitRange against AKS capacity.
- NetworkPolicy default-deny manifests block required flows until allow rules are added.
- Ensure CNI supports NetworkPolicy.
- Kafka topic separation requires Kafka ACLs and tenant-specific credentials in addition to Kubernetes networking.
- Use an internal ClusterIP for Flink REST unless an approved secure access path is required.
- No Flink Kubernetes Operator is included.

| Concern | Session Mode | Application Mode |
|---|---|---|
| Namespace | `session-mode` | `application-mode` |
| Startup | Cluster starts independently; jobs submitted later | Application code packaged in image before cluster/application starts |
| Jobs | Three simulated reference-data jobs share the Session cluster | Three jobs belong to the dedicated domain/tenant application |
| Isolation | Cluster/worker pool shared within Session deployment | Dedicated namespace, RBAC, quotas and network policy |
| Kafka | Existing shared brokers/topics | Same brokers/topics, no topic migration intended |
| Deployment | Existing CLI/scripts; no operator required | Image build plus reviewed native Flink deployment manifests/scripts |
| Upgrade | Savepoint and restart the relevant job | Coordinated savepoint/restore and application restart |

The exact savepoint granularity and restore compatibility must be proven for the implementation.

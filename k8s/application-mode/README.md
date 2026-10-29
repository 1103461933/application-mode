# Application Mode — second phase

This namespace is separate from `session-mode`. The target is one private mini-cluster for these three simulated jobs. The JobManager command and the way all three jobs are packaged/launched must be finalized against Flink 2.2.0 before applying manifests. Review the deny-all NetworkPolicy and add required flows before use.

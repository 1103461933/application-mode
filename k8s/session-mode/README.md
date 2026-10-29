# Session Mode — first phase

Apply namespace and manifests only after filling image and configuration placeholders and validating them against Flink 2.2.0. Submit jobs with the approved Flink CLI from the CI runner/client container. This scaffold does not include a production-ready REST ingress and intentionally uses an internal ClusterIP service.

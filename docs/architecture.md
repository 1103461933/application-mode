# Architecture

One shared Kafka service remains outside these mode-specific namespaces. Session Mode is deployed first into `session-mode`, where jobs are submitted to an already-running cluster. Application Mode is deployed later into `application-mode`, with code baked into the image before startup. Both environments may coexist during validation; do not run duplicate production consumers without a controlled plan.

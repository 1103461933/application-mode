# Flink Session Mode → Application Mode Lab

This repository is a **complete project scaffold** for demonstrating and migrating three simulated reference-data pipelines:

- `equipment-agregate`: aircraft type, compartments, and units within each compartment.
- `ssr-agregate`: reference data for special services such as wheelchair assistance and meal preferences; not linked to an individual passenger.
- `fare-agregate`: fare identifier, classification, and references to associated fares.

Target Flink version: **Apache Flink 2.2.0**. Compatibility of the Python runtime, connectors and deployment command must be verified before cluster deployment.

## Required rollout order

1. Deploy and validate **Session Mode first**, in namespace `session-mode`.
2. Keep the Session Mode environment as the baseline while building and validating Application Mode.
3. Deploy **Application Mode** in a separate namespace, `application-mode`, for side-by-side testing.
4. Cut over only after validating savepoint/restore, Kafka offsets, sink idempotency and rollback.
5. Retain the Session Mode namespace and resources until acceptance is complete.

The two namespaces are separate workloads. Shared platform services (Kafka, Couchbase, Argo CD, Argo Workflows, operators, ingress and storage) must not be deleted or duplicated by this project.

## Architecture

```text
                         Shared Kafka brokers/topics
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
             namespace: session-mode     namespace: application-mode
             Session cluster             Dedicated Application cluster
             jobs submitted at runtime   code packaged in image
             via Flink CLI               before application startup
                    │                           │
                    └─────────────┬─────────────┘
                                  ▼
                         Couchbase (shared service,
                         only if confirmed by config)
```

## Repository layout

- `application/flink/`: synthetic pipeline transformations, generators and container image.
- `config/`: job metadata and environment placeholders.
- `k8s/session-mode/`: raw Kubernetes starter manifests for Session Mode.
- `k8s/application-mode/`: raw Kubernetes starter manifests for Application Mode.
- `charts/flink-lab/`: Helm umbrella chart with separate Session/Application subcharts and staged values.
- `argo-cd/`: AppProject and Applications for each namespace.
- `argo-workflows/`: workflow template for build/deploy/test stages.
- `.github/workflows/`: image build workflow and validation workflow.
- `scripts/`: PowerShell helper scripts for validation and staged rollout.
- `tests/`: unit tests for the synthetic transformations.
- `docs/`: architecture, mode comparison, migration and rollback runbooks.

## Helm umbrella chart and Argo CD version updates

The umbrella chart is under `charts/flink-lab/`. It enables Session Mode by default and keeps Application Mode disabled. Argo CD reconciles desired versions from Git; it does not automatically discover new image tags without a separate image-update mechanism. Use immutable tags and update the values via a reviewed PR or configure Argo CD Image Updater/GitHub Actions.

## Before using

This is a scaffold, not a claim that the jobs have been deployed. The Kafka topics, broker endpoint, Couchbase endpoint, credentials, Azure state paths, AKS context and image registry settings must be verified. Placeholder values are intentionally marked `REPLACE_ME`.

Do not apply the default-deny NetworkPolicy until required DNS, Kafka, Couchbase, Azure Blob Storage, metrics and control-plane traffic have been added and tested. A namespace and NetworkPolicy alone do not isolate Kafka topics; use Kafka ACLs and tenant-specific credentials.

No Flink Kubernetes Operator is required by this target design. The existing cluster/platform services are not modified by these manifests.


## Implementación de pipelines (siguiente paso)

Esta revisión conecta los tres transformadores con un punto de entrada PyFlink que define tres flujos lógicos en una única aplicación: `equipment-agregate`, `ssr-agregate` y `fare-agregate`. La configuración de ejemplo está en `config/jobs.yaml`; los ejemplos de tenants están en `config/tenants/`. Consulte `docs/implementation-status.md`: la integración aún requiere pruebas reales con Flink 2.2.0, Kafka y Couchbase antes de desplegarse.


## Siguiente paso: validación de build

Ejecuta `scripts/build-and-validate.ps1` en Windows o `scripts/build-and-validate.sh` en WSL desde la raíz del repositorio. No despliegues todavía los templates actuales de Application Mode: el comando de inicio aún contiene un placeholder y debe reemplazarse por el envío nativo de Flink a Kubernetes Application Mode. Consulta `docs/next-step-build-validation.md`.

# Next step: build and validate the application image

## Why this step comes before AKS deployment

The repository still has a placeholder in the Application Mode JobManager template.
The native Kubernetes Application Mode CLI creates the JobManager/TaskManager resources
from the Flink CLI (`flink run --target kubernetes-application`); it is not equivalent
to starting a static JobManager Deployment with a placeholder argument. Therefore,
do not apply the current Application Mode Helm workload to AKS yet.

## Run locally from repository root

Windows PowerShell:
```powershell
.\scripts\build-and-validate.ps1
```

Linux/WSL:
```bash
chmod +x scripts/build-and-validate.sh
./scripts/build-and-validate.sh
```

## What passing means

- Python source compiles.
- Unit tests pass.
- Docker can build the image.

It does not prove that the image starts a Flink 2.2.0 application, that the Kafka connector
is loaded, or that Kafka/Couchbase integrations work. Those require a runtime smoke test.

## Important architectural correction before deployment

Use the Flink native Kubernetes Application Mode target to create a private cluster per
domain+tenant, with the cluster ID and namespace set for that deployment. Do not use the
placeholder `REPLACE_ME_WITH_VERIFIED_APPLICATION_MODE_COMMAND`, and do not deploy both
the static JobManager/TaskManager Deployment templates and a native Application Mode
cluster for the same tenant. The next implementation task is to replace those templates
with a controlled native-CLI submission Job and narrowly scoped RBAC, then validate the
command and the image against Flink 2.2.0.

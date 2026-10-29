param([string]$Context = "aks-cluster-dev")
$ErrorActionPreference = "Stop"
kubectl config use-context $Context
kubectl apply -f k8s/session-mode/namespace.yaml
# Apply remaining manifests only after image and Flink 2.2.0 command are verified.
Write-Host "Namespace created/applied. Review and validate manifests before deploying workloads."

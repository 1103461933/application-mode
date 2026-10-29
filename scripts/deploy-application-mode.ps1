param([string]$Context = "aks-cluster-dev")
$ErrorActionPreference = "Stop"
kubectl config use-context $Context
Write-Host "Safety gate: do not deploy Application Mode until Session Mode baseline and savepoint/restore rehearsal are approved."
Write-Host "After review, apply k8s/application-mode/namespace.yaml and reviewed security/workload manifests."

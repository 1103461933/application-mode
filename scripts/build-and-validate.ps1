[CmdletBinding()]
param(
  [string]$ImageName = "flink-reference-jobs",
  [string]$ImageTag = "2.2.0-dev",
  [switch]$SkipDockerBuild
)
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
  Write-Host "1/3 - Python syntax validation"
  python -m compileall -q application/flink/src
  if ($LASTEXITCODE -ne 0) { throw "Python compileall failed." }

  Write-Host "2/3 - Unit tests"
  python -m unittest discover -s tests/unit -v
  if ($LASTEXITCODE -ne 0) { throw "Unit tests failed." }

  if (-not $SkipDockerBuild) {
    Write-Host "3/3 - Docker image build"
    docker build -f application/flink/Dockerfile -t "${ImageName}:${ImageTag}" .
    if ($LASTEXITCODE -ne 0) { throw "Docker build failed." }
    Write-Host "Image built: ${ImageName}:${ImageTag}"
    Write-Host "NOTE: build success does not prove Flink/Kafka/Couchbase end-to-end compatibility."
  } else {
    Write-Host "3/3 - Docker build skipped by request."
  }
} finally {
  Pop-Location
}

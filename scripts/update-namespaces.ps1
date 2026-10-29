$ErrorActionPreference = "Stop"

$sessionNs = "session-mode-bridge-2l"
$appNs = "application-mode-bridge-2l"

$files = @(
    "config/environments/dev.yaml",
    "config/tenants/bridge-2l.yaml",
    "charts/flink-lab/values.yaml",
    "k8s/session-mode/namespace.yaml",
    "k8s/session-mode/jobmanager-deployment.yaml",
    "k8s/session-mode/taskmanager-deployment.yaml",
    "k8s/session-mode/service.yaml",
    "k8s/application-mode/namespace.yaml",
    "k8s/application-mode/jobmanager-deployment.yaml",
    "k8s/application-mode/taskmanager-deployment.yaml",
    "k8s/application-mode/security/serviceaccount-rbac.yaml",
    "k8s/application-mode/security/networkpolicy.yaml",
    "k8s/application-mode/security/resourcequota.yaml",
    "k8s/application-mode/security/limitrange.yaml",
    "application-mode/namespace.yaml",
    "application-mode/security/rbac.yaml",
    "application-mode/security/networkpolicy.yaml",
    "application-mode/security/resourcequota.yaml",
    "application-mode/security/limitrange.yaml",
    "argo-cd/session-mode-application.yaml",
    "argo-cd/application-mode-application.yaml",
    "argo-cd/project.yaml"
)

$backupRoot = "backups/namespaces-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
New-Item -ItemType Directory -Force $backupRoot | Out-Null

foreach ($relativePath in $files) {
    $path = Join-Path (Get-Location) $relativePath

    if (-not (Test-Path $path -PathType Leaf)) {
        Write-Warning "Archivo no encontrado; se omite: $relativePath"
        continue
    }

    $content = [System.IO.File]::ReadAllText($path)
    $updated = $content

    # Actualizar únicamente campos YAML de namespace, no rutas ni nombres.
    $updated = [regex]::Replace(
        $updated,
        '(?m)^(\s*namespace:\s*)session-mode\s*$',
        "`${1}$sessionNs"
    )

    $updated = [regex]::Replace(
        $updated,
        '(?m)^(\s*namespace:\s*)application-mode\s*$',
        "`${1}$appNs"
    )

    $updated = [regex]::Replace(
        $updated,
        '(?m)^(\s*sessionNamespace:\s*)session-mode\s*$',
        "`${1}$sessionNs"
    )

    $updated = [regex]::Replace(
        $updated,
        '(?m)^(\s*applicationNamespace:\s*)application-mode\s*$',
        "`${1}$appNs"
    )

    # Actualizar los destinos de AppProject, que usan "namespace" bajo "destinations".
    if ($relativePath -eq "argo-cd/project.yaml") {
        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*-\s*namespace:\s*)session-mode\s*$',
            "`${1}$sessionNs"
        )

        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*-\s*namespace:\s*)application-mode\s*$',
            "`${1}$appNs"
        )
    }

    # La configuración del tenant debe apuntar a Application Mode.
    if ($relativePath -eq "config/tenants/bridge-2l.yaml") {
        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*namespace:\s*)application-mode-bridge-2l\s*$',
            "`${1}$appNs"
        )
    }

    # Actualizar explícitamente los valores de los dos modos del chart.
    if ($relativePath -eq "charts/flink-lab/values.yaml") {
        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*namespace:\s*)session-mode\s*$',
            "`${1}$sessionNs"
        )

        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*namespace:\s*)application-mode\s*$',
            "`${1}$appNs"
        )
    }

    # Configurar el destino de cada Application de Argo CD.
    if ($relativePath -eq "argo-cd/session-mode-application.yaml") {
        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*namespace:\s*)default\s*$',
            "`${1}$sessionNs"
        )
    }

    if ($relativePath -eq "argo-cd/application-mode-application.yaml") {
        $updated = [regex]::Replace(
            $updated,
            '(?m)^(\s*namespace:\s*)default\s*$',
            "`${1}$appNs"
        )
    }

    if ($updated -ne $content) {
        $backupPath = Join-Path $backupRoot $relativePath
        $backupDir = Split-Path -Parent $backupPath

        New-Item -ItemType Directory -Force $backupDir | Out-Null
        Copy-Item $path $backupPath

        [System.IO.File]::WriteAllText(
            $path,
            $updated,
            [System.Text.UTF8Encoding]::new($false)
        )

        Write-Host "ACTUALIZADO: $relativePath"
    }
    else {
        Write-Host "SIN CAMBIOS: $relativePath"
    }
}

Write-Host ""
Write-Host "Copia de seguridad: $backupRoot"
Write-Host "No se ha ejecutado ningún despliegue en Kubernetes."

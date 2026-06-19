param(
    [int]$DashboardId = 11,
    [string]$ContainerName = "tiki_superset",
    [string]$OutputPath = "superset_exports/tiki_dashboards.zip",
    [string]$Username = ""
)

$ErrorActionPreference = "Stop"

function Get-DotEnvValue {
    param(
        [string]$Name,
        [string]$EnvPath = ".env"
    )

    if (-not (Test-Path $EnvPath)) {
        return ""
    }

    $line = Get-Content $EnvPath | Where-Object { $_ -match "^\s*$Name\s*=" } | Select-Object -First 1
    if (-not $line) {
        return ""
    }

    return ($line -replace "^\s*$Name\s*=", "").Trim()
}

if ([string]::IsNullOrWhiteSpace($Username)) {
    $Username = Get-DotEnvValue -Name "SUPERSET_ADMIN_USER"
}

if ([string]::IsNullOrWhiteSpace($Username)) {
    throw "Username is required. Pass -Username or set SUPERSET_ADMIN_USER in .env."
}

$outputDir = Split-Path -Parent $OutputPath
if (-not [string]::IsNullOrWhiteSpace($outputDir)) {
    New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
}

$containerZipPath = "/tmp/tiki_dashboards.zip"

$python = @"
from zipfile import ZipFile, ZIP_DEFLATED
from flask import g
from flask_login import login_user
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.commands.dashboard.export import ExportDashboardsCommand
    from superset.extensions import security_manager

    user = security_manager.find_user(username="$Username")
    if user is None:
        raise RuntimeError("Superset user not found: $Username")

    with app.test_request_context("/"):
        login_user(user)
        g.user = user
        with ZipFile("$containerZipPath", "w", ZIP_DEFLATED) as bundle:
            for file_name, file_content in ExportDashboardsCommand([$DashboardId], export_related=True).run():
                content = file_content() if callable(file_content) else file_content
                bundle.writestr(file_name, content)
"@

$python | docker exec -i $ContainerName python -
docker cp "${ContainerName}:${containerZipPath}" $OutputPath

Write-Host "Exported Superset dashboard $DashboardId to $OutputPath"

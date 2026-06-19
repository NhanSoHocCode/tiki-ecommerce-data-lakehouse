param(
    [string]$ContainerName = "tiki_superset",
    [string]$InputPath = "superset_exports/tiki_dashboards.zip",
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

if (-not (Test-Path $InputPath)) {
    throw "Superset dashboard export not found: $InputPath"
}

if ([string]::IsNullOrWhiteSpace($Username)) {
    $Username = Get-DotEnvValue -Name "SUPERSET_ADMIN_USER"
}

if ([string]::IsNullOrWhiteSpace($Username)) {
    throw "Username is required. Pass -Username or set SUPERSET_ADMIN_USER in .env."
}

$containerZipPath = "/tmp/tiki_dashboards.zip"

docker cp $InputPath "${ContainerName}:${containerZipPath}"
docker exec $ContainerName superset import-dashboards -p $containerZipPath -u $Username

Write-Host "Imported Superset dashboard from $InputPath as owner $Username"

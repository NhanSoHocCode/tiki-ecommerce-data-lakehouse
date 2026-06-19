param(
    [switch]$Force,
    [string]$PostgresContainer = "tiki_postgres",
    [string]$TrinoContainer = "tiki_trino",
    [string]$PostgresUser = "nhansohoccode",
    [string]$PostgresDb = "tiki_db"
)

$ErrorActionPreference = "Stop"

if (-not $Force) {
    Write-Host "This resets Trino/Iceberg metadata for lakehouse tables."
    Write-Host "Use -Force if MinIO data was deleted or you want a clean rebuild."
    exit 1
}

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Bootstrap = Join-Path $Root "scripts\bootstrap_lakehouse.ps1"

Write-Host "[1/2] Clearing Iceberg JDBC metadata pointers from Postgres..."
$ResetSql = @"
DELETE FROM iceberg_tables
WHERE catalog_name = 'lakehouse';

DELETE FROM iceberg_namespace_properties
WHERE catalog_name = 'lakehouse';
"@
$ResetSql | docker exec -i $PostgresContainer psql -U $PostgresUser -d $PostgresDb

Write-Host "[2/2] Re-registering raw table and lakehouse schemas..."
& $Bootstrap `
    -PostgresContainer $PostgresContainer `
    -TrinoContainer $TrinoContainer `
    -PostgresUser $PostgresUser `
    -PostgresDb $PostgresDb

Write-Host "Reset complete. Run the Airflow DAG or run: cd dbt; dbt run --target trino"

param(
    [string]$PostgresContainer = "tiki_postgres",
    [string]$TrinoContainer = "tiki_trino",
    [string]$PostgresUser = "nhansohoccode",
    [string]$PostgresDb = "tiki_db"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$IcebergSql = Join-Path $Root "trino\init_iceberg_jdbc_catalog.sql"
$RawSql = Join-Path $Root "trino\register_raw_tables.sql"
$SchemaSql = Join-Path $Root "trino\register_tables.sql"

Write-Host "[1/3] Initializing Iceberg JDBC catalog tables in Postgres..."
Get-Content $IcebergSql | docker exec -i $PostgresContainer psql -U $PostgresUser -d $PostgresDb

Write-Host "[2/3] Registering Hive raw.products external table..."
Get-Content $RawSql | docker exec -i $TrinoContainer trino

Write-Host "[3/3] Creating Iceberg lakehouse schemas..."
Get-Content $SchemaSql | docker exec -i $TrinoContainer trino

Write-Host "Bootstrap complete."

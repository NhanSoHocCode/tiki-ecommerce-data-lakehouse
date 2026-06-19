-- Optional one-time setup for Iceberg schemas.
-- dbt-trino creates and updates the Iceberg tables during `dbt run`.

CREATE SCHEMA IF NOT EXISTS lakehouse.staging
WITH (location = 's3://lakehouse/iceberg/staging/');

CREATE SCHEMA IF NOT EXISTS lakehouse.dimensions
WITH (location = 's3://lakehouse/iceberg/dimensions/');

CREATE SCHEMA IF NOT EXISTS lakehouse.facts
WITH (location = 's3://lakehouse/iceberg/facts/');

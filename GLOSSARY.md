# dbt-snowflake-parquet-direct

A dbt package that declares Snowflake Parquet Direct tables as dbt models and keeps them consistent with their declaration.

## Tables and storage

**Parquet Direct table**:
A Snowflake Iceberg table whose catalog integration uses `CATALOG_SOURCE = OBJECT_STORE`, reading Parquet files in place without copying them.
_Avoid_: External table, Parquet table

**Base location**:
The directory, relative to the external volume, that holds the files a Parquet Direct table reads.
_Avoid_: Root path, table path

## Registration

**Registration**:
Snowflake recording a Parquet file as a data file of a Parquet Direct table.
_Avoid_: Ingestion, load, import

**Manual refresh**:
An explicitly issued operation that brings registrations in line with the files under a base location or a refresh path.
_Avoid_: Sync, reload

**Auto refresh**:
Event-driven registration that Snowflake performs from cloud storage notifications.
_Avoid_: Auto-ingest, streaming

**Refresh path**:
A path, relative to the base location, that limits a manual refresh to the files under it.
_Avoid_: Prefix, partition path

**Skipped file**:
A file whose latest registration status is a failure (`SKIPPED`, `REGISTER_FAILED`, or `UNREGISTER_FAILED`), so the table does not match the file in storage.
_Avoid_: Failed file, missing file

## Columns

**Data column**:
A column whose values are read from a Parquet column.
_Avoid_: Physical column, regular column

**Virtual column**:
A column whose values are computed from an expression, such as one over `METADATA$FILENAME`.
_Avoid_: Computed column, derived column

**Partition column**:
A column used to prune files, either inferred from Hive-style paths or computed from `METADATA$FILENAME`.
_Avoid_: Partition key, Hive column

## Changes

**Alterable change**:
A difference between a model's declaration and its existing table that `ALTER` can apply in place.
_Avoid_: Safe change, minor change

**Re-creation-required change**:
A difference between a model's declaration and its existing table that only a re-creation can apply.
_Avoid_: Breaking change, destructive change

**Create-time option**:
A table option that applies only when the table is created or re-created, and is ignored while the table exists.
_Avoid_: Immutable option, static option

**Re-creation**:
Replacing a Parquet Direct table with `CREATE OR REPLACE`, which happens only on a dbt full refresh.
_Avoid_: Rebuild, recreate

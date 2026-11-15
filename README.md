# dbt-snowflake-parquet-direct

A dbt package that declares Snowflake [Parquet Direct](https://docs.snowflake.com/en/user-guide/tables-iceberg-parquet-direct) tables as dbt models. The `parquet_direct` materialization creates the Iceberg table, applies alterable changes with `ALTER`, refreshes it, and fails the run when files are skipped. The package is under development, and Parquet Direct is in Public Preview.

The design and the reasons behind it are in [docs/design.md](docs/design.md). Terms follow [GLOSSARY.md](GLOSSARY.md).

## Usage

Add the package to `packages.yml`, then configure a model. Options live under `config.meta.parquet_direct`, named after the `CREATE ICEBERG TABLE` keywords in lowercase. The model file holds only comments or a `config()` call.

```yaml
models:
  - name: sales
    config:
      materialized: parquet_direct
      full_refresh: false
      meta:
        parquet_direct:
          external_volume: example_volume
          catalog: my_parquet_catalog_integration   # CATALOG_SOURCE = OBJECT_STORE, TABLE_FORMAT = NONE
          base_location: sales/
          columns:
            - name: id
              data_type: number(19,0)
            - name: amount
              data_type: number(12,2)
          partition_columns:
            - name: d
              data_type: date
              expression: "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE"
          refresh_paths:
            - "d={{ var('ds') }}/"
```

```sql
-- models/sales.sql
-- Parquet Direct table. Configured in YAML.
```

Each run:

1. Rejects invalid or contradictory configuration before issuing SQL.
2. Creates the table, or compares it with the declaration. Alterable changes are applied with `ALTER`. A change that needs re-creation stops the run and names `--full-refresh`.
3. With `auto_refresh: false`, refreshes each path in `refresh_paths`. An empty list skips the refresh unless `skip_refresh_if_refresh_paths_empty: false`.
4. Fails when the registration history shows skipped files (`registration_check.on_skipped`, default `error`), and, with `auto_refresh: true`, when auto refresh is not `RUNNING`.

Design sections 3 and 4 list every option and how changes to it are handled.

### Monitoring auto refresh tables

Tables under auto refresh rarely need `dbt run`. Check them on a schedule with the generic test:

```yaml
models:
  - name: sales
    data_tests:
      - dbt_snowflake_parquet_direct.parquet_direct_registration:
          arguments:
            lookback_hours: 24
            # auto_refresh: true   # the expected mode; defaults to the model's auto_refresh
```

Or check one model once:

```bash
dbt run-operation parquet_direct_check_registration --args '{model: sales}'
```

### What the package cannot prevent

- `pattern`, `enable_schema_inference`, `enable_schema_evolution`, `match_by_column_name`, and `refresh_on_create` are create-time options. Snowflake does not report them for an existing table, so a change takes effect only on the next full refresh.
- With schema evolution disabled, a new column in a registered file is ignored and leaves no trace in history.
- With `match_by_column_name: case_sensitive`, a file whose column spelling differs from the established mapping fails to register, and unquoted column names (stored upper-case) do not match lower-case Parquet columns. Use `quote: true` on such columns.
- A refresh path is a prefix: `month=1` also matches `month=10`. End directory paths with `/`.
- Snowflake does not report the `using` columns of a masking policy, so every declared masking policy is re-applied on every run. Removing `using` from a policy that requires it then fails the run, as Snowflake rejects the statement.
- `ignore_file_patterns` run in Python during a model run and in Snowflake in the generic test. Use only syntax both read the same way: literals, `.`, bracket lists such as `[0-9]`, anchors, groups, `|`, and the quantifiers `*`, `+`, `?`, and `{n,m}`.
- Masking policies, row access policies, tags, and comments that the model does not declare are left as they are; the package never removes them. Name policies, tags, and foreign key tables with three parts (`database.schema.name`).

## Development

Install [mise](https://mise.jdx.dev/) and run `mise install`, then use:

- `mise run lint` to check the repository.
- `mise run fix` to apply supported automatic fixes.
- `mise run test` to run the tests.

The test task loads this package into a small dbt project with the DuckDB adapter, parses it, and runs the unit tests in `integration_tests/macros/unit_tests/` with `dbt run-operation run_unit_tests`. The unit tests cover the macros that issue no SQL: validation, DDL generation, change detection, and registration history evaluation.

Tests against Snowflake and S3 live in [integration_tests_snowflake](integration_tests_snowflake/README.md) and need cloud resources to run.

dbt Core uses Python as its runtime; the package itself uses SQL and Jinja.

Licensed under the Apache License, Version 2.0.

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

The [configuration reference](#configuration-reference) lists every option.

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

## Configuration reference

"After creation" says what happens when a value differs from the existing table:

- **ALTER**: applied in place.
- **Re-creation**: the run stops and names `--full-refresh`.
- **Create-time only**: used when the table is created or re-created, and ignored while it exists, because Snowflake does not report it for an existing table.

An option that is not set is left to Snowflake's default and is not compared.

### dbt configs

Set these directly under `config`.

| Config | Values | Default | Meaning |
| --- | --- | --- | --- |
| `materialized` | `parquet_direct` | None | Use this materialization. |
| `full_refresh` | `true`, `false` | dbt's `--full-refresh` flag | `false` ignores `--full-refresh`, so the table is never re-created. Set it on large tables. |
| `on_configuration_change` | `apply`, `continue`, `fail` | `apply` | `apply` runs ALTER for alterable changes and stops on re-creation-required changes. `continue` applies nothing and warns about every difference. `fail` stops on any difference. |
| `grants` | dbt grants | None | Applied after creation or reconciliation, before refresh. |
| `copy_grants` | `true`, `false` | `false` | Adds `COPY GRANTS` when the table is re-created. |
| `persist_docs` | `relation`, `columns` | None | Uses the model and column descriptions as comments when `comment` is not set. |
| `meta.parquet_direct` | mapping | None | The table options below. |

`cluster_by`, `unique_key`, `incremental_strategy`, and `contract.enforced: true` are rejected, because Parquet Direct does not support them. The model's SQL body must contain only comments or a `config()` call.

### Table options (`meta.parquet_direct`)

| Key | Type | Default | Meaning | After creation |
| --- | --- | --- | --- | --- |
| `base_location` | string | Required | `BASE_LOCATION`: the directory relative to the external volume, ending with `/`. | Re-creation |
| `external_volume` | string | Default of the schema, database, or account | `EXTERNAL_VOLUME`. | Re-creation |
| `catalog` | string | Default of the schema, database, or account | `CATALOG`: a catalog integration with `CATALOG_SOURCE = OBJECT_STORE` and `TABLE_FORMAT` `NONE` or `HIVE`. | Re-creation |
| `pattern` | string | None | `PATTERN`: a regular expression that limits the registered files. | Create-time only |
| `iceberg_version` | `2`, `3` | Snowflake default | `ICEBERG_VERSION`. | Re-creation |
| `enable_schema_inference` | boolean | `true` without `columns`, `false` with them | `ENABLE_SCHEMA_INFERENCE`: add columns found in the files at creation. | Create-time only |
| `enable_schema_evolution` | boolean | `false` | `ENABLE_SCHEMA_EVOLUTION`: add columns and widen types on refresh. Requires schema inference. | Create-time only |
| `match_by_column_name` | `case_insensitive`, `case_sensitive` | `case_insensitive` | `MATCH_BY_COLUMN_NAME`. | Create-time only |
| `auto_refresh` | boolean | `false` | `AUTO_REFRESH`: register files from storage event notifications. Requires `AWS_SNS_TOPIC` on the external volume. | ALTER |
| `refresh_on_create` | boolean | `true` | `REFRESH_ON_CREATE`: register existing files at creation. `false` requires `columns`. | Create-time only |
| `replace_invalid_characters` | boolean | Default of the schema, database, or account | `REPLACE_INVALID_CHARACTERS`: replace invalid UTF-8 in query results. | ALTER |
| `log_event_level` | `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`, `OFF` | None | `LOG_EVENT_LEVEL`. Set with ALTER right after creation. | ALTER |
| `comment` | string | Model description with `persist_docs.relation` | `COMMENT`. | ALTER |
| `tag` | mapping of `database.schema.tag` to string | None | `TAG (...)` on the table. | ALTER |
| `row_access_policy` | mapping | None | `ROW ACCESS POLICY ... ON (...)`, see below. | ALTER |
| `columns` | list | Empty | Data columns read from Parquet, see [Columns](#columns). | See [Columns](#columns) |
| `virtual_columns` | list | Empty | Columns computed from an expression. | See [Columns](#columns) |
| `partition_columns` | list | Empty | Columns computed from `METADATA$FILENAME` and listed in `PARTITION BY`. Only with `TABLE_FORMAT = NONE`; HIVE infers them. | See [Columns](#columns) |
| `constraints` | list | Empty | Table constraints, see [Constraints](#constraints). | ALTER |
| `refresh_paths` | list of strings | Empty | With `auto_refresh: false`, paths relative to `base_location` to refresh on every run, including the run that creates the table. A path is a prefix; end directories with `/`. | Not compared |
| `skip_refresh_if_refresh_paths_empty` | boolean | `true` | With `auto_refresh: false` and empty `refresh_paths`: `true` skips the refresh, `false` refreshes the whole `base_location`. | Not compared |
| `registration_check` | mapping | See [Registration check](#registration-check) | How skipped files and auto refresh health are reported. | Not compared |

`row_access_policy` takes:

| Key | Type | Meaning |
| --- | --- | --- |
| `name` | string | The policy as `database.schema.policy`. |
| `on` | non-empty list of column names | The policy arguments. |

### Columns

`columns`, `virtual_columns`, and `partition_columns` share one entry shape. Names must be unique across the three lists, ignoring case.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | Required | The column name. Without `quote`, it must be a valid unquoted identifier and is stored upper-case. |
| `data_type` | string | Required | The Snowflake type, such as `int`, `number(12,2)`, `varchar`, `timestamp_ntz`, or `array(int)`. |
| `expression` | string | None | Required for virtual and partition columns, not allowed for data columns. Partition column expressions must reference `METADATA$FILENAME`. |
| `not_null` | boolean | `false` | `NOT NULL`. Primary key columns are not null without it. |
| `constraints` | list | Empty | Inline constraints on this column: `type`, `rely`, and `references` as in [Constraints](#constraints), without `name` and `columns`. |
| `masking_policy` | string or mapping | None | `MASKING POLICY` as `database.schema.policy`, or a mapping with `name` and `using` (a list of columns). Re-applied with `FORCE` on every run, because Snowflake does not report `using`. |
| `tag` | mapping of `database.schema.tag` to string | None | `TAG (...)` on the column. |
| `comment` | string | Column description with `persist_docs.columns` | Column `COMMENT`. |
| `quote` | boolean | `false` | Quote the name and keep its case. |

When the table exists, differences are handled as follows.

| Difference | Handling |
| --- | --- |
| A declared column is missing, or its type or kind differs | Re-creation |
| A type was widened (int to long, larger decimal precision, also inside nested types) and `enable_schema_evolution` is `true` | No change |
| An undeclared data column exists and schema inference or evolution is on | No change |
| An undeclared column exists otherwise | Re-creation |
| `expression` or `not_null` differs | Re-creation |
| `comment`, `masking_policy` name, or `tag` differs | ALTER |

Expressions are compared the way Snowflake stores them: case and whitespace outside string literals are ignored, and a cast to the column's own type counts as no cast.

### Constraints

Constraints are not enforced by Snowflake. When the model declares constraints, undeclared constraints on the table are dropped and missing ones are added.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `type` | `primary_key`, `unique`, `foreign_key` | Required | The constraint type. |
| `name` | string | Generated by Snowflake | The constraint name. |
| `columns` | non-empty list of column names | Required | The constrained columns. |
| `rely` | boolean | `false` | `RELY`. |
| `references` | mapping | Required for `foreign_key` only | `table` as `database.schema.table` and `columns`, a non-empty list. |

### Registration check

Set under `meta.parquet_direct.registration_check`. A skipped file is a file whose latest registration status is `SKIPPED`, `REGISTER_FAILED`, or `UNREGISTER_FAILED`; a later registration or unregistration resolves it.

| Key | Values | Default | Meaning |
| --- | --- | --- | --- |
| `on_skipped` | `error`, `warn`, `ignore` | `error` | What to do when skipped files exist. |
| `on_unhealthy` | `error`, `warn`, `ignore` | `error` | With `auto_refresh: true`, what to do when auto refresh is off or not `RUNNING`. |
| `lookback_hours` | positive integer | `24` | With `auto_refresh: true`, how far back to read the registration history. Manual refresh checks the current run only. |
| `ignore_file_patterns` | list of regular expressions | Empty | Skipped files whose name matches are reported in the log only. Use only syntax that Python and Snowflake read the same way: literals, `.`, bracket lists such as `[0-9]`, anchors, groups, `|`, and the quantifiers `*`, `+`, `?`, and `{n,m}`. |
| `max_reported_files` | positive integer | `20` | How many skipped files an error or warning lists. The total is always reported. |

### Generic test `parquet_direct_registration`

| Argument | Values | Default | Meaning |
| --- | --- | --- | --- |
| `lookback_hours` | positive integer | `24` | How far back to read the registration history. |
| `ignore_file_patterns` | list of regular expressions | Empty | As in [Registration check](#registration-check). |
| `auto_refresh` | `true`, `false` | The model's `auto_refresh` | The expected mode. The test fails when auto refresh is on for a manual table, or off or not `RUNNING` for an auto refresh table. Required when the model cannot be found by its relation name. |

The test does not support the `where` config.

### Run-operation `parquet_direct_check_registration`

```bash
dbt run-operation parquet_direct_check_registration --args '{model: sales, lookback_hours: 6}'
```

| Argument | Values | Default | Meaning |
| --- | --- | --- | --- |
| `model` | model name | Required | The `parquet_direct` model to check, with its `registration_check` settings. |
| `lookback_hours` | positive integer | The model's `registration_check.lookback_hours`, else `24` | How far back to read the registration history. |

### Validation

Configuration errors stop the run before any SQL and list every violation at once. Snowflake's own rules are checked first: for example, `enable_schema_evolution: true` with `enable_schema_inference: false`, `refresh_on_create: false` without `columns`, `refresh_paths` with `auto_refresh: true`, or `partition_columns` with a `TABLE_FORMAT = HIVE` catalog. The full list is in [docs/design.md](docs/design.md), section 6.

## Development

Install [mise](https://mise.jdx.dev/) and run `mise install`, then use:

- `mise run lint` to check the repository.
- `mise run fix` to apply supported automatic fixes.
- `mise run test` to run the tests.

The test task loads this package into a small dbt project with the DuckDB adapter, parses it, and runs the unit tests in `integration_tests/macros/unit_tests/` with `dbt run-operation run_unit_tests`. The unit tests cover the macros that issue no SQL: validation, DDL generation, change detection, and registration history evaluation.

Tests against Snowflake and S3 live in [integration_tests_snowflake](integration_tests_snowflake/README.md) and need cloud resources to run.

dbt Core uses Python as its runtime; the package itself uses SQL and Jinja.

Licensed under the Apache License, Version 2.0.

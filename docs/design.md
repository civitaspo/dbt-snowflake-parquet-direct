# Design: the `parquet_direct` materialization

- Status: accepted
- Date: 2026-11-15

This package provides a custom materialization, `parquet_direct`, that declares a Snowflake Parquet Direct table as a dbt model. A model's configuration can express every option of `CREATE ICEBERG TABLE` for Parquet files. The materialization rejects contradictory settings before it issues any SQL. It applies alterable changes with `ALTER` and stops with an error on re-creation-required changes. It also fails the run when a refresh leaves skipped files.

Terms follow [GLOSSARY.md](../GLOSSARY.md).

## 1. Goals and scope

Goals:

- Write Parquet Direct DDL as model configuration that mirrors the SQL syntax.
- Compare the declaration with the existing table on every run. Apply alterable changes, and stop on re-creation-required changes with a message that names `--full-refresh`.
- Reject contradictory or ineffective settings before issuing SQL.
- Detect skipped files, which a successful refresh does not reveal, and fail or warn.

| Scope | Contents |
| --- | --- |
| In | The `parquet_direct` materialization, configuration validation, change detection and `ALTER`, manual refresh, registration checks, grants |
| Out | Creating external volumes, catalog integrations, or storage notifications; writing Parquet files; converting to Snowflake-managed Iceberg tables |
| Later | Reconciling registered files with a list of expected files; monitoring through the event table |

The package targets dbt Core 1.10 or later with dbt-snowflake, matching `require-dbt-version` in `dbt_project.yml`. Parquet Direct is in Public Preview as of November 2026, so the design keeps Snowflake-specific behavior in a few macros that are easy to update.

## 2. Parquet Direct behavior that shapes the design

Two facts drive the design: many options cannot change after creation, and a successful refresh can still leave skipped files. "Docs" refers to Snowflake documentation. "Observed" refers to an evaluation in November 2026, recorded in the [appendix](#appendix-observed-behavior). Integration tests in this repository will replace the observations over time.

| Behavior | Source | Effect on the design |
| --- | --- | --- |
| A table is a Parquet Direct table because its catalog integration uses `CATALOG_SOURCE = OBJECT_STORE`. `CREATE ICEBERG TABLE` has no dedicated keyword. | Docs | Inspect the catalog integration before running DDL. |
| With `TABLE_FORMAT = HIVE`, partition columns cannot be declared. With `NONE`, they are expressions over `METADATA$FILENAME` listed in `PARTITION BY`. | Docs | Whether `partition_columns` is allowed depends on the catalog integration. |
| `ENABLE_SCHEMA_INFERENCE` applies only at creation. `SET` after creation fails with `Unsupported feature`. | Docs, observed | It takes effect only through re-creation. |
| `ENABLE_SCHEMA_EVOLUTION` can only go from TRUE to FALSE after creation. Combining it with inference FALSE fails. | Docs, observed | Validate the combination statically. |
| `ADD COLUMN` (including virtual columns), `DROP COLUMN`, `ALTER COLUMN ... SET DATA TYPE`, `SET` / `DROP NOT NULL`, `SET PATTERN`, and `ADD` / `DROP` / `REPLACE PARTITION BY` are rejected. | Observed | These changes require re-creation. |
| Column and table comments, `ADD` / `DROP CONSTRAINT`, masking policies, tags, row access policies, `AUTO_REFRESH`, `REPLACE_INVALID_CHARACTERS`, and `LOG_EVENT_LEVEL` accept `ALTER`. | Docs, observed | These are alterable changes. |
| `REFRESH_ON_CREATE = FALSE` requires column definitions. | Docs | Validate statically. |
| A manual refresh is not allowed while `AUTO_REFRESH = TRUE`. Turning auto refresh on runs an asynchronous backfill. | Docs | `auto_refresh` and `refresh_paths` are mutually exclusive. |
| A manual refresh takes one path per statement. Paths match by prefix, so `month=1` also matches `month=10`. It cannot run inside a transaction. | Docs | Issue one statement per refresh path, outside transactions. |
| For a nonexistent refresh path, the docs say the whole base location is refreshed. In practice nothing was registered. | Docs, observed | Log the number of changes per path so either outcome is visible. |
| Under auto refresh, a file with a conflicting type becomes `SKIPPED` (`Column registration failed.`) while `executionState` stays `RUNNING`. | Observed | Check registration history, not only the state. |
| Under manual refresh, a type conflict fails the statement, and a bulk refresh then registers none of the compatible files either. A broken file becomes `SKIPPED` while the statement succeeds. | Observed | Check registration history after manual refreshes too. |
| With schema evolution disabled, a file with a new column registers successfully and the new column is silently ignored. | Observed | Configuration cannot prevent this. Document it. |
| With schema evolution enabled, refreshes add columns and widen types such as int to long. | Docs, observed | Change detection treats these as no change. |
| `CREATE OR REPLACE` is atomic, but drops table history and makes streams stale. Re-creation registers every file again, at 0.35 credits per TB added from 2026-12-01. | Docs | Re-creation is expensive. Prevent it unless intended. |
| `CLUSTER BY`, DML, cloning, `DATA_RETENTION_TIME_IN_DAYS`, and `UNDROP` are unsupported. | Docs | Reject the matching dbt configuration. |
| `PATTERN`, `ENABLE_SCHEMA_INFERENCE`, `ENABLE_SCHEMA_EVOLUTION`, and `MATCH_BY_COLUMN_NAME` do not appear in `GET_DDL`, `SHOW ICEBERG TABLES`, or `SHOW PARAMETERS`. | Observed | They are create-time options: used at creation and re-creation, ignored while the table exists. |
| With `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, a file whose column spelling differs from the established mapping (such as `Id` after `ID`) fails to register. | Observed | Document it. Unquoted column names do not match lower-case Parquet columns in this mode. |

`EXTERNAL_VOLUME` and `CATALOG` can default from the schema, database, or account, in that order. When a model omits them, the materialization resolves the effective values and validates those.

References: [CREATE ICEBERG TABLE (Parquet files)](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-parquet), [Parquet Direct](https://docs.snowflake.com/en/user-guide/tables-iceberg-parquet-direct), [ALTER ICEBERG TABLE](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table), [ALTER ICEBERG TABLE ... REFRESH](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table-refresh).

## 3. Interface

The materialization is named `parquet_direct`. Its DDL options live under `config.meta.parquet_direct`, and each key is the DDL keyword in lowercase. New Snowflake options then need no naming discussion.

### Where configuration lives

Since dbt 1.10, custom keys directly under `config` raise `CustomKeyInConfigDeprecation`, and custom attributes belong in `meta` ([Deprecations](https://docs.getdbt.com/reference/deprecations)). dbt Fusion and later dbt Core versions are stricter, so the package does not use the deprecated form.

| Location | Contents | Read with |
| --- | --- | --- |
| Directly under `config` | Standard dbt keys: `materialized`, `full_refresh`, `on_configuration_change`, `grants`, `copy_grants`, `persist_docs`, `snowflake_warehouse`, hooks | `config.get()` |
| `config.meta.parquet_direct` | DDL options, column definitions, refresh and registration check settings | `config.meta_get('parquet_direct')` |

An unknown key under `meta.parquet_direct` is an error. `meta` is free-form, so the package must catch typos itself.

### Model files

The model's SQL body is not used. The file contains only a `config()` call and comments, and anything else is an error. This prevents the misunderstanding that a `SELECT` in the body does something.

```yaml
# models/raw/_raw.yml
models:
  - name: sales
    config:
      materialized: parquet_direct
      full_refresh: false            # forbid re-creation of a large table
      on_configuration_change: apply
      grants:
        select: [analyst]
      meta:
        parquet_direct:
          external_volume: example_volume
          catalog: my_parquet_catalog_integration   # TABLE_FORMAT = NONE
          base_location: sales/
          pattern: '.*[.]parquet'
          enable_schema_inference: false
          enable_schema_evolution: false
          match_by_column_name: case_insensitive
          auto_refresh: false
          refresh_on_create: true
          replace_invalid_characters: false
          columns:
            - name: id
              data_type: number(19,0)
              not_null: true
            - name: amount
              data_type: number(12,2)
              masking_policy: governance.policies.mask_amount
          virtual_columns:
            - name: source_file
              data_type: varchar
              expression: METADATA$FILENAME
          partition_columns:
            - name: d
              data_type: date
              expression: "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE"
          tag:
            governance.tags.domain: sales
          refresh_paths:
            - "d={{ var('ds') }}/"
          registration_check:
            on_skipped: error
```

```sql
-- models/raw/sales.sql
-- Parquet Direct table. Configured in _raw.yml.
```

### DDL options

"After creation" says how a difference from the existing table is handled:

- **`ALTER`**: an alterable change, applied in place.
- **Re-creation**: a re-creation-required change. The run stops unless it is a full refresh.
- **Create-time only**: a create-time option. It is used in `CREATE` at creation and re-creation, and ignored while the table exists. These options either cannot be read back from an existing table or only matter at creation, so they cannot be compared. Changing one takes effect on the next full refresh.

| Key | DDL | Type | Default | After creation |
| --- | --- | --- | --- | --- |
| `external_volume` | `EXTERNAL_VOLUME` | string | Inherited default | Re-creation |
| `catalog` | `CATALOG` | string | Inherited default | Re-creation |
| `base_location` | `BASE_LOCATION` | string (required) | None | Re-creation |
| `pattern` | `PATTERN` | string | None | Create-time only |
| `iceberg_version` | `ICEBERG_VERSION` | 2 or 3 | Snowflake default | Re-creation (Parquet Direct does not support `UPGRADE`) |
| `enable_schema_inference` | `ENABLE_SCHEMA_INFERENCE` | bool | TRUE without columns, FALSE with columns | Create-time only |
| `enable_schema_evolution` | `ENABLE_SCHEMA_EVOLUTION` | bool | FALSE | Create-time only |
| `match_by_column_name` | `MATCH_BY_COLUMN_NAME` | `case_insensitive` or `case_sensitive` | `case_insensitive` | Create-time only |
| `auto_refresh` | `AUTO_REFRESH` | bool | FALSE | `ALTER` |
| `refresh_on_create` | `REFRESH_ON_CREATE` | bool | TRUE | Create-time only |
| `replace_invalid_characters` | `REPLACE_INVALID_CHARACTERS` | bool | Inherited value | `ALTER` |
| `log_event_level` | `LOG_EVENT_LEVEL` | string | None | `ALTER` |
| `columns`, `virtual_columns`, `partition_columns` | Column definitions and `PARTITION BY` | list | Empty | Section 4 |
| `constraints` | Out-of-line constraints | list | Empty | `ALTER` (`ADD` / `DROP CONSTRAINT`) |
| `row_access_policy` | `ROW ACCESS POLICY ... ON (...)` | `{name, on}` | None | `ALTER` |
| `tag` | `TAG (...)` | map | None | `ALTER` |
| `comment` | `COMMENT` | string | The model `description` when `persist_docs.relation` is on | `ALTER` |

The materialization decides whether to add `OR REPLACE` or `IF NOT EXISTS`, so configuration does not expose them. `iceberg_version` is not listed on the syntax page, but it was accepted in practice. Enum values are case-insensitive and are upper-cased in generated DDL.

`MATCH_BY_COLUMN_NAME` and `ENABLE_SCHEMA_EVOLUTION` (TRUE to FALSE) accept `ALTER`, but their current values cannot be read, so the package cannot tell whether an `ALTER` is needed. Treating them as create-time options keeps their behavior the same as the other unreadable options.

### Package options

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `refresh_paths` | list[string] | Empty | With `auto_refresh: false`, the refresh paths to refresh on every run, including the run that creates the table. `var()` can supply dates. When empty, `skip_refresh_if_refresh_paths_empty` decides. |
| `skip_refresh_if_refresh_paths_empty` | bool | TRUE | With `auto_refresh: false` and empty `refresh_paths`: TRUE skips the manual refresh, FALSE refreshes the whole base location. Also applies right after creation. |
| `registration_check` | object | Section 7 | Checks for skipped files and auto refresh health |
| `previous_versions` | list[object] | Empty | Tables that hold files written before an incompatible type change, combined with the current table in a view (section 8) |

## 4. Column definitions

The three column kinds share one entry shape. They differ only in whether `expression` is required and where they are allowed. DDL lists data columns, then virtual columns, then partition columns. `PARTITION BY (...)` follows the order of `partition_columns`.

### Entry attributes

| Attribute | `columns` | `virtual_columns` | `partition_columns` | DDL |
| --- | --- | --- | --- | --- |
| `name` | Required | Required | Required | Column name |
| `data_type` | Required | Required | Required | Type |
| `expression` | Not allowed | Required | Required, references `METADATA$FILENAME` | `AS (<expr>)` |
| `not_null` | Optional | Optional | Optional | `NOT NULL` |
| `constraints` | Optional | Optional | Optional | Inline `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY`, including `RELY` |
| `masking_policy` | Optional | Optional | Optional | `MASKING POLICY <name> [USING (...)]` |
| `tag` | Optional | Optional | Optional | Column `TAG (...)` |
| `comment` | Optional | Optional | Optional | Column `COMMENT`. Defaults to `columns[].description` in YAML when `persist_docs.columns` is on. |
| `quote` | Optional | Optional | Optional | Quote the column name. Off by default. |

Snowflake does not enforce constraints. `DEFAULT`, `AUTOINCREMENT`, and `IDENTITY` are unsupported, so there are no attributes for them.

### Rules per kind

- **Data columns** map to Parquet columns. The list can be empty only when schema inference is on and `refresh_on_create` is TRUE. With inference on, Snowflake adds columns beyond those declared.
- **Virtual columns** are computed from an expression. A common use is keeping the source file name through `METADATA$FILENAME`.
- **Partition columns** are allowed only when the catalog integration uses `TABLE_FORMAT = NONE`. With `HIVE`, Snowflake infers them from `key=value` paths, so declaring them is an error. Expressions may use only the functions Snowflake allows, such as `SPLIT_PART`, `SUBSTR`, `CAST`, and `CASE`. The package checks only that the expression contains `METADATA$FILENAME` and leaves function checks to Snowflake.
- Names must be unique across all three lists, ignoring case. Snowflake also rejects columns that differ only in case.

### Comparing with the existing table

When the declaration is compared with the columns from `DESC ICEBERG TABLE`, changes that Snowflake makes on its own are not differences.

| Existing table | Condition | Result |
| --- | --- | --- |
| Has a column not in the declaration | Inference or schema evolution is declared on, or it is a Hive partition column | No change (logged) |
| Has a column not in the declaration | Otherwise | Re-creation-required change |
| Lacks a declared column | Any | Re-creation-required change |
| A type is wider (int to long, float to double, wider decimal precision) | Schema evolution is declared on | No change (logged) |
| A type differs | Otherwise | Re-creation-required change |
| `comment`, `masking_policy`, `tag`, or a constraint differs | Any | Alterable change |

Masking policies, row access policies, tags, and comments that the model does not declare are left as they are, so the package never removes governance settings applied outside dbt. A masking policy is replaced with `SET MASKING POLICY ... FORCE`, so the column is never unmasked in between. `POLICY_REFERENCES` returns no `USING` columns for masking policies, so adding, changing, or removing `using` cannot be detected. Every declared masking policy is therefore re-applied with `FORCE` on every run that applies changes. A re-application is not a difference: the run still reports `unchanged`, and `on_configuration_change: fail` does not fail because of it. Under `continue`, when other differences exist, nothing is applied, re-applications included. Constraints are managed only when the model declares some; then undeclared constraints are dropped. A constraint matches by type, columns, references, and `RELY`, and also by name when the model declares one; a constraint without a declared name keeps the name Snowflake generated.
| `expression` or `not_null` differs | Any | Re-creation-required change |

The declared values of create-time options decide these conditions. If a create-time option was changed without a full refresh, the table can drift from the declaration, for example by gaining columns. The comparison then reports a re-creation-required change, which prompts the full refresh that applies the option.

Type comparison first normalizes aliases such as `int` and `number(10,0)`, or `string` and `varchar(134217728)`, element by element inside `OBJECT`, `ARRAY`, and `MAP`. The normalization table follows Snowflake's Iceberg type mapping. A type counts as widened only when every element matches except a `NUMBER` whose precision grew at the same scale.

Expression comparison folds case and whitespace outside string literals and quoted identifiers, which are compared verbatim, removes wrapping parentheses, and reads a trailing `x::TYPE` as `CAST(x AS TYPE)`. An outermost cast to the column's own type is removed: Snowflake drops a cast to the type the expression already returns and keeps other casts as `CAST(... AS TYPE)`, and such a cast does not change the column's value. Primary key columns are expected to be `NOT NULL`, because Snowflake makes them so without `not_null: true`. The [appendix](#integration-tests-on-2026-11-15) records both observations.

## 5. Lifecycle

Each run goes through static validation, pre-flight checks, creation or reconciliation, refresh, and registration checks, in that order. Every failure that can happen before DDL happens before DDL.

1. **Static validation**: Check unknown keys, types, combinations, column definitions, and path shapes using configuration only (section 6). No SQL is issued.
2. **Pre-flight checks** (read-only):
    - Look up the catalog integration and confirm `CATALOG_SOURCE = OBJECT_STORE` and its `TABLE_FORMAT`. When `catalog` is omitted, use the value resolved from the schema, database, or account.
    - Look up any object with the same name. If it is a table or view that is not a Parquet Direct table, fail even with `--full-refresh`.
3. **Creation** (no table yet): Issue `CREATE ICEBERG TABLE`.
4. **Re-creation** (`--full-refresh`, unless `full_refresh` is false): Issue `CREATE OR REPLACE ICEBERG TABLE`, with `COPY GRANTS` when dbt-snowflake's standard `copy_grants` config is true (default false). When the model sets `full_refresh: false`, dbt's standard behavior ignores `--full-refresh`, and the run continues with reconciliation.
5. **Reconciliation** (table exists): Read the table state, compare it with the declaration, and classify each difference as an alterable change or a re-creation-required change. Create-time options are not compared. `on_configuration_change` selects the behavior:
    - `apply` (default): If any re-creation-required change exists, fail without issuing any `ALTER`. Otherwise issue the `ALTER` statements.
    - `continue`: Issue nothing and fail on nothing. Log every difference as a warning and move on. Meant for temporary use during incidents.
    - `fail`: Fail on any difference.
6. **Refresh**: With `auto_refresh: false`, issue `ALTER ICEBERG TABLE ... REFRESH '<path>'` for each refresh path in order. With `auto_refresh: true`, issue no manual refresh. This step also runs right after creation and re-creation. When `refresh_paths` is empty, `skip_refresh_if_refresh_paths_empty` (default TRUE) decides.
7. **Registration checks**: Check registration history for skipped files and check auto refresh health (section 7).
8. **Post-processing**: Record a summary in the log and in `run_results.json`: files registered, unregistered, and skipped. Grants are applied earlier, right after creation or reconciliation, so a refresh or registration failure never leaves a new table without them.

A manual refresh cannot run inside a transaction, so the materialization does not wrap its statements in `BEGIN` and `COMMIT`. Every statement is idempotent, so rerunning the model recovers from a failure partway through.

### Adjustments at creation

When `refresh_on_create: false` and `auto_refresh: true` are set together, the materialization creates the table with `AUTO_REFRESH = FALSE` and then issues `ALTER ... SET AUTO_REFRESH = TRUE`. Snowflake recommends this sequence for large tables because it registers existing files through an asynchronous backfill. It works whether or not creating with `AUTO_REFRESH = TRUE` directly would also backfill, which is still unverified (section 11).

With `refresh_on_create: false`, a re-created table holds only the files under `refresh_paths` right after re-creation. If `refresh_paths` lists only a date path, older files are not registered. Under auto refresh, only part of the data is visible until the backfill finishes. The run logs a warning in this case.

## 6. Fail-first validation

Validation runs in three stages. Each stage collects every violation before failing, instead of stopping at the first. Three principles decide the rules:

- Reject combinations that Snowflake would reject, before issuing SQL.
- Reject settings that would have no effect, instead of silently ignoring them.
- Treat a change of unknown alterability as re-creation-required.

### Stage 1: static validation (no SQL)

| ID | Condition | Reason |
| --- | --- | --- |
| S01 | An unknown key under `meta.parquet_direct` | Catches typos. The error lists the valid keys. |
| S02 | An invalid type or enum value, or a policy, tag, or foreign key table that is not a three-part name | Snowflake resolves unqualified names in the session schema, which can differ from the model's schema, so only three-part names compare reliably. |
| S03 | `base_location` is missing, starts with `/`, contains `..`, has a scheme such as `s3://`, or does not end with `/` | It must be a directory relative to the external volume. |
| S04 | `enable_schema_evolution: true` with `enable_schema_inference: false` | Snowflake rejects it. |
| S05 | `refresh_on_create: false` with empty `columns` | Snowflake rejects it. |
| S06 | Empty `columns` with `enable_schema_inference: false` | The table would have no columns. |
| S07 | `auto_refresh: true` with `refresh_paths` or `skip_refresh_if_refresh_paths_empty` set | A manual refresh is not allowed under auto refresh. |
| S08 | `refresh_paths` contains an empty string, `..`, or duplicates | None |
| S09 | A data column has `expression`, or a virtual or partition column lacks it | None |
| S10 | A partition column expression does not contain `METADATA$FILENAME` | Snowflake requires it. |
| S11 | Column names repeat across the three lists, ignoring case | Snowflake requires unique names. |
| S12 | `constraints` or `row_access_policy.on` references an undeclared column | None |
| S13 | `registration_check.lookback_hours` with `auto_refresh: false` | Manual refresh does not use it. |
| S14 | The compiled model body contains anything besides comments (`config()` renders to nothing) | The body is not used. |
| S15 | Unsupported dbt configuration: `cluster_by`, `unique_key`, `incremental_strategy`, or `contract.enforced: true` | It has no effect or is unsupported on Parquet Direct. `transient` is not checked, because projects often set it for every model. |
| S16 | `previous_versions` without `columns`, a table listed twice, or an expression for an undeclared column | The combined view exposes the declared columns. |
| S17 | `previous_versions` with `auto_refresh: true` | Auto refresh next to a previous version's files is not verified. |

Prefix matching of refresh paths is documented Snowflake behavior, so a refresh path without a trailing `/` is allowed. The README explains the prefix behavior.

### Stage 2: pre-flight checks (read-only)

| ID | Condition | Reason |
| --- | --- | --- |
| R01 | The catalog integration does not exist, or it is not `CATALOG_SOURCE = OBJECT_STORE` with `TABLE_FORMAT` `NONE` or `HIVE` | Object storage integrations also serve Delta and Iceberg metadata, which are not Parquet Direct. |
| R02 | `TABLE_FORMAT = HIVE` with `partition_columns` | Snowflake rejects it. |
| R03 | An object with the same name is a table or view that is not a Parquet Direct table | Never replace an unrelated object. A combined view left after removing `previous_versions` also stops here. |
| R04 | `--full-refresh` re-creates an existing table with `refresh_on_create: false` | Warning: only the files under `refresh_paths` are registered. |
| R05 | A previous version is the model's own name or its current table | They would read themselves. |
| R06 | A previous version table does not exist | The error prints the `RENAME` statement for the table that holds its files: the model's table, else the current table. |
| R07 | The model's name is still a table although every previous version exists | The combined view needs the name. |
| R08 | The current table is created with `refresh_on_create: true` while a previous version's base location overlaps its own | Creation would register the old files, and Snowflake rejects their type. |
| R09 | A previous version's column has neither the declared type, a widening of it, nor an expression | Converting types needs a decision only the user can make. |

### Stage 3: differences from the existing table

Section 3 (After creation) and section 4 (comparison table) classify the differences. Create-time options and omitted options, such as an `external_volume` inherited from the schema, are not compared. A re-creation-required change produces an error like this:

```text
Parquet Direct table ANALYTICS.RAW.SALES has changes that ALTER cannot apply:
  - base_location: 'sales/' -> 'sales_v2/'
  - columns: column AMOUNT type NUMBER(12,2) -> NUMBER(12,3)
Changes that ALTER can apply (not applied):
  - auto_refresh: false -> true
Run `dbt run --full-refresh -s sales` to re-create the table.
Re-creating re-registers every file (data registration charges apply),
drops the table history, and makes streams on the table stale.
This model sets full_refresh: false; remove it before running --full-refresh.
```

The last line appears only for models with `full_refresh: false`.

### Sources of table state

Reconciliation reads the existing table from these sources, as confirmed on 2026-11-15:

- `SHOW ICEBERG TABLES`: `external_volume_name`, `catalog_name`, `base_location`, `comment`
- `SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS`: whether auto refresh is on. It returns `Auto refresh is not enabled for table: ...` when it is off.
- `SHOW PARAMETERS IN TABLE`: `REPLACE_INVALID_CHARACTERS`, `ICEBERG_VERSION`, and `LOG_EVENT_LEVEL`. A value set on the table has `level = TABLE`. Others are inherited.
- `DESC ICEBERG TABLE`: columns, types, `kind` (`COLUMN`, `VIRTUAL`, or `PARTITION`), `expression`, nullability, and column comments
- `SHOW PRIMARY KEYS`, `SHOW UNIQUE KEYS`, and `SHOW IMPORTED KEYS`: constraints, including `RELY`
- `POLICY_REFERENCES`, `TAG_REFERENCES`, and `TAG_REFERENCES_ALL_COLUMNS`: masking policies, row access policies, and tags

Policies, tags, and constraints are read only when the model declares them. `PARTITION BY` order is not compared: every partition column is in `PARTITION BY`, and `DESC ICEBERG TABLE` already reports each column's kind.

`SHOW ICEBERG TABLES` reported `iceberg_table_format_version = 2` for a table created with `ICEBERG_VERSION = 3`, while `SHOW PARAMETERS` reported 3. The comparison uses `SHOW PARAMETERS`.

## 7. Refresh and skipped-file detection

The check groups registration history by file and treats a file whose latest status is a failed status as a skipped file. The failed statuses are `REGISTER_FAILED` and `UNREGISTER_FAILED`, which the [documentation](https://docs.snowflake.com/en/sql-reference/functions/iceberg_table_file_registration_history) lists, and `SKIPPED`, which the November 2026 evaluation observed. `REGISTER_SKIPPED` means the file was already registered, and `REGISTER_SKIPPED_PATTERN_MISMATCH` means `PATTERN` excludes it, so neither is a failure. By default, any skipped file fails the model. Manual refresh and auto refresh differ only in where the check window starts.

### Manual refresh (`auto_refresh: false`)

1. Record the start time with `SELECT CURRENT_TIMESTAMP()`.
2. Issue `ALTER ICEBERG TABLE <t> REFRESH '<path>'` for each refresh path in order. When `refresh_paths` is empty, skip this when `skip_refresh_if_refresh_paths_empty` is TRUE (the default), or issue one refresh without a path when it is FALSE. This also runs right after creation and re-creation. If a statement fails, for example on a type conflict, fail the model there.
3. Check registration history since the start time. A broken file becomes `SKIPPED` even when the statement succeeds, and this step catches it.
4. Log the counts of registered, unregistered, and skipped files per refresh path. The docs and observed behavior disagree on nonexistent paths, so paths with zero changes are visible in the log.

### Auto refresh (`auto_refresh: true`)

1. Call `SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS`. If `executionState` is not `RUNNING` (`STALLED` or `STOPPED`), follow `on_unhealthy`. The error includes `channelErrorMessage` and `invalidExecutionStateReason`.
2. Check registration history from `lookback_hours` before now.
3. Log `pendingFileCount`. A count of zero does not prove that every file is registered, because notifications can be delayed or lost.

Set `lookback_hours` to at least twice the job interval, so a skipped run does not hide skipped files. A skipped file is reported on every run until it is resolved or leaves the window.

### When a skipped file is resolved

A later `REGISTERED_NEW`, `REGISTERED_UPDATE`, or `UNREGISTERED` for the same file resolves it. In the evaluation, replacing the file with compatible content and refreshing again recorded `REGISTERED_NEW` after `SKIPPED`.

`ICEBERG_TABLE_FILE_REGISTRATION_HISTORY` accepts only `TABLE_NAME` and `START_TIME`, and returns `OPERATION_TIME`, `FILE_NAME`, `OPERATION_STATUS`, `MESSAGE`, `FILE_SIZE`, `ROW_COUNT`, and `LAST_MODIFIED`. The observed statuses are `REGISTERED_NEW`, `UNREGISTERED`, and `SKIPPED`. The documentation lists `REGISTERED_NEW`, `REGISTERED_UPDATE`, `REGISTER_SKIPPED`, `REGISTER_SKIPPED_PATTERN_MISMATCH`, `REGISTER_FAILED`, `UNREGISTERED`, and `UNREGISTER_FAILED` instead of `SKIPPED`, so the check accepts both sets. History goes back at most 65 days. An overwritten file appears as `UNREGISTERED` followed by `REGISTERED_NEW`. A history of 1,201 events was returned in full, and rows appeared immediately after a manual refresh.

```sql
with history as (
    select
        file_name,
        operation_status,
        message,
        operation_time,
        row_number() over (partition by file_name order by operation_time desc) as recency_rank
    from table({{ database }}.information_schema.iceberg_table_file_registration_history(
        table_name => '{{ database }}.{{ schema }}.{{ identifier }}',
        start_time => {{ since }}
    ))
)
select file_name, message, operation_time
from history
where recency_rank = 1
  and operation_status = 'SKIPPED'
order by operation_time
```

### Settings

| Key | Values | Default | Meaning |
| --- | --- | --- | --- |
| `registration_check.on_skipped` | `error`, `warn`, `ignore` | `error` | What to do when skipped files exist |
| `registration_check.on_unhealthy` | `error`, `warn`, `ignore` | `error` | What to do when `executionState` is not `RUNNING` (auto refresh only) |
| `registration_check.lookback_hours` | integer | 24 | How far back to read history under auto refresh |
| `registration_check.ignore_file_patterns` | list[regex] | Empty | Excludes known bad files. Model runs evaluate patterns in Python and the generic test in Snowflake, so validation rejects syntax the two read differently: backslash letter classes such as `\d`, POSIX classes such as `[[:digit:]]`, `(?` groups, and lazy quantifiers. |
| `registration_check.max_reported_files` | integer | 20 | Maximum files listed in an error or warning. The total is always reported. |

### Checks outside model runs

A table under auto refresh rarely needs `dbt run` after creation. The same check is therefore available through two more entry points for scheduled monitoring. Both use the same macro as the materialization.

- The generic test `parquet_direct_registration` runs in `dbt test` and `dbt build`. Its severity uses dbt's standard `severity` and `warn_if`. It also fails when the table's auto refresh mode differs from the expected one, taken from the model's `auto_refresh` unless the test's `auto_refresh` argument sets it, so a table whose auto refresh was turned off by mistake is caught. The argument must be a boolean. Without it, the model is found by its exact `relation_name`, so quoted names that differ only in case stay distinct, and the test fails to compile unless exactly one model matches. It does not support the `where` config.
- Both entry points require `lookback_hours` to be a positive integer.
- `dbt run-operation parquet_direct_check_registration --args '{model: sales}'` serves one-off investigation.

## 8. Previous versions

Some type changes cannot be applied to a Parquet Direct table, such as a decimal scale change or an integer becoming a boolean. Files written after such a change fail to register (appendix: type widening and conflicts), and re-creation does not help, because the files written before the change keep the old type. `previous_versions` keeps both readable under the model's name. Incompatible changes are rare, so a model without them stays a single table, and the view appears only when a model declares a previous version.

```yaml
columns:
  - name: is_active
    data_type: boolean
refresh_on_create: false
refresh_paths: ["d={{ var('ds') }}/"]
previous_versions:                       # oldest first
  - table: customers_before_boolean
    columns:
      is_active: "case is_active when 0 then false when 1 then true end"
```

| Object | Name | Built by |
| --- | --- | --- |
| Combined view | The model's name | The package, on every run |
| Current table | `<model>__current` | The package, like a model without previous versions |
| Previous version | `previous_versions[].table`, in the model's schema | The user, by renaming the table that read the old files |

### Moving to a previous version

1. A run fails because files with the new type do not register.
2. The user declares the new type, adds the previous version with expressions for the changed columns, and runs. The run stops with R06 and prints the `ALTER ICEBERG TABLE ... RENAME TO ...` statement. The package never renames an object that consumers may query; the user decides when the name moves.
3. The user runs the statement. The next run creates the current table, builds the combined view, refreshes `refresh_paths`, and checks registration.

A later incompatible change repeats the steps with the current table as the table to rename, and the new previous version is listed last.

### The combined view

- Its columns are the declared columns, in order. For each previous version, a declared column comes from its expression, else from the same-named column if that column has the declared type or a widening of it, else NULL when the table lacks it. Any other type stops the run with R09 instead of relying on an implicit conversion, because only the user knows whether `0` means `false`.
- Each previous version's `SELECT` casts every column to its declared type, and `UNION ALL BY NAME` combines them with the current table. Each `SELECT` is compiled with `EXPLAIN` before anything is created, so a wrong expression fails first.
- Masking policies, row access policies, tags, and comments declared on the model are part of the `CREATE OR REPLACE VIEW ... COPY GRANTS` statement. Policies set with `ALTER` are dropped when a view is replaced, while policies in the statement and copied grants survive (appendix: verification on 2026-11-16), so the view is never unprotected. A masking policy's signature has the declared type, which matches the view and not necessarily a previous version.
- The current table gets the constraints only. Previous versions keep what they had before the rename. dbt `grants` apply to the view, which reads the tables with its owner's privileges.
- The registration check, the generic test, and the run-operation read the current table. Previous versions are never refreshed or altered.
- The current table may share the base location of a previous version only with `refresh_on_create: false` (R08) and refresh paths that hold new files: a refresh without a path fails on the old files.
- Removing `previous_versions` is not supported. The view stays, and R03 stops the run.

## 9. Operational features

The first release (P0) prevents unintended re-creation and unnoticed skipped files. Features that matter once operations start come in P1 and later.

| Priority | Feature | Problem it prevents |
| --- | --- | --- |
| P0 | Re-creation safeguard: honor dbt's `full_refresh: false`, and state the impact in errors (registration charges, lost history, stale streams) | Accidental re-registration and charges on large tables |
| P0 | Protect existing objects: never replace an object that is not a Parquet Direct table (R03) | Dropping another table through a configuration mistake |
| P0 | Run summary: counts of registered, unregistered, and skipped files, the `ALTER` statements issued, and `pendingFileCount`, in the log and `run_results.json` | Runs that succeed while registering nothing |
| P0 | Grants: use dbt's `grants` and reapply them after re-creation | Lost privileges after re-creation |
| P0 | Schema change log: record columns added and types widened by schema evolution (section 4) | Unexpected effects on downstream models |
| P1 | Repair operation `parquet_direct_repair`: disable auto refresh, manually refresh the given paths, and re-enable auto refresh | Re-registering skipped files after fixing them, using the sequence observed to work |
| P1 | Auto refresh backlog detection: limits on `pendingFileCount` and on the age of `lastReceivedMessageTimestamp` | Broken notification setup and growing backlogs |
| P1 | Staged initial registration of large tables: create with `refresh_on_create: false` and refresh path by path through a run-operation | Long or timed-out creation |
| P2 | Reconciliation with expected files: compare a file list from the writer with `ICEBERG_TABLE_FILES` | Missing registrations from lost notifications, which never show as `SKIPPED` |
| P2 | Event table integration: read `parquet_direct_file_ingestion_failure` | Monitoring beyond the history retention. Requires a request to Snowflake Support. |
| P0 | Previous versions (section 8): keep files written before an incompatible type change readable through a combined view | Type changes that re-creation cannot solve, such as a decimal scale change or integer to boolean |

The README also states what the package cannot prevent:

- With schema evolution disabled, a new column in a registered file is ignored and leaves no trace in history. Detecting it would require reading the Parquet files.
- With `match_by_column_name: case_sensitive`, a file whose column spelling differs from the established mapping fails to register, and unquoted column names (stored upper-case) do not match lower-case Parquet columns. Use `quote: true` on such columns.
- A change to a create-time option takes effect only on the next full refresh.

## 10. Rejected alternatives

| Alternative | Why it was rejected |
| --- | --- |
| Options directly under `config` | dbt 1.10 deprecates custom keys there, and dbt Fusion and later dbt Core versions are stricter. |
| Data columns in YAML `columns[].data_type` | It would split one table definition across two places. YAML `columns` stays for descriptions and tests. |
| `refresh_all_paths: true` to refresh the whole base location | The name suggested it governs initial registration, which `refresh_on_create` controls. A descriptive `skip_refresh_if_refresh_paths_empty`, usually left at its default, replaced it. |
| Rejecting refresh paths without a trailing `/` (`allow_prefix_paths`) | Snowflake documents prefix matching, so there is no reason to reject it. |
| A combined registration check in `on-run-end` | The history function is called per table, so combining calls with `UNION ALL` reads the same metadata. It saves only round trips, which dbt threads already parallelize. A failure would also leave the model marked successful, and a missing hook would silently disable the check. `ACCOUNT_USAGE` has no registration history view that could replace the per-table calls. |
| Detecting changes to unreadable options by storing a hash of them in the table `COMMENT` or a tag | dbt writes the `COMMENT` through `persist_docs`, so the package cannot own it. A tag needs an extra object and privileges. Making these options create-time only needs neither. |
| Re-applying `MATCH_BY_COLUMN_NAME` with `ALTER` on every run | It would work, but it would treat one unreadable option differently from the rest. All unreadable options behave the same way as create-time options. |
| `on_configuration_change: continue` still failing on re-creation-required changes | `continue` is an explicit escape hatch for incidents. It warns on every difference instead. |
| `on_skipped: warn` as the default | A skipped file is missing data. Failing by default matches the fail-first principle. |
| Previous versions as plain dbt models and a hand-written view | It leaves the table, the view, and their consistency to each project, and the model no longer shows which tables hold its history. |
| Switching to a combined view automatically when a type change is detected | Type detection does not say how old values map to the new type, such as `0` and `1` to booleans. |
| Renaming the model's table automatically | Consumers query that name. The user runs the printed statement, so the name moves only when they decide. |
| Applying the view's policies with `ALTER` after `CREATE OR REPLACE VIEW` | Replacing the view drops them, so the view would be unprotected between the statements. |
| `COMMENT` only from `persist_docs` | `comment` mirrors the DDL. It takes precedence, and `persist_docs` remains the fallback. |

## 11. Open verification

A verification on 2026-11-15 settled the readable table state, the alterability of each option, the history function, nonexistent refresh paths, `COPY GRANTS`, and the absence of an `ACCOUNT_USAGE` history view. The [appendix](#verification-on-2026-11-15) records the results. The integration tests in `integration_tests_snowflake/` passed on 2026-11-15 for every scenario except auto refresh, so changes during Public Preview are caught by rerunning them.

Still open, because the test environment had no S3 event notifications. Snowflake rejects `SET AUTO_REFRESH = TRUE` with error 091364 unless the external volume's active storage location sets `AWS_SNS_TOPIC`. Auto refresh is not used yet, so it will be verified when a model needs it:

- Whether creating with `AUTO_REFRESH = TRUE` and `REFRESH_ON_CREATE = FALSE` backfills existing files. The creation adjustment in section 5 works either way.
- How an auto refresh type conflict appears in history beyond `SKIPPED` with `Column registration failed.`, which the November evaluation observed. A broken file under manual refresh was still recorded as `SKIPPED`, not the documented `REGISTER_FAILED`, on 2026-11-15.
- The retention of `ICEBERG_TABLE_FILE_REGISTRATION_HISTORY` and any row limit above 1,201 events. Events about three days old were still returned.

## Appendix: observed behavior

These observations come from an evaluation on Amazon S3 between 2026-11-11 and 2026-11-13, and from a verification on 2026-11-15 on Snowflake 10.41.101, while Parquet Direct was in Public Preview. Object names are shortened or replaced.

### A catalog integration and a table

```sql
CREATE CATALOG INTEGRATION my_parquet_catalog_integration
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = HIVE
  ENABLED = TRUE;

CREATE ICEBERG TABLE sales
  EXTERNAL_VOLUME = 'example_volume'
  CATALOG = 'my_parquet_catalog_integration'
  BASE_LOCATION = 'sales/'
  AUTO_REFRESH = FALSE
  ENABLE_SCHEMA_INFERENCE = TRUE
  ENABLE_SCHEMA_EVOLUTION = TRUE
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  REPLACE_INVALID_CHARACTERS = FALSE;
```

### A nonexistent refresh path registers nothing

Two unregistered Parquet files were placed under the base location of a table created with `REFRESH_ON_CREATE = FALSE` and `AUTO_REFRESH = FALSE`.

```sql
SELECT COUNT(*) AS N FROM manual_refresh_retest;
-- 0

ALTER ICEBERG TABLE manual_refresh_retest REFRESH 'no-such-file.parquet';
-- Statement executed successfully.

SELECT COUNT(*) AS N FROM manual_refresh_retest;
-- 0

ALTER ICEBERG TABLE manual_refresh_retest REFRESH;
-- Statement executed successfully.

SELECT COUNT(*) AS N FROM manual_refresh_retest;
-- 2
```

A nonexistent prefix behaved the same way on another table with two files and four rows:

```sql
ALTER ICEBERG TABLE missing_dir_table REFRESH 'missing-directory/';
-- Statement executed successfully.
SELECT COUNT(*) AS N FROM missing_dir_table;
-- 0

ALTER ICEBERG TABLE missing_dir_table REFRESH;
SELECT COUNT(*) AS N FROM missing_dir_table;
-- 4
```

The documentation says a nonexistent path refreshes as if no path were given.

### `PATTERN` persists; a refresh path does not

With files at `run-a/part.parquet` (ID 1) and `run-b/part.parquet` (ID 2), `PATTERN = '.*run-a/.*'` registered only ID 1, and the pattern also applies to later refreshes. A manual refresh of `'run-a/'` on a table without a pattern also registered only ID 1, but a later full refresh or enabling auto refresh registers the other files too.

### Auto refresh skips a conflicting file and keeps running

On a table with `AUTO_REFRESH = TRUE` and `ENABLE_SCHEMA_EVOLUTION = TRUE`, files were added in order: a compatible file, a file whose `id` is a string, and another compatible file. No manual refresh was run.

```sql
SELECT TO_VARCHAR(CONVERT_TIMEZONE('UTC', OPERATION_TIME), 'YYYY-MM-DD HH24:MI:SS.FF3') AS OPERATION_TIME_UTC,
       SPLIT_PART(FILE_NAME, '/', -1) AS FILE_NAME,
       OPERATION_STATUS, MESSAGE
FROM TABLE(my_db.INFORMATION_SCHEMA.ICEBERG_TABLE_FILE_REGISTRATION_HISTORY(
  TABLE_NAME => 'my_db.my_schema.my_table'))
ORDER BY OPERATION_TIME;

-- OPERATION_TIME_UTC      | FILE_NAME            | OPERATION_STATUS | MESSAGE
-- 2026-11-11 17:16:36.776 | base.parquet         | REGISTERED_NEW   | File registered successfully.
-- 2026-11-11 17:17:27.463 | good-before.parquet  | REGISTERED_NEW   | File registered successfully.
-- 2026-11-11 17:18:39.142 | incompatible.parquet | SKIPPED          | Column registration failed.
-- 2026-11-11 17:19:41.655 | good-after.parquet   | REGISTERED_NEW   | File registered successfully.

SELECT PARSE_JSON(SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS(
  'my_db.my_schema.my_table')):executionState::STRING AS EXECUTION_STATE;
-- RUNNING

SELECT SPLIT_PART(FILE_NAME, '/', -1) AS FILE_NAME, ROW_COUNT
FROM TABLE(my_db.INFORMATION_SCHEMA.ICEBERG_TABLE_FILES(
  TABLE_NAME => 'my_db.my_schema.my_table'))
ORDER BY FILE_NAME;
-- base.parquet        | 2
-- good-after.parquet  | 2
-- good-before.parquet | 2
```

The skipped file was not retried in the following minute and was absent from the registered files. The history showed no detail beyond `Column registration failed.`

### Re-registering a skipped file

After existing files were registered through auto refresh and `incompatible.parquet` became `SKIPPED`:

```sql
ALTER ICEBERG TABLE retry_table SET AUTO_REFRESH = FALSE;

-- Replace incompatible.parquet on S3 with a compatible file.
ALTER ICEBERG TABLE retry_table REFRESH 'incompatible.parquet';

SELECT ID, MESSAGE FROM retry_table ORDER BY ID;
-- 1 | base-1
-- 2 | base-2
-- 7 | replacement-7
-- 8 | replacement-8
```

The history recorded `SKIPPED` before the replacement and `REGISTERED_NEW` after the manual refresh.

### A manual refresh with a conflicting file fails as a whole

`base.parquet` (integer `ID`) was registered. Then `a-good.parquet` (integer) and `z-bad.parquet` (string) were added.

```sql
ALTER ICEBERG TABLE atomic_table REFRESH;
-- Parquet file schema data type is incompatible with table column.
-- Parquet Type: 'STRING', Expected Type: 'INT', Parquet Path: 'id'
-- Registered files afterwards: base.parquet only.

ALTER ICEBERG TABLE atomic_table REFRESH 'z-bad.parquet';
-- Same error.

ALTER ICEBERG TABLE atomic_table REFRESH 'a-good.parquet';
SELECT ID, VALUE FROM atomic_table ORDER BY ID;
-- 1 | base
-- 2 | good
```

### A broken file is skipped while the manual refresh succeeds

```sql
ALTER ICEBERG TABLE invalid_table REFRESH;
-- Statement executed successfully.

-- FILE_NAME        | OPERATION_STATUS | MESSAGE
-- base.parquet     | REGISTERED_NEW   | File registered successfully.
-- z-broken.parquet | SKIPPED          | Invalid: File is too small for Parquet.
-- z-broken.parquet | SKIPPED          | Invalid: File is too small for Parquet.
-- a-good.parquet   | REGISTERED_NEW   | File registered successfully.
```

### Schema inference and schema evolution

`ENABLE_SCHEMA_INFERENCE = FALSE` with `ENABLE_SCHEMA_EVOLUTION = TRUE` fails even with explicit columns:

```text
SQL compilation error:
ENABLE_SCHEMA_EVOLUTION requires ENABLE_SCHEMA_INFERENCE to be enabled.
Cannot explicitly disable schema inference when schema evolution is enabled.
```

`ALTER ICEBERG TABLE ... SET ENABLE_SCHEMA_INFERENCE = TRUE` fails with `Unsupported feature 'ENABLE_SCHEMA_INFERENCE'.`

With inference on and only `id` declared, `DESC ICEBERG TABLE` also showed `MESSAGE` from the file. With inference off, only `ID` existed. Declaring `id` with `ENABLE_SCHEMA_EVOLUTION = TRUE` turned inference on and added `MESSAGE`. With `REFRESH_ON_CREATE = FALSE`, inference added nothing at creation, and a later refresh without schema evolution did not add `MESSAGE`.

After a file with an extra column `extra` was added to a table with `id` and `message`:

| `ENABLE_SCHEMA_EVOLUTION` | Rows after refresh | `extra` |
| --- | ---: | --- |
| TRUE | 3 | Added; `new-column` in the new row, NULL in the old rows |
| FALSE | 3 | Not added; the value cannot be read |

`ADD COLUMN` on a table without schema evolution was rejected. `DROP COLUMN` and `ALTER COLUMN` type changes were rejected with schema evolution on and off.

### Type widening and conflicts

With schema evolution on, these changes registered and both values were readable:

| Parquet change | Values read | Table type afterwards |
| --- | --- | --- |
| INT32 to INT64 | 9, 2147483648 | LONG |
| FLOAT to DOUBLE | 1.25, 2.5 | DOUBLE |
| DECIMAL(8,2) to DECIMAL(12,2) | 123.45, 123456789.12 | DECIMAL(12,2) |
| struct, list, and map with an inner INT32 to INT64 | Both | `OBJECT(n LONG)`, `ARRAY(LONG)`, `MAP(STRING, LONG)` |

With schema evolution off, INT64 after INT32 failed, and nested changes failed with:

```text
Parquet file schema data type is incompatible with table column.
Kind: 'physical', Parquet Type: 'INT64', Expected Type: 'INT32',
Parquet Path: 'value.n'
```

Rejected even with schema evolution on:

```text
Conflicting data types identified for column VALUE in Parquet files.
Conflict: {"types":["NUMBER(12,2)","NUMBER(12,3)"]}.

Conflicting data types identified for column VALUE in Parquet files.
Conflict: {"types":["TIMESTAMP_NTZ(3)","TIMESTAMP_NTZ(6)"]}.

Parquet file timestamp column has wrong UTC adjustment.
TIMESTAMP_NTZ columns expect values without UTC adjustment and
TIMESTAMP_LTZ columns expect values with UTC adjustment.
Actual adjustment: 'true'
```

Integer and string conflicts were rejected in both directions.

### Column name matching

With `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, registering `upper.parquet` (`ID`, `VALUE`) and then `lower.parquet` (`id`, `value`) failed:

```text
Parquet Column 'id' has a case-insensitively duplicated name to the existing name mapping.
The matching Snowflake Table Column 'ID' already maps to the Parquet Column 'ID'.
```

`CASE_INSENSITIVE` registered both. A single file with both `Name` and `name` failed under `CASE_INSENSITIVE` with `Parquet file schema has two fields 'name' and 'Name' mapped to the same column 'NAME'`.

### Overwritten and deleted files

Overwriting a registered file at the same key made reads fail until a refresh, with both manual and auto refresh:

```text
The parquet file 'blog/base.parquet' for table 'my_db.my_schema.EVOLVE_ISOLATED_OFF' was modified.
```

After a registered file was deleted, a query served from cache returned the old result, while a query on another warehouse with result cache off failed:

```text
The parquet file 'blog/deep-nov13/delete2/base.parquet'
for table 'my_db.my_schema.BLOG_DEEP_DELETE2_NOV13'
was inaccessible.
```

Under auto refresh, a deleted file was unregistered about 34 seconds after deletion.

### Invalid UTF-8

`REPLACE_INVALID_CHARACTERS = FALSE` made reads of the affected column fail with `Invalid UTF8 detected in string 'invalid:0xFF:end'`. `ALTER ICEBERG TABLE ... SET REPLACE_INVALID_CHARACTERS = TRUE` after creation returned `invalid:�:end`. The source file was unchanged.

### Auto refresh status

```sql
SELECT SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS('my_db.my_schema.my_table');
-- {
--   "notificationChannelName": "<queue ARN>",
--   "lastReceivedMessageTimestamp": "2026-11-11T17:17:44.072Z",
--   "lastForwardedFilePath": "<bucket>/<path>/good-before.parquet",
--   "pendingFileCount": 1,
--   "executionState": "RUNNING",
--   "numOutstandingMessagesOnChannel": 1,
--   "lastForwardedMessageTimestamp": "2026-11-11T17:17:19.373Z"
-- }
```

The function requires OWNERSHIP of the table.

### Verification on 2026-11-15

#### Readable table state

A table on a `TABLE_FORMAT = NONE` catalog integration was created with explicit columns, a virtual column, a partition column, `PATTERN`, `ICEBERG_VERSION = 3`, `ENABLE_SCHEMA_INFERENCE = TRUE`, `ENABLE_SCHEMA_EVOLUTION = TRUE`, `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, `REPLACE_INVALID_CHARACTERS = TRUE`, a `NOT NULL` column, a `PRIMARY KEY ... RELY`, and comments. `GET_DDL` returned:

```sql
create or replace ICEBERG TABLE V1_NONE (
	"id" INT NOT NULL COMMENT 'identifier',
	"message" STRING,
	SOURCE_FILE STRING AS (METADATA$FILENAME),
	D STRING AS (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)),
	constraint V1_PK primary key ("id") rely
)
 PARTITION BY (D)COMMENT='v1 options'

 EXTERNAL_VOLUME = '<EXTERNAL_VOLUME>'
 CATALOG = '<CATALOG_NONE>'
 BASE_LOCATION = '<S3_PREFIX>/v1/';
```

| Setting | Readable from |
| --- | --- |
| `EXTERNAL_VOLUME`, `CATALOG` | `GET_DDL`, `SHOW ICEBERG TABLES`, `SHOW PARAMETERS` |
| `BASE_LOCATION` | `GET_DDL`, `SHOW ICEBERG TABLES` |
| `PATTERN` | Not readable |
| `ICEBERG_VERSION` | `SHOW PARAMETERS` (3, `level = TABLE`). `SHOW ICEBERG TABLES` reported `iceberg_table_format_version = 2` for the same table. |
| `ENABLE_SCHEMA_INFERENCE`, `ENABLE_SCHEMA_EVOLUTION` | Not readable |
| `MATCH_BY_COLUMN_NAME` | Not readable, including `SHOW PARAMETERS LIKE 'MATCH_BY_COLUMN_NAME'` |
| `REPLACE_INVALID_CHARACTERS` | `SHOW PARAMETERS` |
| `AUTO_REFRESH` | `SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS` returns `Auto refresh is not enabled for table: ...` when off |
| `REFRESH_ON_CREATE` | Not readable |
| Columns, virtual and partition expressions, `NOT NULL` | `DESC ICEBERG TABLE` (`kind` = `COLUMN`, `VIRTUAL`, `PARTITION`; `expression`; `null?`) and `GET_DDL` |
| `PARTITION BY` | `GET_DDL` |
| Primary key and `RELY` | `GET_DDL`, `SHOW PRIMARY KEYS` |
| Table comment | `GET_DDL`, `SHOW ICEBERG TABLES` |
| Column comment | `GET_DDL`, `DESC ICEBERG TABLE` |

On a `TABLE_FORMAT = HIVE` table created without columns, `GET_DDL` showed the inferred partition column as an expression:

```sql
D DATE AS (IFF((REGEXP_SUBSTR(METADATA$FILENAME, '(^|/)d=([^/]*)/', 1, 1, 'ei', 2)) = '__HIVE_DEFAULT_PARTITION__', null, TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '(^|/)d=([^/]*)/', 1, 1, 'ei', 2))))
```

With `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, the first attempt with unquoted columns `ID` and `MESSAGE` failed at creation against files with lower-case columns:

```text
Parquet Column 'id' has a case-insensitively duplicated name to the existing name mapping.
The matching Snowflake Table Column 'ID' already maps to the Parquet Column 'ID'.
```

#### ALTER on a Parquet Direct table

| Statement | Result |
| --- | --- |
| `SET PATTERN = '...'` | Failed: `Unsupported feature 'PATTERN'.` |
| `UNSET PATTERN` | Failed: `invalid property 'PATTERN' for 'TABLE'` |
| `ADD COLUMN ... AS (METADATA$FILENAME)` (virtual) | Failed: `This ALTER statement is not supported for a table with an external catalog integration and Iceberg as source table format, or a table with Parquet as source.` |
| `DROP COLUMN` (virtual) | Failed: same message |
| `ALTER COLUMN ... SET EXPRESSION (...)` | Failed: syntax error |
| `ADD` / `DROP` / `REPLACE PARTITION BY` | Failed: `Unsupported feature '... PARTITION BY on externally-managed iceberg tables'.` |
| `ALTER COLUMN ... SET NOT NULL` / `DROP NOT NULL` | Failed: same message as `ADD COLUMN` |
| `ALTER COLUMN ... COMMENT` / `UNSET COMMENT` | Succeeded |
| `ADD CONSTRAINT ... PRIMARY KEY` / `UNIQUE` / `FOREIGN KEY`, `DROP CONSTRAINT` | Succeeded |
| `ALTER COLUMN ... SET` / `UNSET MASKING POLICY` | Succeeded |
| `ALTER COLUMN ... SET` / `UNSET TAG`, table `SET` / `UNSET TAG` | Succeeded |
| `ADD` / `DROP ROW ACCESS POLICY` | Succeeded |
| `SET COMMENT` / `UNSET COMMENT` | Succeeded |
| `SET MATCH_BY_COLUMN_NAME` (both values) | Succeeded |
| `SET REPLACE_INVALID_CHARACTERS = TRUE`, `SET LOG_EVENT_LEVEL = ERROR` | Succeeded |
| `SET ENABLE_SCHEMA_EVOLUTION = TRUE` on a table created with FALSE | Failed: `Enabling schema evolution after table creation is not supported. Table 'V2' must be recreated with ENABLE_SCHEMA_EVOLUTION=true.` |
| `SET ENABLE_SCHEMA_EVOLUTION = FALSE` on a table created with TRUE | Succeeded |

Each successful statement was followed by a `REFRESH` and a `SELECT`, which kept working.

#### Switching MATCH_BY_COLUMN_NAME

A table with `ID`, `VALUE` and `CASE_INSENSITIVE` registered `upper.parquet` (`ID`, `VALUE`) and `lower.parquet` (`id`, `value`). After `SET MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, existing rows stayed readable and another lower-case file registered, because `id` was an established mapping. A file spelled `Id` then failed:

```text
Parquet Column 'Id' has a case-insensitively duplicated name to the existing name mapping.
The matching Snowflake Table Column 'ID' already maps to the Parquet Column 'ID'.
```

After switching back to `CASE_INSENSITIVE`, the same file registered.

#### Registration history

- Arguments: `TABLE_NAME` and `START_TIME`. `END_TIME` and `RESULT_LIMIT` were rejected (`unexpected argument`), and a third argument failed with `expected 2, got 3`.
- Columns: `OPERATION_TIME`, `FILE_NAME`, `OPERATION_STATUS`, `MESSAGE`, `FILE_SIZE`, `ROW_COUNT`, `LAST_MODIFIED`.
- A new file appeared in the first query after the refresh, about 0.42 seconds later.
- After refreshing a prefix with 1,200 files, the function returned all 1,201 events, with and without `START_TIME`.
- Overwriting a file and refreshing recorded `UNREGISTERED` then `REGISTERED_NEW`. Deleting a file recorded `UNREGISTERED`. A corrupt file recorded `SKIPPED` with `Invalid: File is too small for Parquet.` while the refresh succeeded.
- A table created on 2026-11-12 still returned its events on 2026-11-15.

#### Nonexistent refresh paths, again

`REFRESH 'no-such-file.parquet'` and `REFRESH 'missing-directory/'` left the count at 0. `REFRESH` without a path then registered both files (4 rows).

#### COPY GRANTS

`CREATE OR REPLACE ICEBERG TABLE ... COPY GRANTS` with the same definition succeeded, and `SHOW GRANTS ON TABLE` still listed the `SELECT` grant to the test role.

#### ACCOUNT_USAGE

`SHOW VIEWS IN SCHEMA SNOWFLAKE.ACCOUNT_USAGE` listed no registration history view. `ICEBERG_STORAGE_OPTIMIZATION_HISTORY` exists but covers storage optimization.

### Integration tests on 2026-11-15

The first run failed on two false differences, which blocked every rerun of an unchanged table until the comparison was fixed.

Snowflake stores virtual column expressions in its own form. `DESC ICEBERG TABLE` returned:

| Declared | Stored |
| --- | --- |
| `SPLIT_PART(METADATA$FILENAME, '/', -1)::VARCHAR` | `SPLIT_PART(METADATA$FILENAME, '/', -1)` |
| `SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE` | `CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1) AS DATE)` |
| `CAST(SPLIT_PART(METADATA$FILENAME, '/', 1) AS DATE)` | `CAST(SPLIT_PART(METADATA$FILENAME, '/', 1) AS DATE)` |
| `lower(split_part(METADATA$FILENAME, 'd=', 2))` | `LOWER(SPLIT_PART(METADATA$FILENAME, 'd=', 2))` |
| `(METADATA$FILENAME)` | `METADATA$FILENAME` |
| `METADATA$FILENAME \|\| '-x'` | `METADATA$FILENAME \|\| '-x'` |

A column declared without `not_null` but covered by a `PRIMARY KEY` was reported with `null? = N`.

After both fixes, 14 scenarios passed and the auto refresh scenario was skipped, in 93 seconds. A broken file registered by manual refresh appeared as `SKIPPED`. Re-applying an unchanged masking policy with `SET MASKING POLICY ... FORCE` succeeded, and declared tags, policies, and constraints read back as unchanged.

### Verification on 2026-11-16

These probes shaped section 8. One table read integer files under `d=2026-11-01/`, and a boolean file arrived under `d=2026-11-02/`.

| Operation | Result |
| --- | --- |
| `REFRESH 'd=2026-11-02/'` on the integer table | Failed with 100496, `Parquet file schema data type is incompatible with table column` |
| `ALTER ICEBERG TABLE ... RENAME TO ...` | Succeeded; the renamed table still read its files and kept its history |
| `CREATE ICEBERG TABLE` with the boolean type, the same base location, and `REFRESH_ON_CREATE = FALSE` | Succeeded with no rows |
| `REFRESH 'd=2026-11-02/'` on that table | Registered only the boolean file |
| `REFRESH` without a path on that table | Failed with 100496 on the integer file |
| `CREATE ICEBERG TABLE` with the boolean type and the default `REFRESH_ON_CREATE` | Failed with 100496 on the integer file |
| `UNION ALL BY NAME` | Matches columns by name and fills missing ones with NULL; a `NUMBER` and a `BOOLEAN` column fail with 001790 |
| `CREATE OR REPLACE VIEW` over a table's name | Failed with 001998 |
| `CREATE OR REPLACE VIEW ... COPY GRANTS` after `ALTER VIEW ... SET MASKING POLICY` | The masking policy was gone |
| `CREATE OR REPLACE VIEW ... COPY GRANTS (col WITH MASKING POLICY ... WITH TAG (...)) WITH ROW ACCESS POLICY ... WITH TAG (...)`, replaced again | Policies, tags, and grants were all kept |


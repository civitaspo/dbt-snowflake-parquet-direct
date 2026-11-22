# Snowflake integration tests

These tests run the `parquet_direct` materialization against a real Snowflake account and an S3 bucket. CI does not run them, because they need cloud resources.

## Prerequisites

- An external volume with read access to an S3 location you can write to.
- Two catalog integrations with `CATALOG_SOURCE = OBJECT_STORE`: one with `TABLE_FORMAT = NONE` and one with `TABLE_FORMAT = HIVE`.
- A role that can create schemas in the database, and use the external volume and both catalog integrations.
- AWS credentials that boto3 can find, with `s3:PutObject`, `s3:ListBucket`, and `s3:DeleteObject` under the test prefix.
- For `test_auto_refresh_backfills_existing_files` only: S3 event notifications for the test prefix sent to an SNS topic, set as `AWS_SNS_TOPIC` on the external volume's storage location. Without it, Snowflake rejects `SET AUTO_REFRESH = TRUE` (error 091364).

## Environment variables

| Variable | Meaning |
| --- | --- |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` | Connection settings |
| `SNOWFLAKE_AUTHENTICATOR` | Optional. `externalbrowser` or `oauth_authorization_code` for a browser login; the connector caches the token in the OS credential store. Defaults to `snowflake` |
| `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_PRIVATE_KEY_PATH` | Authentication for the default authenticator |
| `PARQUET_DIRECT_EXTERNAL_VOLUME` | The external volume name |
| `PARQUET_DIRECT_CATALOG_NONE`, `PARQUET_DIRECT_CATALOG_HIVE` | The two catalog integration names |
| `PARQUET_DIRECT_STORAGE_URL` | The external volume's `STORAGE_BASE_URL`, such as `s3://bucket/root/` |
| `PARQUET_DIRECT_BASE_PREFIX` | Optional. A directory under that location for test files. Defaults to `integration_test` |
| `PARQUET_DIRECT_AUTO_REFRESH` | Set to `1` to run the auto refresh test |

Each run creates the schema `PARQUET_DIRECT_INTEGRATION_TEST_<random>` and the S3 prefix `<PARQUET_DIRECT_BASE_PREFIX>/<random>/`, and removes both at the end.

## Run

```bash
uv run --group snowflake pytest integration_tests/tests -v
```

Without the environment variables, every test is skipped. Put them in `mise.local.toml` (ignored by git) under `[env]` and run the command with `mise exec --`.

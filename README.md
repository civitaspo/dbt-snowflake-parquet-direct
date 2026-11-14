# dbt-snowflake-parquet-direct

A dbt package for working with Parquet files directly in Snowflake. The package is under development.

## Development

Install [mise](https://mise.jdx.dev/) and run `mise install`, then use:

- `mise run lint` to check the repository.
- `mise run fix` to apply supported automatic fixes.
- `mise run test` to run the tests.

The test task loads this package into a small dbt project and parses it with the DuckDB adapter. dbt Core uses Python as its runtime; the package itself uses SQL and Jinja.

Licensed under the Apache License, Version 2.0.

CI verification is in progress.

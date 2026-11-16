{#
  Fails for each skipped file in the last `lookback_hours`, and when the auto refresh mode is not the expected one
  or auto refresh is on but not RUNNING. Use it to monitor tables on a schedule without running the model.
  `auto_refresh` is the expected mode; by default it comes from the model's meta.parquet_direct.auto_refresh.
#}
{% test parquet_direct_registration(model, lookback_hours=24, ignore_file_patterns=[], auto_refresh=none) %}
{%- set package = dbt_snowflake_parquet_direct -%}
{%- if model is string -%}
  {{ exceptions.raise_compiler_error('parquet_direct_registration does not support the where config, because it reads the table by name') }}
{%- endif -%}
{%- set errors = package.parquet_direct__test_argument_errors(lookback_hours, ignore_file_patterns) -%}
{%- set expected = {'value': none, 'error': none} -%}
{%- if execute -%}
  {%- set expected = package.parquet_direct__expected_auto_refresh(auto_refresh, model | string, graph.nodes.values()) -%}
  {%- if expected.error -%}{%- do errors.append(expected.error) -%}{%- endif -%}
{%- endif -%}
{%- if errors -%}
  {{ exceptions.raise_compiler_error('parquet_direct_registration on ' ~ model ~ ':\n  - ' ~ errors | join('\n  - ')) }}
{%- endif -%}
{#- A model with previous versions registers files in its current table, not in its combined view. -#}
{%- set relation = package.parquet_direct__registration_relation(model, graph.nodes.values()) if execute else model -%}
{{ package.parquet_direct__registration_test_sql(relation, lookback_hours, ignore_file_patterns, expected.value) }}
{% endtest %}

{#
  dbt run-operation parquet_direct_find_skipped_files --args '{model: sales}'
  Checks one parquet_direct model's registrations over the last `lookback_hours` (default: the model's
  registration_check.lookback_hours, else 24) with the model's registration_check settings.
#}
{% macro parquet_direct_find_skipped_files(model, lookback_hours=none) %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set nodes = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | selectattr('name', 'equalto', model) | list -%}
  {%- if nodes | length != 1 -%}
    {{ exceptions.raise_compiler_error('Expected one model named ' ~ model ~ ', found ' ~ nodes | length) }}
  {%- endif -%}
  {%- set raw_options = (nodes[0].config.meta or {}).get('parquet_direct') -%}
  {%- set errors = package.parquet_direct__validate(raw_options) if raw_options is mapping else ['model ' ~ model ~ ' has no meta.parquet_direct'] -%}
  {%- set check = raw_options.get('registration_check') if raw_options is mapping else none -%}
  {%- set hours = lookback_hours if lookback_hours is not none else check.get('lookback_hours', 24) if check is mapping else 24 -%}
  {%- set lookback_error = package.parquet_direct__lookback_error(hours) -%}
  {%- if lookback_error -%}{%- do errors.append(lookback_error) -%}{%- endif -%}
  {%- if errors -%}
    {{ exceptions.raise_compiler_error('Cannot check ' ~ model ~ ':\n  - ' ~ errors | join('\n  - ')) }}
  {%- endif -%}
  {%- set declaration = package.parquet_direct__normalize(raw_options) -%}
  {%- set relation = package.parquet_direct__current_relation(ref(model)) if declaration.previous_versions else ref(model) -%}
  {%- set summary = package.parquet_direct__check_registration(relation, declaration, "dateadd('hour', -" ~ hours ~ ", current_timestamp())") -%}
  {%- do log(relation ~ ': registered ' ~ summary.counts.registered ~ ', unregistered ' ~ summary.counts.unregistered ~ ', skipped ' ~ summary.skipped | length ~ ' in the last ' ~ hours ~ ' hours', info=true) -%}
{% endmacro %}

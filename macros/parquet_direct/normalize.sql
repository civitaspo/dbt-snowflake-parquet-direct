{#
  Turn a validated meta.parquet_direct mapping into the shape the other macros use.
  `options` keeps the raw mapping: DDL emits only options the model sets, so Snowflake defaults apply otherwise.
  `model_columns` is the model's YAML columns (name -> object with description).
#}
{% macro parquet_direct__normalize(declaration, model_description=none, model_columns={}, persist_relation=false, persist_columns=false) -%}
  {%- set descriptions = {} -%}
  {%- for name, info in model_columns.items() -%}
    {%- do descriptions.update({name | upper: info.description}) -%}
  {%- endfor -%}

  {%- set columns = [] -%}
  {%- set constraints = [] -%}
  {%- for kind, column_kind in [('columns', 'COLUMN'), ('virtual_columns', 'VIRTUAL'), ('partition_columns', 'PARTITION')] -%}
    {%- for entry in declaration.get(kind) or [] -%}
      {%- set identifier = entry.name if entry.get('quote') else entry.name | upper -%}
      {%- set policy = entry.get('masking_policy') -%}
      {%- if policy is string -%}
        {%- set policy = {'name': policy, 'using': []} -%}
      {%- elif policy is mapping -%}
        {%- set policy = {'name': policy.name, 'using': policy.get('using') or []} -%}
      {%- endif -%}
      {%- set comment = entry.get('comment') -%}
      {%- if comment is none and persist_columns and descriptions.get(entry.name | upper) -%}
        {%- set comment = descriptions.get(entry.name | upper) -%}
      {%- endif -%}
      {%- do columns.append({
        'name': entry.name,
        'identifier': identifier,
        'sql': dbt_snowflake_parquet_direct.parquet_direct__quote_identifier(entry.name) if entry.get('quote') else entry.name,
        'kind': column_kind,
        'data_type': entry.data_type,
        'expression': entry.get('expression'),
        'not_null': entry.get('not_null') == true,
        'masking_policy': policy,
        'tag': entry.get('tag') or {},
        'comment': comment,
      }) -%}
      {%- for inline in entry.get('constraints') or [] -%}
        {%- do constraints.append({'type': inline.type, 'name': none, 'columns': [identifier], 'rely': inline.get('rely') == true, 'references': inline.get('references')}) -%}
      {%- endfor -%}
    {%- endfor -%}
  {%- endfor -%}

  {%- for constraint in declaration.get('constraints') or [] -%}
    {%- set identifiers = [] -%}
    {%- for name in constraint.columns -%}{%- do identifiers.append(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(name)) -%}{%- endfor -%}
    {%- do constraints.append({'type': constraint.type, 'name': constraint.get('name'), 'columns': identifiers, 'rely': constraint.get('rely') == true, 'references': constraint.get('references')}) -%}
  {%- endfor -%}

  {#- Expressions are keyed by the identifier of the declared column they produce. -#}
  {%- set previous_versions = [] -%}
  {%- for version in declaration.get('previous_versions') or [] -%}
    {%- set expressions = {} -%}
    {%- for name, expression in (version.get('columns') or {}).items() -%}
      {%- for column in columns if column.name | upper == dbt_snowflake_parquet_direct.parquet_direct__column_key(name) -%}
        {%- do expressions.update({column.identifier: expression}) -%}
      {%- endfor -%}
    {%- endfor -%}
    {%- do previous_versions.append({'table': version.table, 'identifier': dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(version.table), 'columns': expressions}) -%}
  {%- endfor -%}

  {%- set comment = declaration.get('comment') -%}
  {%- if comment is none and persist_relation and model_description -%}
    {%- set comment = model_description -%}
  {%- endif -%}

  {%- set inference = declaration.get('enable_schema_inference') -%}
  {%- set evolution = declaration.get('enable_schema_evolution') == true -%}
  {%- set check = declaration.get('registration_check') or {} -%}
  {%- set data_columns = columns | selectattr('kind', 'equalto', 'COLUMN') | list -%}

  {{ return({
    'options': declaration,
    'columns': columns,
    'partition_columns': columns | selectattr('kind', 'equalto', 'PARTITION') | list,
    'constraints': constraints,
    'comment': comment,
    'tag': declaration.get('tag') or {},
    'row_access_policy': declaration.get('row_access_policy'),
    'base_location': declaration.base_location,
    'auto_refresh': declaration.get('auto_refresh') == true,
    'refresh_on_create': declaration.get('refresh_on_create') != false,
    'schema_inference_file_pattern_on_create': declaration.get('schema_inference_file_pattern_on_create'),
    'schema_inference': inference if inference is not none else (data_columns | length == 0 or evolution),
    'schema_evolution': evolution,
    'refresh_paths': declaration.get('refresh_paths') or [],
    'previous_versions': previous_versions,
    'skip_refresh_if_refresh_paths_empty': declaration.get('skip_refresh_if_refresh_paths_empty') != false,
    'registration_check': {
      'on_skipped': check.get('on_skipped', 'error'),
      'on_unhealthy': check.get('on_unhealthy', 'error'),
      'lookback_hours': check.get('lookback_hours', 24),
      'ignore_file_patterns': check.get('ignore_file_patterns') or [],
      'max_reported_files': check.get('max_reported_files', 20),
    },
  }) }}
{%- endmacro %}

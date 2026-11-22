{# DDL builders. They return SQL strings and issue nothing. #}

{% macro parquet_direct__column_sql(column) -%}
  {%- set sql = column.sql ~ ' ' ~ column.data_type -%}
  {%- if column.expression -%}{%- set sql = sql ~ ' as (' ~ column.expression ~ ')' -%}{%- endif -%}
  {%- if column.not_null -%}{%- set sql = sql ~ ' not null' -%}{%- endif -%}
  {%- if column.masking_policy -%}
    {%- set sql = sql ~ ' with masking policy ' ~ column.masking_policy.name -%}
    {%- if column.masking_policy.using -%}{%- set sql = sql ~ ' using (' ~ column.masking_policy.using | join(', ') ~ ')' -%}{%- endif -%}
  {%- endif -%}
  {%- if column.tag -%}{%- set sql = sql ~ ' with tag (' ~ dbt_snowflake_parquet_direct.parquet_direct__tag_list(column.tag) ~ ')' -%}{%- endif -%}
  {%- if column.comment is not none -%}{%- set sql = sql ~ ' comment ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(column.comment) -%}{%- endif -%}
  {{ return(sql) }}
{%- endmacro %}

{# The body of a constraint without "constraint <name>", shared by CREATE and ALTER ... ADD. #}
{#- Constraint columns are kept as Snowflake stores them, so they are always quoted to keep their case. -#}
{% macro parquet_direct__constraint_body_sql(constraint) -%}
  {%- set columns = [] -%}
  {%- for name in constraint.columns -%}{%- do columns.append(dbt_snowflake_parquet_direct.parquet_direct__quote_identifier(name)) -%}{%- endfor -%}
  {%- set sql = constraint.type | replace('_', ' ') ~ ' (' ~ columns | join(', ') ~ ')' -%}
  {%- if constraint.references -%}
    {%- set referenced = [] -%}
    {%- for name in constraint.references.columns -%}
      {%- do referenced.append(dbt_snowflake_parquet_direct.parquet_direct__quote_identifier(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(name))) -%}
    {%- endfor -%}
    {%- set sql = sql ~ ' references ' ~ constraint.references.table ~ ' (' ~ referenced | join(', ') ~ ')' -%}
  {%- endif -%}
  {%- if constraint.rely -%}{%- set sql = sql ~ ' rely' -%}{%- endif -%}
  {{ return(sql) }}
{%- endmacro %}

{% macro parquet_direct__constraint_sql(constraint) -%}
  {%- set prefix = 'constraint ' ~ constraint.name ~ ' ' if constraint.name else '' -%}
  {{ return(prefix ~ dbt_snowflake_parquet_direct.parquet_direct__constraint_body_sql(constraint)) }}
{%- endmacro %}

{#
  CREATE ICEBERG TABLE plus any ALTER that must follow it:
  - LOG_EVENT_LEVEL is not part of the CREATE syntax, so it is set afterwards.
  - With refresh_on_create: false and auto_refresh: true, the table is created with AUTO_REFRESH = FALSE and then
    switched on, the order Snowflake documents for backfilling existing files.
#}
{% macro parquet_direct__create_statements(relation, declaration, replace=false, copy_grants=false, inferred_columns_sql=none) -%}
  {%- set options = declaration.options -%}
  {%- set defer_auto_refresh = declaration.auto_refresh and not declaration.refresh_on_create -%}
  {%- set lines = ['create ' ~ ('or replace ' if replace else '') ~ 'iceberg table ' ~ relation] -%}

  {%- set elements = [inferred_columns_sql] if inferred_columns_sql is not none else [] -%}
  {%- for column in declaration.columns -%}{%- do elements.append(dbt_snowflake_parquet_direct.parquet_direct__column_sql(column)) -%}{%- endfor -%}
  {%- for constraint in declaration.constraints -%}{%- do elements.append(dbt_snowflake_parquet_direct.parquet_direct__constraint_sql(constraint)) -%}{%- endfor -%}
  {%- if elements -%}
    {%- do lines.append('(\n    ' ~ elements | join(',\n    ') ~ '\n)') -%}
  {%- endif -%}
  {%- if declaration.partition_columns -%}
    {%- do lines.append('partition by (' ~ declaration.partition_columns | map(attribute='sql') | join(', ') ~ ')') -%}
  {%- endif -%}

  {%- for key in ['external_volume', 'catalog', 'base_location', 'pattern'] if options.get(key) is not none -%}
    {%- do lines.append(key ~ ' = ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(options.get(key))) -%}
  {%- endfor -%}
  {%- if options.get('iceberg_version') is not none -%}
    {%- do lines.append('iceberg_version = ' ~ options.iceberg_version) -%}
  {%- endif -%}
  {%- for key in ['enable_schema_inference', 'enable_schema_evolution'] if options.get(key) is not none -%}
    {%- do lines.append(key ~ ' = ' ~ dbt_snowflake_parquet_direct.parquet_direct__bool_sql(options.get(key))) -%}
  {%- endfor -%}
  {%- if options.get('match_by_column_name') is not none -%}
    {%- do lines.append('match_by_column_name = ' ~ options.match_by_column_name | upper) -%}
  {%- endif -%}
  {%- if options.get('auto_refresh') is not none -%}
    {%- do lines.append('auto_refresh = ' ~ dbt_snowflake_parquet_direct.parquet_direct__bool_sql(declaration.auto_refresh and not defer_auto_refresh)) -%}
  {%- endif -%}
  {%- for key in ['refresh_on_create', 'replace_invalid_characters'] if options.get(key) is not none -%}
    {%- do lines.append(key ~ ' = ' ~ dbt_snowflake_parquet_direct.parquet_direct__bool_sql(options.get(key))) -%}
  {%- endfor -%}
  {%- if copy_grants -%}
    {%- do lines.append('copy grants') -%}
  {%- endif -%}
  {%- if declaration.row_access_policy -%}
    {%- do lines.append('with row access policy ' ~ declaration.row_access_policy.name ~ ' on (' ~ declaration.row_access_policy.on | join(', ') ~ ')') -%}
  {%- endif -%}
  {%- if declaration.tag -%}
    {%- do lines.append('with tag (' ~ dbt_snowflake_parquet_direct.parquet_direct__tag_list(declaration.tag) ~ ')') -%}
  {%- endif -%}
  {%- if declaration.comment is not none -%}
    {%- do lines.append('comment = ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(declaration.comment)) -%}
  {%- endif -%}

  {%- set statements = [lines | join('\n')] -%}
  {%- if options.get('log_event_level') is not none -%}
    {%- do statements.append('alter iceberg table ' ~ relation ~ ' set log_event_level = ' ~ options.log_event_level | upper) -%}
  {%- endif -%}
  {%- if defer_auto_refresh -%}
    {%- do statements.append('alter iceberg table ' ~ relation ~ ' set auto_refresh = true') -%}
  {%- endif -%}
  {{ return(statements) }}
{%- endmacro %}

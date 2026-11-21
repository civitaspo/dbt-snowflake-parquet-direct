{% macro parquet_direct__inference_relation(relation) -%}
  {{ return(relation.incorporate(path={'identifier': '__tmp_dbt_parquet_direct__' ~ relation.identifier})) }}
{%- endmacro %}

{% macro parquet_direct__inference_probe_comment() -%}
  {{ return('Schema inference probe managed by dbt-snowflake-parquet-direct') }}
{%- endmacro %}

{% macro parquet_direct__inference_create_sql(probe, declaration) -%}
  {%- set sql_string = dbt_snowflake_parquet_direct.parquet_direct__sql_string -%}
  {%- set lines = ['create iceberg table ' ~ probe] -%}
  {%- for key in ['external_volume', 'catalog', 'base_location'] if declaration.options.get(key) is not none -%}
    {%- do lines.append(key ~ ' = ' ~ sql_string(declaration.options[key])) -%}
  {%- endfor -%}
  {%- do lines.append('pattern = ' ~ sql_string(declaration.schema_inference_file_pattern_on_create)) -%}
  {%- do lines.append('comment = ' ~ sql_string(dbt_snowflake_parquet_direct.parquet_direct__inference_probe_comment())) -%}
  {{ return(lines | join('\n')) }}
{%- endmacro %}

{# shortcut: this relies on Snowflake's GET_DDL formatting for the probe. Revisit if that format changes. #}
{% macro parquet_direct__inferred_column_list(ddl) -%}
  {%- set identifier = '(?:"(?:[^"]|"")*"|[A-Z_][A-Z0-9_$]*)' -%}
  {%- set header = modules.re.match('^\\s*create\\s+(?:or\\s+replace\\s+)?iceberg\\s+table\\s+' ~ identifier ~ '\\s*\\(', ddl, modules.re.I) -%}
  {%- if not header -%}{{ return('') }}{%- endif -%}
  {%- set closing = ddl.find('\n)', header.end()) -%}
  {{ return(ddl[header.end():closing] | trim if closing >= 0 else '') }}
{%- endmacro %}

{% macro parquet_direct__infer_columns_sql(relation, declaration) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set probe = package.parquet_direct__inference_relation(relation) -%}
  {%- set inference_pattern = declaration.schema_inference_file_pattern_on_create -%}
  {%- if probe.identifier | length > 255 -%}
    {%- if inference_pattern is not none -%}
      {{ exceptions.raise_compiler_error('Cannot infer columns for ' ~ relation ~ ': the probe name would exceed Snowflake\'s 255-character limit. Use a shorter model alias or declare columns explicitly.') }}
    {%- endif -%}
    {{ return(none) }}
  {%- endif -%}

  {#- Clean up interrupted runs even after a model switches to declared columns. -#}
  {%- set identifier = package.parquet_direct__stored_identifier(probe) -%}
  {%- set candidates = package.parquet_direct__rows('show iceberg tables in schema ' ~ probe.include(identifier=false) ~ ' starts with ' ~ package.parquet_direct__sql_string(identifier)) -%}
  {%- if candidates | selectattr('name', 'equalto', identifier) | selectattr('comment', 'equalto', package.parquet_direct__inference_probe_comment()) | list -%}
    {%- do run_query('drop table if exists ' ~ probe) -%}
  {%- endif -%}
  {%- if inference_pattern is none -%}{{ return(none) }}{%- endif -%}

  {%- do run_query(package.parquet_direct__inference_create_sql(probe, declaration)) -%}
  {%- set ddl = run_query("select get_ddl('table', " ~ package.parquet_direct__sql_string(probe) ~ ", false)").columns[0].values()[0] -%}
  {%- do run_query('drop table if exists ' ~ probe) -%}

  {#- GET_DDL preserves physical Iceberg types such as LONG, which DESCRIBE reports as NUMBER(19,0). -#}
  {%- set columns = package.parquet_direct__inferred_column_list(ddl) -%}
  {%- if not columns -%}
    {{ exceptions.raise_compiler_error('Could not read inferred columns for ' ~ relation ~ ' with schema_inference_file_pattern_on_create = ' ~ package.parquet_direct__sql_string(inference_pattern) ~ ':\n' ~ ddl) }}
  {%- endif -%}
  {{ return(columns) }}
{%- endmacro %}

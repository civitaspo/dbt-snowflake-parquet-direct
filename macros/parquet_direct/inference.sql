{% macro parquet_direct__inference_relation(relation) -%}
  {%- set identifier = '__tmp_dbt_parquet_direct__' ~ relation.identifier -%}
  {%- set probe = relation.incorporate(path={'identifier': identifier}) -%}
  {%- set overflow = (probe | string | length) - 255 -%}
  {%- if overflow > 0 -%}
    {%- set retained_length = (identifier | length) - overflow - 7 -%}
    {%- set identifier = identifier[:retained_length] ~ local_md5(relation.identifier)[:7] -%}
    {%- set probe = relation.incorporate(path={'identifier': identifier}) -%}
  {%- endif -%}
  {{ return(probe) }}
{%- endmacro %}

{% macro parquet_direct__inference_create_sql(probe, declaration) -%}
  {%- set sql_string = dbt_snowflake_parquet_direct.parquet_direct__sql_string -%}
  {%- set lines = ['create or replace iceberg table ' ~ probe] -%}
  {%- for key in ['external_volume', 'catalog', 'base_location'] if declaration.options.get(key) is not none -%}
    {%- do lines.append(key ~ ' = ' ~ sql_string(declaration.options[key])) -%}
  {%- endfor -%}
  {%- do lines.append('pattern = ' ~ sql_string(declaration.schema_inference_file_pattern_on_create)) -%}
  {{ return(lines | join('\n')) }}
{%- endmacro %}

{# shortcut: this relies on Snowflake's GET_DDL formatting for the probe. Revisit if that format changes. #}
{% macro parquet_direct__inferred_column_list(ddl) -%}
  {%- set opening = ddl.find('(\n') -%}
  {%- set closing = ddl.find('\n)', opening + 1) -%}
  {{ return(ddl[opening + 2:closing] | trim if opening >= 0 and closing > opening else '') }}
{%- endmacro %}

{% macro parquet_direct__infer_columns_sql(relation, declaration) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set probe = package.parquet_direct__inference_relation(relation) -%}
  {%- set inference_pattern = declaration.schema_inference_file_pattern_on_create -%}
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

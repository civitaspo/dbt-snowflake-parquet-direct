{# Small helpers shared by the parquet_direct macros. They issue no SQL. #}

{% macro parquet_direct__sql_string(value) -%}
  {{ return("'" ~ (value | string).replace("\\", "\\\\").replace("'", "\\'") ~ "'") }}
{%- endmacro %}

{% macro parquet_direct__is_list(value) -%}
  {{ return(value is iterable and value is not string and value is not mapping) }}
{%- endmacro %}

{# An identifier as Snowflake stores it: "quoted" keeps its case, anything else is upper-cased. #}
{% macro parquet_direct__normalize_identifier(name) -%}
  {%- set text = name | string | trim -%}
  {%- if text | length > 1 and text.startswith('"') and text.endswith('"') -%}
    {{ return(text[1:-1].replace('""', '"')) }}
  {%- endif -%}
  {{ return(text | upper) }}
{%- endmacro %}

{# The SQL form of a stored identifier, keeping its case and escaping embedded double quotes. #}
{% macro parquet_direct__quote_identifier(name) -%}
  {{ return('"' ~ name.replace('"', '""') ~ '"') }}
{%- endmacro %}

{# A three-part object name as Snowflake stores it, such as DB.SCHEMA.POLICY. #}
{% macro parquet_direct__qualify(name) -%}
  {#- shortcut: dots inside quoted identifiers are not supported. -#}
  {%- set parts = [] -%}
  {%- for part in (name | string).split('.') -%}
    {%- do parts.append(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(part)) -%}
  {%- endfor -%}
  {{ return(parts | join('.')) }}
{%- endmacro %}

{% macro parquet_direct__is_three_part(name) -%}
  {{ return(name is string and name.split('.') | length == 3 and '' not in name.split('.')) }}
{%- endmacro %}

{% macro parquet_direct__bool_sql(value) -%}
  {{ return('true' if value else 'false') }}
{%- endmacro %}

{% macro parquet_direct__tag_list(tags) -%}
  {%- set items = [] -%}
  {%- for key, value in tags | dictsort -%}
    {%- do items.append(key ~ ' = ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(value)) -%}
  {%- endfor -%}
  {{ return(items | join(', ')) }}
{%- endmacro %}

{#
  ignore_file_patterns run in Python (model runs) and in Snowflake (the generic test). Reject syntax the two engines
  read differently: backslash letter classes such as \d, POSIX classes such as [[:digit:]], (? groups, and lazy quantifiers.
#}
{% macro parquet_direct__ignore_pattern_errors(patterns, where) -%}
  {%- set errors = [] -%}
  {%- for pattern in patterns if pattern is string and modules.re.search('\\\\[A-Za-z]|\\[:|\\(\\?|[*+?}]\\?', pattern) -%}
    {%- do errors.append("S02: " ~ where ~ " pattern '" ~ pattern ~ "' uses syntax that Python and Snowflake read differently; use literals, '.', bracket lists such as [0-9], anchors, groups, '|', and the quantifiers * + ? {n,m}") -%}
  {%- endfor -%}
  {{ return(errors) }}
{%- endmacro %}

{# Raise, warn, or log a message according to an error / warn / ignore setting. #}
{% macro parquet_direct__report(severity, message) -%}
  {%- if severity == 'error' -%}
    {{ exceptions.raise_compiler_error(message) }}
  {%- elif severity == 'warn' -%}
    {%- do exceptions.warn(message) -%}
  {%- else -%}
    {%- do log(message, info=true) -%}
  {%- endif -%}
{%- endmacro %}

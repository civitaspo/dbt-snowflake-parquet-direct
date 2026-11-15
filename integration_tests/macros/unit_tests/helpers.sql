{# Unit tests for the package's pure macros. Run with: dbt run-operation run_unit_tests #}

{% macro run_unit_tests() %}
  {%- set tests = [
    test_validate, test_preflight, test_config_effects, test_ddl, test_type_comparison,
    test_diff, test_snowflake_stored_forms, test_recreation_error, test_refresh_statements, test_history, test_registration_findings, test_registration_test_sql, test_expected_auto_refresh,
  ] -%}
  {%- for test in tests -%}
    {%- do test() -%}
  {%- endfor -%}
  {%- do log(tests | length ~ ' unit test groups passed', info=true) -%}
{% endmacro %}

{% macro assert_equal(name, actual, expected) %}
  {%- if actual != expected -%}
    {{ exceptions.raise_compiler_error(name ~ ' failed\nexpected: ' ~ tojson(expected) ~ '\nactual:   ' ~ tojson(actual)) }}
  {%- endif -%}
{% endmacro %}

{% macro error_codes(errors) %}
  {%- set codes = [] -%}
  {%- for error in errors if error[:3] not in codes -%}{%- do codes.append(error[:3]) -%}{%- endfor -%}
  {{ return(codes | sort) }}
{% endmacro %}

{% macro with_overrides(base, overrides={}, remove=[]) %}
  {%- set copy = {} -%}
  {%- do copy.update(base) -%}
  {%- do copy.update(overrides) -%}
  {%- for key in remove -%}{%- do copy.pop(key) -%}{%- endfor -%}
  {{ return(copy) }}
{% endmacro %}

{% macro test_relation() %}
  {{ return(api.Relation.create(database='DB', schema='SCH', identifier='T', quote_policy={'database': false, 'schema': false, 'identifier': false})) }}
{% endmacro %}

{# A configuration that uses every option. #}
{% macro full_config() %}
  {{ return({
    'external_volume': 'example_volume',
    'catalog': 'my_catalog',
    'base_location': 'sales/',
    'pattern': '.*[.]parquet',
    'iceberg_version': 3,
    'enable_schema_inference': false,
    'enable_schema_evolution': false,
    'match_by_column_name': 'case_insensitive',
    'auto_refresh': false,
    'refresh_on_create': true,
    'replace_invalid_characters': false,
    'log_event_level': 'error',
    'columns': [
      {'name': 'id', 'data_type': 'number(19,0)', 'not_null': true, 'constraints': [{'type': 'primary_key', 'rely': true}]},
      {'name': 'amount', 'data_type': 'number(12,2)', 'masking_policy': 'gov.policies.mask_amount', 'comment': "it's money"},
    ],
    'virtual_columns': [{'name': 'source_file', 'data_type': 'varchar', 'expression': 'METADATA$FILENAME'}],
    'partition_columns': [{'name': 'd', 'data_type': 'date', 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE"}],
    'constraints': [{'type': 'unique', 'name': 'uq_amount', 'columns': ['amount']}],
    'row_access_policy': {'name': 'gov.policies.row_access_policy', 'on': ['id']},
    'tag': {'gov.tags.domain': 'sales'},
    'comment': 'Sales',
    'refresh_paths': ['d=2026-11-15/'],
    'registration_check': {'on_skipped': 'warn', 'ignore_file_patterns': ['_tmp']},
  }) }}
{% endmacro %}

{# The state Snowflake reports for a table created from full_config(). #}
{% macro full_state() %}
  {{ return({
    'exists': true,
    'is_parquet_direct': true,
    'external_volume': 'EXAMPLE_VOLUME',
    'catalog': 'MY_CATALOG',
    'base_location': 'sales/',
    'comment': 'Sales',
    'auto_refresh': false,
    'parameters': {'ICEBERG_VERSION': '3', 'REPLACE_INVALID_CHARACTERS': 'false', 'LOG_EVENT_LEVEL': 'ERROR'},
    'columns': [
      {'name': 'ID', 'type': 'NUMBER(19,0)', 'kind': 'COLUMN', 'nullable': false, 'expression': none, 'comment': none},
      {'name': 'AMOUNT', 'type': 'NUMBER(12,2)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': "it's money"},
      {'name': 'SOURCE_FILE', 'type': 'VARCHAR(134217728)', 'kind': 'VIRTUAL', 'nullable': true, 'expression': 'METADATA$FILENAME', 'comment': none},
      {'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE", 'comment': none},
    ],
    'masking_policies': {'AMOUNT': 'GOV.POLICIES.MASK_AMOUNT'},
    'row_access_policy': {'name': 'GOV.POLICIES.ROW_ACCESS_POLICY', 'on': ['ID']},
    'table_tags': {'GOV.TAGS.DOMAIN': 'sales'},
    'column_tags': {},
    'constraints': [
      {'type': 'primary_key', 'name': 'SYS_CONSTRAINT_1', 'columns': ['ID'], 'rely': true, 'references': none},
      {'type': 'unique', 'name': 'UQ_AMOUNT', 'columns': ['AMOUNT'], 'rely': false, 'references': none},
    ],
  }) }}
{% endmacro %}

{% macro none_catalog() %}
  {{ return({'name': 'MY_CATALOG', 'catalog_source': 'OBJECT_STORE', 'table_format': 'NONE'}) }}
{% endmacro %}

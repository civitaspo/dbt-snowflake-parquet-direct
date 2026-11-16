{% macro test_validate() %}
  {%- set validate = dbt_snowflake_parquet_direct.parquet_direct__validate -%}
  {%- set base = full_config() -%}
  {%- set body = "-- Parquet Direct table\n/* configured\n   in YAML */\n\n" -%}

  {%- do assert_equal('full config is valid', validate(base, {}, body), []) -%}
  {%- do assert_equal('minimal config is valid', validate({'base_location': 'sales/'}), []) -%}
  {%- do assert_equal('not a mapping', error_codes(validate('sales/')), ['S02']) -%}

  {%- set data_columns = [{'name': 'id', 'data_type': 'int'}] -%}
  {%- set cases = [
    ('unknown key', {'externl_volume': 'v'}, [], ['S01']),
    ('unknown column key', {'columns': [{'name': 'id', 'data_type': 'int', 'nullable': true}], 'constraints': [], 'row_access_policy': none}, [], ['S01']),
    ('unknown registration_check key', {'registration_check': {'on_skip': 'warn'}}, [], ['S01']),
    ('boolean as string', {'auto_refresh': 'yes'}, [], ['S02']),
    ('iceberg_version', {'iceberg_version': 4}, [], ['S02']),
    ('match_by_column_name', {'match_by_column_name': 'exact'}, [], ['S02']),
    ('log_event_level', {'log_event_level': 'verbose'}, [], ['S02']),
    ('tag value', {'tag': {'t': 1}}, [], ['S02']),
    ('row_access_policy without on', {'row_access_policy': {'name': 'governance.policies.mask_id', 'on': []}}, [], ['S02']),
    ('on_skipped', {'registration_check': {'on_skipped': 'fail'}}, [], ['S02']),
    ('max_reported_files', {'registration_check': {'max_reported_files': 0}}, [], ['S02']),
    ('unquoted name with a space', {'columns': [{'name': 'my id', 'data_type': 'int'}], 'constraints': [], 'row_access_policy': none}, [], ['S02']),
    ('quoted name with a space', {'columns': [{'name': 'my id', 'data_type': 'int', 'quote': true}], 'constraints': [], 'row_access_policy': none}, [], []),
    ('missing data_type', {'columns': [{'name': 'id'}], 'constraints': [], 'row_access_policy': none}, [], ['S02']),
    ('masking_policy type', {'columns': [{'name': 'id', 'data_type': 'int', 'masking_policy': 1}], 'constraints': [], 'row_access_policy': none}, [], ['S02']),
    ('two-part masking policy name', {'columns': [{'name': 'id', 'data_type': 'int', 'masking_policy': 'policies.mask_id'}], 'constraints': [], 'row_access_policy': none}, [], ['S02']),
    ('two-part row access policy name', {'row_access_policy': {'name': 'policies.mask_id', 'on': ['id']}}, [], ['S02']),
    ('one-part tag name', {'tag': {'domain': 'sales'}}, [], ['S02']),
    ('two-part foreign key table', {'constraints': [{'type': 'foreign_key', 'columns': ['id'], 'references': {'table': 'sales.parent', 'columns': ['id']}}]}, [], ['S02']),
    ('foreign key without references', {'constraints': [{'type': 'foreign_key', 'columns': ['id']}]}, [], ['S02']),
    ('references on a unique key', {'constraints': [{'type': 'unique', 'columns': ['id'], 'references': {'table': 't', 'columns': ['id']}}]}, [], ['S02']),
    ('constraint type', {'constraints': [{'type': 'check', 'columns': ['id']}]}, [], ['S02']),
    ('backslash class in ignore pattern', {'registration_check': {'ignore_file_patterns': ['\\d+']}}, [], ['S02']),
    ('POSIX class in ignore pattern', {'registration_check': {'ignore_file_patterns': ['[[:digit:]]']}}, [], ['S02']),
    ('inline flag in ignore pattern', {'registration_check': {'ignore_file_patterns': ['(?i)tmp']}}, [], ['S02']),
    ('lazy quantifier in ignore pattern', {'registration_check': {'ignore_file_patterns': ['a.*?b']}}, [], ['S02']),
    ('portable ignore pattern', {'registration_check': {'ignore_file_patterns': ['^sales/d=[0-9]+/(_tmp|_staging)/', '[.]crc$', 'x{2,3}']}}, [], []),
    ('missing base_location', {}, ['base_location'], ['S03']),
    ('absolute base_location', {'base_location': '/sales/'}, [], ['S03']),
    ('base_location without trailing slash', {'base_location': 'sales'}, [], ['S03']),
    ('base_location with a scheme', {'base_location': 's3://bucket/sales/'}, [], ['S03']),
    ('base_location with ..', {'base_location': 'a/../sales/'}, [], ['S03']),
    ('evolution without inference', {'enable_schema_evolution': true}, [], ['S04']),
    ('refresh_on_create false without columns', {'refresh_on_create': false, 'columns': [], 'constraints': [], 'row_access_policy': none, 'enable_schema_inference': none}, [], ['S05']),
    ('inference false without columns', {'columns': [], 'constraints': [], 'row_access_policy': none}, [], ['S06']),
    ('auto_refresh with refresh_paths', {'auto_refresh': true}, [], ['S07']),
    ('auto_refresh with skip setting', {'auto_refresh': true, 'refresh_paths': none, 'skip_refresh_if_refresh_paths_empty': true}, [], ['S07']),
    ('duplicate refresh path', {'refresh_paths': ['d=1/', 'd=1/']}, [], ['S08']),
    ('refresh path with ..', {'refresh_paths': ['../d=1/']}, [], ['S08']),
    ('empty refresh path', {'refresh_paths': ['']}, [], ['S08']),
    ('data column with expression', {'columns': [{'name': 'id', 'data_type': 'int', 'expression': '1'}], 'constraints': [], 'row_access_policy': none}, [], ['S09']),
    ('virtual column without expression', {'virtual_columns': [{'name': 'v', 'data_type': 'int'}]}, [], ['S09']),
    ('partition expression without METADATA$FILENAME', {'partition_columns': [{'name': 'p', 'data_type': 'int', 'expression': '1'}]}, [], ['S10']),
    ('duplicate column across lists', {'virtual_columns': [{'name': 'ID', 'data_type': 'varchar', 'expression': 'METADATA$FILENAME'}]}, [], ['S11']),
    ('constraint on an undeclared column', {'constraints': [{'type': 'unique', 'columns': ['missing']}]}, [], ['S12']),
    ('row access policy on an undeclared column', {'row_access_policy': {'name': 'governance.policies.mask_id', 'on': ['missing']}}, [], ['S12']),
    ('lookback_hours with manual refresh', {'registration_check': {'lookback_hours': 6}}, [], ['S13']),
  ] -%}
  {%- for name, overrides, remove, expected in cases -%}
    {%- do assert_equal('validate: ' ~ name, error_codes(validate(with_overrides(base, overrides, remove))), expected) -%}
  {%- endfor -%}

  {%- do assert_equal('S12 is skipped when columns are inferred',
    error_codes(validate({'base_location': 'sales/', 'constraints': [{'type': 'unique', 'columns': ['inferred']}]})), []) -%}
  {%- do assert_equal('lookback_hours with auto refresh',
    validate({'base_location': 'sales/', 'auto_refresh': true, 'registration_check': {'lookback_hours': 6}}), []) -%}
  {%- do assert_equal('S14 model body with SQL', error_codes(validate(base, {}, body ~ 'select 1')), ['S14']) -%}
  {%- do assert_equal('S15 cluster_by', error_codes(validate(base, {'cluster_by': ['id']}, '')), ['S15']) -%}
  {%- do assert_equal('S15 enforced contract', error_codes(validate(base, {'contract': {'enforced': true}}, '')), ['S15']) -%}
  {%- do assert_equal('S15 unenforced contract', validate(base, {'contract': {'enforced': false}}, ''), []) -%}
  {%- do assert_equal('all violations are collected',
    error_codes(validate({'base_location': 'sales', 'auto_refresh': 'no', 'bogus': 1})), ['S01', 'S02', 'S03']) -%}
{% endmacro %}

{% macro test_preflight() %}
  {%- set preflight = dbt_snowflake_parquet_direct.parquet_direct__preflight -%}
  {%- set declaration = dbt_snowflake_parquet_direct.parquet_direct__normalize(full_config()) -%}
  {%- set absent = {'exists': false} -%}
  {%- set hive = {'name': 'HIVE_CATALOG', 'catalog_source': 'OBJECT_STORE', 'table_format': 'HIVE'} -%}
  {%- set glue = {'name': 'GLUE', 'catalog_source': 'GLUE', 'table_format': ''} -%}

  {%- do assert_equal('preflight passes', preflight(declaration, none_catalog(), absent, false), {'errors': [], 'warnings': []}) -%}
  {%- do assert_equal('R01 no catalog', error_codes(preflight(declaration, none, absent, false).errors), ['R01']) -%}
  {%- do assert_equal('R01 not an object store catalog', error_codes(preflight(declaration, glue, absent, false).errors), ['R01']) -%}
  {%- set delta = {'name': 'DELTA', 'catalog_source': 'OBJECT_STORE', 'table_format': 'DELTA'} -%}
  {%- do assert_equal('R01 an object store catalog for Delta files', error_codes(preflight(declaration, delta, absent, false).errors), ['R01']) -%}
  {%- do assert_equal('R02 partition columns with HIVE', error_codes(preflight(declaration, hive, absent, false).errors), ['R02']) -%}
  {%- set hive_declaration = dbt_snowflake_parquet_direct.parquet_direct__normalize({'base_location': 'sales/'}) -%}
  {%- do assert_equal('HIVE without partition columns', preflight(hive_declaration, hive, absent, false).errors, []) -%}
  {%- do assert_equal('R03 another object', error_codes(preflight(declaration, none_catalog(), {'exists': true, 'is_parquet_direct': false, 'object_type': 'view'}, true).errors), ['R03']) -%}
  {%- set lazy_declaration = dbt_snowflake_parquet_direct.parquet_direct__normalize(with_overrides(full_config(), {'refresh_on_create': false})) -%}
  {%- set existing = {'exists': true, 'is_parquet_direct': true} -%}
  {%- do assert_equal('R04 re-creation without refresh on create', error_codes(preflight(lazy_declaration, none_catalog(), existing, true).warnings), ['R04']) -%}
  {%- do assert_equal('R04 only on full refresh', preflight(lazy_declaration, none_catalog(), existing, false).warnings, []) -%}
{% endmacro %}

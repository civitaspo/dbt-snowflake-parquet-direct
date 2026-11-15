{#- Declared types compared with the type DESC ICEBERG TABLE reports. -#}
{% macro test_type_comparison() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set equal = [
    ('int', 'NUMBER(10,0)'), ('Integer', 'NUMBER(10,0)'), ('long', 'NUMBER(19,0)'),
    ('number', 'NUMBER(38,0)'), ('decimal(12)', 'NUMBER(12,0)'), ('number(12, 2)', 'NUMBER(12,2)'),
    ('string', 'VARCHAR(134217728)'), ('varchar(10)', 'VARCHAR(134217728)'),
    ('double', 'FLOAT'), ('float', 'FLOAT'), ('timestamp', 'TIMESTAMP_NTZ(6)'), ('timestamp_ntz(3)', 'TIMESTAMP_NTZ(3)'),
    ('timestamp_ltz', 'TIMESTAMP_LTZ(6)'), ('time', 'TIME(6)'), ('binary', 'BINARY(8388608)'), ('date', 'DATE'),
    ('boolean', 'BOOLEAN'), ('variant', 'VARIANT'), ('array(long)', 'ARRAY(LONG)'), ('array(int)', 'ARRAY(NUMBER(10,0))'),
    ('object(n long, m string)', 'OBJECT(n NUMBER(19,0), m VARCHAR(134217728))'), ('map(string, int)', 'MAP(VARCHAR(134217728), NUMBER(10,0))'),
    ('double precision', 'FLOAT'),
  ] -%}
  {#- declared, actual, schema evolution, expected [changes, notes] -#}
  {%- set cases = [
    ('int', 'NUMBER(19,0)', false, [1, 0]),
    ('int', 'NUMBER(19,0)', true, [0, 1]),
    ('number(8,2)', 'NUMBER(12,2)', true, [0, 1]),
    ('number(12,2)', 'NUMBER(12,3)', true, [1, 0]),
    ('long', 'NUMBER(10,0)', true, [1, 0]),
    ('array(int)', 'ARRAY(LONG)', true, [0, 1]),
    ('object(n int, m string)', 'OBJECT(n NUMBER(19,0), m VARCHAR(134217728))', true, [0, 1]),
    ('array(int)', 'ARRAY(VARCHAR(134217728))', true, [1, 0]),
    ('object(n int)', 'OBJECT(n NUMBER(19,0), extra VARCHAR(134217728))', true, [1, 0]),
    ('array(number(12,2))', 'ARRAY(NUMBER(12,3))', true, [1, 0]),
    ('string', 'NUMBER(19,0)', true, [1, 0]),
  ] -%}
  {%- for declared, actual in equal -%}
    {%- do cases.append((declared, actual, false, [0, 0])) -%}
  {%- endfor -%}
  {%- for declared, actual, evolution, expected in cases -%}
    {%- set declaration = package.parquet_direct__normalize({'base_location': 'a/', 'enable_schema_evolution': evolution, 'columns': [{'name': 'c', 'data_type': declared}]}) -%}
    {%- set state = {'base_location': 'a/', 'auto_refresh': false, 'columns': [{'name': 'C', 'type': actual, 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none}]} -%}
    {%- set result = package.parquet_direct__diff(test_relation(), declaration, state, none_catalog()) -%}
    {%- do assert_equal('type ' ~ declared ~ ' vs ' ~ actual ~ (' with evolution' if evolution else ''), [result.changes | length, result.notes | length], expected) -%}
  {%- endfor -%}
{% endmacro %}

{% macro test_diff() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set relation = test_relation() -%}
  {%- set declaration = package.parquet_direct__normalize(full_config()) -%}

  {%- set result = package.parquet_direct__diff(relation, declaration, full_state(), none_catalog()) -%}
  {%- do assert_equal('matching table has no changes', [result.changes, result.notes], [[], []]) -%}
  {%- set alter = 'alter iceberg table ANALYTICS.SALES.ORDERS' -%}
  {%- do assert_equal('declared masking policies are re-applied on every run', result.reapply,
    [alter ~ ' alter column amount set masking policy governance.policies.mask_amount force']) -%}
  {%- for name, using, expected in [
    ('using added', ['amount', 'id'], ' using (amount, id)'),
    ('using removed', none, ''),
  ] -%}
    {%- set policy_declaration = package.parquet_direct__normalize(with_overrides(full_config(), {'columns': [full_config().columns[0], {'name': 'amount', 'data_type': 'number(12,2)', 'masking_policy': {'name': 'governance.policies.mask_amount', 'using': using}, 'comment': "it's money"}]})) -%}
    {%- set policy_result = package.parquet_direct__diff(relation, policy_declaration, full_state(), none_catalog()) -%}
    {%- do assert_equal('masking policy ' ~ name, [policy_result.changes, policy_result.reapply],
      [[], [alter ~ ' alter column amount set masking policy governance.policies.mask_amount' ~ expected ~ ' force']]) -%}
  {%- endfor -%}

  {#- Each case: name, config overrides, state overrides, expected [key, alterable, statements] list. -#}
  {%- set alter = 'alter iceberg table ANALYTICS.SALES.ORDERS' -%}
  {%- set columns = full_state().columns -%}
  {%- set cases = [
    ('base_location', {'base_location': 'sales_v2/'}, {}, [['base_location', false, []]]),
    ('base_location trailing slash is ignored', {}, {'base_location': 'sales'}, []),
    ('external_volume', {'external_volume': 'other_volume'}, {}, [['external_volume', false, []]]),
    ('omitted external_volume is not compared', {'external_volume': none}, {'external_volume': 'ANYTHING'}, []),
    ('iceberg_version', {'iceberg_version': 2}, {}, [['iceberg_version', false, []]]),
    ('create-time options are not compared', {'pattern': '.*', 'enable_schema_inference': true, 'match_by_column_name': 'case_sensitive', 'refresh_on_create': false}, {}, []),
    ('auto_refresh', {}, {'auto_refresh': true}, [['auto_refresh', true, [alter ~ ' set auto_refresh = false']]]),
    ('replace_invalid_characters', {'replace_invalid_characters': true}, {}, [['replace_invalid_characters', true, [alter ~ ' set replace_invalid_characters = true']]]),
    ('log_event_level', {'log_event_level': 'warn'}, {}, [['log_event_level', true, [alter ~ ' set log_event_level = WARN']]]),
    ('comment', {'comment': "New's"}, {}, [['comment', true, [alter ~ " set comment = 'New\\'s'"]]]),
    ('omitted comment is not compared', {'comment': none}, {'comment': 'Other'}, []),
    ('row access policy', {'row_access_policy': {'name': 'governance.policies.row_access_policy_v2', 'on': ['id']}}, {}, [['row_access_policy', true, [alter ~ ' drop row access policy GOVERNANCE.POLICIES.ROW_ACCESS_POLICY, add row access policy governance.policies.row_access_policy_v2 on (id)']]]),
    ('new row access policy', {}, {'row_access_policy': none}, [['row_access_policy', true, [alter ~ ' add row access policy governance.policies.row_access_policy on (id)']]]),
    ('table tag', {'tag': {'governance.tags.domain': 'finance', 'governance.tags.owner': 'data'}}, {}, [['tag', true, [alter ~ " set tag governance.tags.domain = 'finance', governance.tags.owner = 'data'"]]]),
    ('extra table tags are left alone', {}, {'table_tags': {'GOVERNANCE.TAGS.DOMAIN': 'sales', 'GOVERNANCE.TAGS.OTHER': 'x'}}, []),
    ('missing column', {}, {'columns': columns[1:]}, [['columns', false, []]]),
    ('column type', {}, {'columns': [columns[0], {'name': 'AMOUNT', 'type': 'NUMBER(12,3)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': "it's money"}] + columns[2:]}, [['columns', false, []]]),
    ('column kind', {}, {'columns': columns[:2] + [{'name': 'SOURCE_FILE', 'type': 'VARCHAR(134217728)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none}] + columns[3:]}, [['columns', false, []], ['columns', false, []]]),
    ('expression', {'virtual_columns': [{'name': 'source_file', 'data_type': 'varchar', 'expression': 'LOWER(METADATA$FILENAME)'}]}, {}, [['columns', false, []]]),
    ('a trailing cast matches the CAST form', {}, {'columns': columns[:3] + [{'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': "CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1) AS DATE)", 'comment': none}]}, []),
    ('wrapping parentheses are ignored', {}, {'columns': columns[:3] + [{'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': "(( SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE ))", 'comment': none}]}, []),
    ('a different cast target is a change', {}, {'columns': columns[:3] + [{'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': "CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1) AS VARCHAR)", 'comment': none}]}, [['columns', false, []]]),
    ('a changed string literal is a change', {}, {'columns': columns[:3] + [{'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'D=', 2), '/', 1)::DATE", 'comment': none}]}, [['columns', false, []]]),
    ('whitespace inside a literal is a change', {}, {'columns': columns[:3] + [{'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd= ', 2), '/', 1)::DATE", 'comment': none}]}, [['columns', false, []]]),
    ('parentheses inside a literal do not confuse unwrapping', {'virtual_columns': [{'name': 'source_file', 'data_type': 'varchar', 'expression': "(CONCAT(METADATA$FILENAME, ')('))"}]}, {'columns': columns[:2] + [{'name': 'SOURCE_FILE', 'type': 'VARCHAR(134217728)', 'kind': 'VIRTUAL', 'nullable': true, 'expression': "concat(metadata$filename, ')(')", 'comment': none}] + columns[3:]}, []),
    ('expression whitespace and case are ignored', {'virtual_columns': [{'name': 'source_file', 'data_type': 'string', 'expression': '  metadata$filename '}]}, {}, []),
    ('not_null', {}, {'columns': [{'name': 'ID', 'type': 'NUMBER(19,0)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none}] + columns[1:]}, [['columns', false, []]]),
    ('undeclared column', {}, {'columns': columns + [{'name': 'EXTRA', 'type': 'VARCHAR(134217728)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none}]}, [['columns', false, []]]),
    ('column comment', {}, {'columns': [columns[0], {'name': 'AMOUNT', 'type': 'NUMBER(12,2)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': 'old'}] + columns[2:]}, [['columns', true, [alter ~ " alter column amount comment 'it\\'s money'"]]]),
    ('masking policy', {}, {'masking_policies': {'AMOUNT': 'GOVERNANCE.POLICIES.OLD'}}, [['columns', true, [alter ~ ' alter column amount set masking policy governance.policies.mask_amount force']]]),
    ('column tag', {'columns': [full_config().columns[0], {'name': 'amount', 'data_type': 'number(12,2)', 'masking_policy': 'governance.policies.mask_amount', 'comment': "it's money", 'tag': {'governance.tags.pii': 'yes'}}]}, {}, [['columns', true, [alter ~ " alter column amount set tag governance.tags.pii = 'yes'"]]]),
    ('missing constraint is added', {}, {'constraints': full_state().constraints[:1]}, [['constraints', true, [alter ~ ' add constraint unique_amount unique ("AMOUNT")']]]),
    ('changed constraint is dropped and added', {'constraints': [{'type': 'unique', 'name': 'unique_amount', 'columns': ['amount'], 'rely': true}]}, {}, [['constraints', true, [alter ~ ' drop constraint "UNIQUE_AMOUNT"', alter ~ ' add constraint unique_amount unique ("AMOUNT") rely']]]),
    ('renamed constraint is dropped and added', {'constraints': [{'type': 'unique', 'name': 'renamed_amount', 'columns': ['amount']}]}, {}, [['constraints', true, [alter ~ ' drop constraint "UNIQUE_AMOUNT"', alter ~ ' add constraint renamed_amount unique ("AMOUNT")']]]),
    ('an omitted constraint name accepts the generated one', {'constraints': [{'type': 'unique', 'columns': ['amount']}]}, {}, []),
    ('a quoted constraint name keeps its case', {'constraints': [{'type': 'unique', 'name': '"Unique_Amount"', 'columns': ['amount']}]}, {'constraints': full_state().constraints[:1] + [{'type': 'unique', 'name': 'Unique_Amount', 'columns': ['AMOUNT'], 'rely': false, 'references': none}]}, []),
  ] -%}
  {%- for name, config_overrides, state_overrides, expected in cases -%}
    {%- set case_declaration = package.parquet_direct__normalize(with_overrides(full_config(), config_overrides)) -%}
    {%- set changes = package.parquet_direct__diff(relation, case_declaration, with_overrides(full_state(), state_overrides), none_catalog()).changes -%}
    {%- set actual = [] -%}
    {%- for change in changes -%}{%- do actual.append([change.key, change.alterable, change.statements]) -%}{%- endfor -%}
    {%- do assert_equal('diff: ' ~ name, actual, expected) -%}
  {%- endfor -%}

  {#- Changes Snowflake makes on its own are notes, not changes. -#}
  {%- set evolving = package.parquet_direct__normalize({'base_location': 'sales/', 'enable_schema_evolution': true, 'columns': [{'name': 'id', 'data_type': 'int'}]}) -%}
  {%- set evolved = {'base_location': 'sales/', 'auto_refresh': false, 'columns': [
    {'name': 'ID', 'type': 'NUMBER(19,0)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none},
    {'name': 'EXTRA', 'type': 'VARCHAR(134217728)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none},
  ]} -%}
  {%- set result = package.parquet_direct__diff(relation, evolving, evolved, none_catalog()) -%}
  {%- do assert_equal('schema evolution changes', [result.changes, result.notes | length], [[], 2]) -%}
  {%- set fixed = package.parquet_direct__normalize({'base_location': 'sales/', 'columns': [{'name': 'id', 'data_type': 'int'}]}) -%}
  {%- do assert_equal('widening without schema evolution', package.parquet_direct__diff(relation, fixed, evolved, none_catalog()).changes | length, 2) -%}

  {%- set hive_state = {'base_location': 'sales/', 'auto_refresh': false, 'columns': [
    {'name': 'ID', 'type': 'NUMBER(10,0)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none},
    {'name': 'D', 'type': 'DATE', 'kind': 'PARTITION', 'nullable': true, 'expression': 'IFF(...)', 'comment': none},
  ]} -%}
  {%- set hive = {'name': 'H', 'catalog_source': 'OBJECT_STORE', 'table_format': 'HIVE'} -%}
  {%- set result = package.parquet_direct__diff(relation, fixed, hive_state, hive) -%}
  {%- do assert_equal('hive partition column', [result.changes, result.notes | length], [[], 1]) -%}
  {%- do assert_equal('partition column without HIVE', package.parquet_direct__diff(relation, fixed, hive_state, none_catalog()).changes | length, 1) -%}
{% endmacro %}

{% macro test_recreation_error() %}
  {%- set changes = [
    {'key': 'base_location', 'description': "base_location: 'sales/' -> 'sales_v2/'", 'alterable': false, 'statements': []},
    {'key': 'auto_refresh', 'description': 'auto_refresh: false -> true', 'alterable': true, 'statements': ['x']},
  ] -%}
  {%- set message = dbt_snowflake_parquet_direct.parquet_direct__recreation_error(test_relation(), changes, 'sales', true) -%}
  {%- set expected -%}
Parquet Direct table ANALYTICS.SALES.ORDERS has changes that ALTER cannot apply:
  - base_location: 'sales/' -> 'sales_v2/'
Changes that ALTER can apply (not applied):
  - auto_refresh: false -> true
Run `dbt run --full-refresh -s sales` to re-create the table.
Re-creating re-registers every file (data registration charges apply),
drops the table history, and makes streams on the table stale.
This model sets full_refresh: false; remove it before running --full-refresh.
  {%- endset -%}
  {%- do assert_equal('recreation error', message, expected | trim) -%}
  {%- set short = dbt_snowflake_parquet_direct.parquet_direct__recreation_error(test_relation(), changes[:1], 'sales', false) -%}
  {%- do assert_equal('recreation error without full_refresh: false', short.split('\n') | length, 5) -%}
{% endmacro %}

{#- Declared expressions and the form Snowflake stored for them, observed with DESC ICEBERG TABLE on 2026-11-15. -#}
{% macro test_snowflake_stored_forms() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set observed = [
    ('varchar', "SPLIT_PART(METADATA$FILENAME, '/', -1)::VARCHAR", "SPLIT_PART(METADATA$FILENAME, '/', -1)"),
    ('date', "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE", "CAST(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1) AS DATE)"),
    ('date', "CAST(SPLIT_PART(METADATA$FILENAME, '/', 1) AS DATE)", "CAST(SPLIT_PART(METADATA$FILENAME, '/', 1) AS DATE)"),
    ('varchar', "METADATA$FILENAME", "METADATA$FILENAME"),
    ('varchar', "lower(split_part(METADATA$FILENAME, 'd=', 2))", "LOWER(SPLIT_PART(METADATA$FILENAME, 'd=', 2))"),
    ('varchar', "(METADATA$FILENAME)", "METADATA$FILENAME"),
    ('varchar', "METADATA$FILENAME || '-x'", "METADATA$FILENAME || '-x'"),
  ] -%}
  {%- set changed = [
    ('a cast to another type', 'varchar', "SPLIT_PART(METADATA$FILENAME, '/', 1)::DATE", "SPLIT_PART(METADATA$FILENAME, '/', 1)"),
    ('two casts joined', 'date', "CAST(a AS DATE) || CAST(b AS DATE)", "a || CAST(b AS DATE)"),
    ('a different literal', 'varchar', "lower(split_part(METADATA$FILENAME, 'D=', 2))", "LOWER(SPLIT_PART(METADATA$FILENAME, 'd=', 2))"),
  ] -%}
  {%- set cases = [] -%}
  {%- for data_type, declared, stored in observed -%}{%- do cases.append(('stored form of ' ~ declared, data_type, declared, stored, 0)) -%}{%- endfor -%}
  {%- for name, data_type, declared, stored in changed -%}{%- do cases.append((name, data_type, declared, stored, 1)) -%}{%- endfor -%}
  {%- for name, data_type, declared, stored, expected in cases -%}
    {%- set declaration = package.parquet_direct__normalize({'base_location': 'a/', 'virtual_columns': [{'name': 'v', 'data_type': data_type, 'expression': declared}]}) -%}
    {%- set state = {'base_location': 'a/', 'auto_refresh': false, 'columns': [{'name': 'V', 'type': package.parquet_direct__normalize_type(data_type), 'kind': 'VIRTUAL', 'nullable': true, 'expression': stored, 'comment': none}]} -%}
    {%- do assert_equal(name, package.parquet_direct__diff(test_relation(), declaration, state, none_catalog()).changes | length, expected) -%}
  {%- endfor -%}

  {#- Snowflake made a primary key column NOT NULL although the model did not set not_null (observed on 2026-11-15). -#}
  {%- set not_null_state = {'base_location': 'a/', 'auto_refresh': false, 'columns': [{'name': 'ID', 'type': 'NUMBER(10,0)', 'kind': 'COLUMN', 'nullable': false, 'expression': none, 'comment': none}]} -%}
  {%- for name, column, expected in [
    ('a primary key column is not null', {'name': 'id', 'data_type': 'int', 'constraints': [{'type': 'primary_key', 'rely': true}]}, 0),
    ('a column without primary key or not_null is nullable', {'name': 'id', 'data_type': 'int'}, 1),
  ] -%}
    {%- set declaration = package.parquet_direct__normalize({'base_location': 'a/', 'columns': [column]}) -%}
    {%- set changes = package.parquet_direct__diff(test_relation(), declaration, not_null_state, none_catalog()).changes | rejectattr('key', 'equalto', 'constraints') | list -%}
    {%- do assert_equal(name, changes | length, expected) -%}
  {%- endfor -%}
{% endmacro %}

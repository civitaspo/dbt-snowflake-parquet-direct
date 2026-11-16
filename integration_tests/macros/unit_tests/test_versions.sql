{# Previous versions: validation, the current table's declaration, pre-flight, and the combined view. #}

{% macro versioned_config(overrides={}) %}
  {%- set config = with_overrides(full_config(), {
    'refresh_on_create': false,
    'previous_versions': [{'table': 'orders_before_amount_scale', 'columns': {'amount': 'amount_cents / 100'}}],
  }) -%}
  {{ return(with_overrides(config, overrides)) }}
{% endmacro %}

{% macro test_previous_versions_validation() %}
  {%- set validate = dbt_snowflake_parquet_direct.parquet_direct__validate -%}
  {%- do assert_equal('previous versions are valid', validate(versioned_config()), []) -%}
  {%- set cases = [
    ('not a list', {'previous_versions': 'orders_old'}, ['S02']),
    ('not a mapping', {'previous_versions': ['orders_old']}, ['S02']),
    ('unknown key', {'previous_versions': [{'table': 'orders_old', 'where': 'd < 1'}]}, ['S01']),
    ('qualified table name', {'previous_versions': [{'table': 'sales.orders_old'}]}, ['S02']),
    ('quoted table name', {'previous_versions': [{'table': '"Orders Old"'}]}, []),
    ('embedded double quote', {'previous_versions': [{'table': '"Orders ""Old"""'}]}, ['S02']),
    ('table listed twice', {'previous_versions': [{'table': 'orders_old'}, {'table': 'ORDERS_OLD'}]}, ['S16']),
    ('columns not a mapping', {'previous_versions': [{'table': 'orders_old', 'columns': ['amount']}]}, ['S02']),
    ('empty expression', {'previous_versions': [{'table': 'orders_old', 'columns': {'amount': ' '}}]}, ['S02']),
    ('undeclared column', {'previous_versions': [{'table': 'orders_old', 'columns': {'amount_cents': 'amount_cents / 100'}}]}, ['S16']),
    ('auto refresh', {'auto_refresh': true, 'refresh_paths': none, 'registration_check': none}, ['S17']),
  ] -%}
  {%- for name, overrides, expected in cases -%}
    {%- do assert_equal('previous_versions: ' ~ name, error_codes(validate(versioned_config(overrides))), expected) -%}
  {%- endfor -%}
  {%- do assert_equal('previous_versions need declared columns',
    error_codes(validate({'base_location': 'sales/', 'previous_versions': [{'table': 'orders_old'}]})), ['S16']) -%}
{% endmacro %}

{% macro test_previous_versions_declaration() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set declaration = package.parquet_direct__normalize(versioned_config({'previous_versions': [
    {'table': 'orders_before_amount_scale', 'columns': {'AMOUNT': 'amount_cents / 100'}},
    {'table': '"Orders Old"'},
  ]})) -%}
  {%- do assert_equal('expressions are keyed by column identifier', declaration.previous_versions, [
    {'table': 'orders_before_amount_scale', 'identifier': 'ORDERS_BEFORE_AMOUNT_SCALE', 'columns': {'AMOUNT': 'amount_cents / 100'}},
    {'table': '"Orders Old"', 'identifier': 'Orders Old', 'columns': {}},
  ]) -%}

  {%- set table = package.parquet_direct__table_declaration(declaration) -%}
  {%- do assert_equal('the current table has no governance', [
    table.columns | selectattr('masking_policy') | list, table.columns | selectattr('tag') | list,
    table.columns | rejectattr('comment', 'none') | list, table.comment, table.tag, table.row_access_policy,
  ], [[], [], [], none, {}, none]) -%}
  {%- do assert_equal('the current table keeps its columns and constraints', [
    table.columns | map(attribute='identifier') | list, table.partition_columns | map(attribute='identifier') | list, table.constraints,
  ], [declaration.columns | map(attribute='identifier') | list, ['D'], declaration.constraints]) -%}
  {%- do assert_equal('the view declaration keeps governance', declaration.columns[1].masking_policy.name, 'governance.policies.mask_amount') -%}
  {%- set plain = package.parquet_direct__normalize(full_config()) -%}
  {%- do assert_equal('without previous versions the declaration is unchanged', package.parquet_direct__table_declaration(plain), plain) -%}

  {%- do assert_equal('current table name', package.parquet_direct__current_relation(test_relation()) | string, 'ANALYTICS.SALES.ORDERS__current') -%}
  {%- do assert_equal('previous version name keeps its case', package.parquet_direct__previous_relation(test_relation(), 'Orders Old') | string, 'ANALYTICS.SALES."Orders Old"') -%}
{% endmacro %}

{% macro test_previous_versions_preflight() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set relation = test_relation() -%}
  {%- set declaration = package.parquet_direct__normalize(versioned_config()) -%}
  {%- set eager = package.parquet_direct__normalize(versioned_config({'refresh_on_create': true})) -%}
  {%- set absent = {'exists': false} -%}
  {%- set view = {'exists': true, 'is_parquet_direct': false, 'object_type': 'view', 'text': 'create or alter view ANALYTICS.SALES.ORDERS as select * from analytics.sales.orders__current'} -%}
  {%- set table = {'exists': true, 'is_parquet_direct': true, 'object_type': 'iceberg table', 'base_location': 'sales/'} -%}
  {%- set previous_relation = package.parquet_direct__previous_relation(relation, 'ORDERS_BEFORE_AMOUNT_SCALE') -%}
  {%- set missing = [{'version': declaration.previous_versions[0], 'relation': previous_relation, 'state': absent, 'columns': {}}] -%}
  {%- set present = [{'version': declaration.previous_versions[0], 'relation': previous_relation, 'state': table, 'columns': {}}] -%}
  {%- set elsewhere = [{'version': declaration.previous_versions[0], 'relation': previous_relation, 'state': {'exists': true, 'object_type': 'iceberg table', 'base_location': 'sales_v1/'}, 'columns': {}}] -%}
  {%- set preflight = package.parquet_direct__versions_preflight -%}

  {%- do assert_equal('first transition names the public table to rename', preflight(relation, declaration, table, absent, missing, true),
    ['R06: previous version table ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE" does not exist; rename the table that holds its files first: alter iceberg table ANALYTICS.SALES.ORDERS rename to ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE"']) -%}
  {%- do assert_equal('a later transition names the current table to rename', preflight(relation, declaration, view, table, missing, false),
    ['R06: previous version table ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE" does not exist; rename the table that holds its files first: alter iceberg table ANALYTICS.SALES.ORDERS__current rename to ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE"']) -%}
  {%- do assert_equal('a missing table without a source has no hint', preflight(relation, declaration, absent, absent, missing, true),
    ['R06: previous version table ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE" does not exist']) -%}
  {%- do assert_equal('the public table must be renamed', error_codes(preflight(relation, declaration, table, absent, present, true)), ['R07']) -%}
  {%- do assert_equal('the combined view is rebuilt', preflight(relation, declaration, view, table, present, false), []) -%}
  {%- set unrelated = {'exists': true, 'is_parquet_direct': false, 'object_type': 'view', 'text': 'create view ANALYTICS.SALES.ORDERS as select 1 as id'} -%}
  {%- do assert_equal('a view that does not read the current table is not replaced', preflight(relation, declaration, unrelated, table, present, false),
    ['R10: view ANALYTICS.SALES.ORDERS does not read ANALYTICS.SALES.ORDERS__current, so previous_versions did not create it; drop or rename it']) -%}
  {%- set reserved = package.parquet_direct__normalize(versioned_config({'previous_versions': [{'table': 'orders__current'}]})) -%}
  {%- do assert_equal('the current table is not a previous version',
    error_codes(preflight(relation, reserved, view, table, [{'version': reserved.previous_versions[0], 'relation': relation, 'state': table, 'columns': {}}], false)), ['R05']) -%}
  {%- do assert_equal('a shared base_location needs refresh_on_create: false', error_codes(preflight(relation, eager, absent, absent, present, true)), ['R08']) -%}
  {%- set whole = package.parquet_direct__normalize(versioned_config({'refresh_paths': [], 'skip_refresh_if_refresh_paths_empty': false})) -%}
  {%- do assert_equal('a shared base_location never refreshes every file', preflight(relation, whole, view, table, present, false),
    ["R08: previous version ANALYTICS.SALES.\"ORDERS_BEFORE_AMOUNT_SCALE\" reads base_location 'sales/', which overlaps 'sales/'; set refresh_paths, or skip_refresh_if_refresh_paths_empty: true, so the refresh never reads every file"]) -%}
  {%- set skipped = package.parquet_direct__normalize(versioned_config({'refresh_paths': []})) -%}
  {%- do assert_equal('empty refresh_paths are skipped by default', preflight(relation, skipped, view, table, present, false), []) -%}
  {%- do assert_equal('a shared base_location with refresh_on_create: false', preflight(relation, declaration, absent, absent, present, true), []) -%}
  {%- do assert_equal('a separate base_location may refresh on create', preflight(relation, eager, absent, absent, elsewhere, true), []) -%}
  {%- do assert_equal('an existing current table is not created', preflight(relation, eager, view, table, present, false), []) -%}
{% endmacro %}

{% macro test_combined_view() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set relation = test_relation() -%}
  {%- set declaration = package.parquet_direct__normalize(versioned_config()) -%}
  {%- set previous_relation = package.parquet_direct__previous_relation(relation, 'ORDERS_BEFORE_AMOUNT_SCALE') -%}
  {%- set item = {'version': declaration.previous_versions[0], 'relation': previous_relation, 'state': {'exists': true}, 'columns': {
    'ID': 'NUMBER(10,0)', 'AMOUNT_CENTS': 'NUMBER(19,0)', 'SOURCE_FILE': 'VARCHAR(134217728)',
  }} -%}
  {%- set select = package.parquet_direct__previous_version_select(declaration, item, 0) -%}
  {%- do assert_equal('expression, widening, and missing columns', select, {'errors': [], 'sql':
    'select\n'
    ~ '    cast("ID" as NUMBER(19,0)) as "ID",\n'
    ~ '    cast((amount_cents / 100) as NUMBER(12,2)) as "AMOUNT",\n'
    ~ '    cast("SOURCE_FILE" as VARCHAR(134217728)) as "SOURCE_FILE",\n'
    ~ '    cast(null as DATE) as "D"\n'
    ~ 'from ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE"'}) -%}

  {%- do item.columns.update({'D': 'VARCHAR(16777216)'}) -%}
  {%- do assert_equal('an incompatible type needs an expression', package.parquet_direct__previous_version_select(declaration, item, 0).errors,
    ['R09: column D is VARCHAR(134217728) in ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE" but DATE in the model; set previous_versions[0].columns.d to an expression that converts it']) -%}
  {%- do item.columns.update({'D': 'DATE', 'AMOUNT': 'NUMBER(12,3)'}) -%}
  {%- do assert_equal('a declared expression wins over an incompatible column', package.parquet_direct__previous_version_select(declaration, item, 0).errors, []) -%}

  {%- set view_relation = relation.incorporate(type='view') -%}
  {%- set definition = 'create or alter view ANALYTICS.SALES.ORDERS\n'
    ~ '(\n'
    ~ '    id,\n'
    ~ "    amount comment 'it\\'s money',\n"
    ~ '    source_file,\n'
    ~ '    d\n'
    ~ ')\n'
    ~ "comment = 'Sales'\n"
    ~ 'as\n'
    ~ 'select 1 as "ID"\n'
    ~ 'union all by name\n'
    ~ 'select "ID", "AMOUNT", "SOURCE_FILE", "D"\n'
    ~ 'from ANALYTICS.SALES.ORDERS__current' -%}
  {%- set view_statements = package.parquet_direct__view_statements -%}
  {%- do assert_equal('a new view carries its policies and tags', view_statements(view_relation, declaration, ['select 1 as "ID"'], {'exists': false}), {
    'create': [
      'create view ANALYTICS.SALES.ORDERS\n'
      ~ '(\n'
      ~ '    id,\n'
      ~ "    amount with masking policy governance.policies.mask_amount comment 'it\\'s money',\n"
      ~ '    source_file,\n'
      ~ '    d\n'
      ~ ')\n'
      ~ 'with row access policy governance.policies.row_access_policy on (id)\n'
      ~ "with tag (governance.tags.domain = 'sales')\n"
      ~ "comment = 'Sales'\n"
      ~ 'as\n'
      ~ 'select 1 as "ID"\n'
      ~ 'union all by name\n'
      ~ 'select "ID", "AMOUNT", "SOURCE_FILE", "D"\n'
      ~ 'from ANALYTICS.SALES.ORDERS__current',
    ],
    'changes': [], 'reapply': []}) -%}

  {%- set governed = {
    'masking_policies': {'AMOUNT': 'GOVERNANCE.POLICIES.MASK_AMOUNT'},
    'row_access_policy': {'name': 'GOVERNANCE.POLICIES.ROW_ACCESS_POLICY', 'on': ['ID']},
    'table_tags': {'GOVERNANCE.TAGS.DOMAIN': 'sales'},
    'column_tags': {},
  } -%}
  {%- set comments = {'comment': 'Sales', 'column_comments': {'ID': none, 'AMOUNT': "it's money"}} -%}
  {%- set stored = {'exists': true, 'text': 'create view ANALYTICS.SALES.ORDERS (id, amount with masking policy governance.policies.mask_amount) as \n'
    ~ 'select 1 as "ID"\nunion all by name\nselect "ID", "AMOUNT", "SOURCE_FILE", "D"  \nfrom ANALYTICS.SALES.ORDERS__current\n/* {"app": "dbt"} */'} -%}
  {%- do stored.update(comments) -%}
  {%- do stored.update(governed) -%}
  {%- do assert_equal('an unchanged view is left alone, apart from re-applying masking policies', view_statements(view_relation, declaration, ['select 1 as "ID"'], stored), {
    'create': [], 'changes': [],
    'reapply': ['alter view ANALYTICS.SALES.ORDERS alter column amount set masking policy governance.policies.mask_amount force']}) -%}
  {%- set changed = view_statements(view_relation, declaration, ['select 2 as "ID"'], stored) -%}
  {%- do assert_equal('a changed query is altered in place', [changed.changes | map(attribute='description') | list, changed.changes[0].statements[0]],
    [['view definition changed: query'], definition.replace('select 1', 'select 2')]) -%}
  {%- do stored.update({'comment': 'Old', 'column_comments': {}}) -%}
  {%- do assert_equal('changed comments are altered in place', view_statements(view_relation, declaration, ['select 1 as "ID"'], stored).changes | map(attribute='description') | list,
    ['view definition changed: comment, AMOUNT comment']) -%}
  {%- set ungoverned = {'exists': true, 'text': definition} -%}
  {%- do ungoverned.update(comments) -%}
  {%- set ungoverned = view_statements(view_relation, declaration, ['select 1 as "ID"'], ungoverned) -%}
  {%- do assert_equal('governance changes name the view', ungoverned.changes | map(attribute='description') | list, [
    'view row_access_policy: none -> governance.policies.row_access_policy', 'view tag: governance.tags.domain', 'view columns: AMOUNT masking_policy -> governance.policies.mask_amount',
  ]) -%}
  {%- do assert_equal('declared governance is set with ALTER VIEW', ungoverned.changes | map(attribute='statements') | list, [
    ['alter view ANALYTICS.SALES.ORDERS add row access policy governance.policies.row_access_policy on (id)'],
    ["alter view ANALYTICS.SALES.ORDERS set tag governance.tags.domain = 'sales'"],
    ['alter view ANALYTICS.SALES.ORDERS alter column amount set masking policy governance.policies.mask_amount force'],
  ]) -%}

  {%- set nodes = [
    {'resource_type': 'model', 'relation_name': 'ANALYTICS.SALES.ORDERS', 'config': {'meta': {'parquet_direct': {'previous_versions': [{'table': 'orders_old'}]}}}},
    {'resource_type': 'model', 'relation_name': 'ANALYTICS.SALES.PLAIN', 'config': {'meta': {'parquet_direct': {}}}},
  ] -%}
  {%- do assert_equal('the check reads the current table', package.parquet_direct__registration_relation(relation, nodes) | string, 'ANALYTICS.SALES.ORDERS__current') -%}
  {%- set plain = relation.incorporate(path={'identifier': 'PLAIN'}) -%}
  {%- do assert_equal('the check reads a plain table as is', package.parquet_direct__registration_relation(plain, nodes) | string, 'ANALYTICS.SALES.PLAIN') -%}
{% endmacro %}

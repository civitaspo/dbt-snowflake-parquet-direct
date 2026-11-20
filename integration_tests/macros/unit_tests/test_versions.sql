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
    ('two keys for one column', {'previous_versions': [{'table': 'orders_old', 'columns': {'amount': 'amount_cents / 100', '"AMOUNT"': 'amount_cents'}}]}, ['S16']),
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

  {%- set quoted = with_overrides(versioned_config(), {
    'columns': [{'name': 'id', 'data_type': 'number(19,0)'}, {'name': 'amount', 'data_type': 'number(12,2)'}, {'name': 'a"b', 'data_type': 'number(19,0)', 'quote': true}],
    'previous_versions': [{'table': 'orders_old', 'columns': {'a"b': 'ab', 'ID': 'id'}}],
  }) -%}
  {%- do assert_equal('a key names a column as written, quotes included', [package.parquet_direct__validate(quoted), package.parquet_direct__normalize(quoted).previous_versions[0].columns],
    [[], {'a"b': 'ab', 'ID': 'id'}]) -%}

  {%- set table = package.parquet_direct__table_declaration(declaration) -%}
  {%- do assert_equal('the current table has no governance and is marked by its comment', [
    table.columns | selectattr('masking_policy') | list, table.columns | selectattr('tag') | list,
    table.columns | rejectattr('comment', 'none') | list, table.comment, table.tag, table.row_access_policy,
  ], [[], [], [], package.parquet_direct__current_table_comment(), {}, none]) -%}
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
  {%- set marker = package.parquet_direct__view_marker('0123456789abcdef0123456789abcdef') -%}
  {%- set declaration = package.parquet_direct__normalize(versioned_config()) -%}
  {%- set eager = package.parquet_direct__normalize(versioned_config({'refresh_on_create': true})) -%}
  {%- set absent = {'exists': false} -%}
  {%- set view = {'exists': true, 'is_parquet_direct': false, 'object_type': 'view', 'text': 'create view ANALYTICS.SALES.ORDERS\n' ~ marker ~ '\nas select * from analytics.sales.orders__current'} -%}
  {%- set current = {'exists': true, 'is_parquet_direct': true, 'object_type': 'iceberg table', 'base_location': 'sales/', 'comment': package.parquet_direct__current_table_comment()} -%}
  {%- set old = {'exists': true, 'is_parquet_direct': true, 'object_type': 'iceberg table', 'base_location': 'sales_v1/'} -%}
  {%- set public_table = {'exists': true, 'is_parquet_direct': true, 'object_type': 'iceberg table', 'base_location': 'sales_v1/', 'comment': 'Sales'} -%}
  {%- set previous_relation = package.parquet_direct__previous_relation(relation, 'ORDERS_BEFORE_AMOUNT_SCALE') -%}
  {%- set version = declaration.previous_versions[0] -%}
  {%- set missing = [{'version': version, 'relation': previous_relation, 'state': absent, 'columns': {}}] -%}
  {%- set present = [{'version': version, 'relation': previous_relation, 'state': old, 'columns': {}}] -%}
  {%- set shared_state = {} -%}
  {%- do shared_state.update(old) -%}
  {%- do shared_state.update({'base_location': 'sales/'}) -%}
  {%- set shared = [{'version': version, 'relation': previous_relation, 'state': shared_state, 'columns': {}}] -%}
  {%- set preflight = package.parquet_direct__versions_preflight -%}
  {%- set source = package.parquet_direct__rename_source -%}

  {%- do assert_equal('the first transition renames the public table', source(relation, public_table, absent, missing) | string, 'ANALYTICS.SALES.ORDERS') -%}
  {%- do assert_equal('a later transition renames the current table', source(relation, view, current, missing) | string, 'ANALYTICS.SALES.ORDERS__current') -%}
  {%- do assert_equal('nothing to rename without a source', source(relation, absent, absent, missing), none) -%}
  {%- do assert_equal('nothing to rename when every version exists', source(relation, public_table, absent, present), none) -%}
  {%- set two = package.parquet_direct__normalize(versioned_config({'previous_versions': [{'table': 'orders_oldest'}, {'table': 'orders_before_amount_scale'}]})) -%}
  {%- set oldest = {'version': two.previous_versions[0], 'relation': package.parquet_direct__previous_relation(relation, 'ORDERS_OLDEST'), 'state': absent, 'columns': {}} -%}
  {%- do assert_equal('a missing older version is a wrong name or a dropped table', source(relation, view, current, [oldest] + present), none) -%}
  {%- do assert_equal('a table that is not a Parquet Direct table is never renamed', source(relation, {'exists': true, 'is_parquet_direct': false, 'object_type': 'table'}, absent, missing), none) -%}
  {%- set other_current = {} -%}
  {%- do other_current.update(public_table) -%}
  {%- do other_current.update({'comment': package.parquet_direct__current_table_comment()}) -%}
  {%- do assert_equal('another model''s current table under the model''s name is never renamed', source(relation, other_current, absent, missing), none) -%}
  {%- set foreign = {'exists': true, 'is_parquet_direct': true, 'object_type': 'iceberg table', 'base_location': 'other/', 'comment': 'Orders of another model'} -%}
  {%- do assert_equal('a current table that previous_versions did not create is never renamed', source(relation, view, foreign, missing), none) -%}

  {%- do assert_equal('a missing previous version', preflight(relation, declaration, absent, absent, missing, true),
    ['R06: previous version table ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE" does not exist']) -%}
  {%- do assert_equal('the public table must be renamed', error_codes(preflight(relation, declaration, public_table, absent, present, true)), ['R07']) -%}
  {%- do assert_equal('the combined view is rebuilt', preflight(relation, declaration, view, current, present, false), []) -%}
  {%- set secure = {'exists': true, 'object_type': 'view', 'text': 'create or replace secure view ANALYTICS.SALES."Orders"\n' ~ marker ~ '\n(\n    id\n)\ncopy grants\nas select 1'} -%}
  {%- do assert_equal('a re-created secure view is recognized', preflight(relation, declaration, secure, current, present, false), []) -%}
  {%- set unrelated = {'exists': true, 'is_parquet_direct': false, 'object_type': 'view', 'text': 'create view ANALYTICS.SALES.ORDERS as select 1 as id'} -%}
  {%- do assert_equal('a view that previous_versions did not create is not changed', preflight(relation, declaration, unrelated, current, present, false),
    ['R10: view ANALYTICS.SALES.ORDERS lacks the comment that previous_versions writes after its name, so previous_versions did not create it; drop or rename it']) -%}
  {%- for text in [
    'create view ANALYTICS.SALES.ORDERS as select * from ANALYTICS.SALES.ORDERS__current',
    'create view ANALYTICS.SALES.ORDERS as select * from ANALYTICS.SALES.ORDERS__current_archive',
    'create view ANALYTICS.SALES.ORDERS as select 1 as "from ANALYTICS.SALES.ORDERS__current"',
    "create view ANALYTICS.SALES.ORDERS as select '" ~ marker ~ "' as note",
    'create view ANALYTICS.SALES.ORDERS as select 1 as id -- create view copy ' ~ marker,
  ] -%}
    {%- do assert_equal('a hand-made view is not changed: ' ~ text, error_codes(preflight(relation, declaration, {'exists': true, 'object_type': 'view', 'text': text}, current, present, false)), ['R10']) -%}
  {%- endfor -%}
  {%- do assert_equal('a current table that previous_versions did not create is not taken over', preflight(relation, declaration, absent, foreign, present, false),
    ["R11: ANALYTICS.SALES.ORDERS__current exists without the comment '" ~ package.parquet_direct__current_table_comment() ~ "', so previous_versions did not create it; rename it if another model or process uses it, or set that comment if it holds this model's files"]) -%}
  {%- do assert_equal('a full refresh does not replace it either', error_codes(preflight(relation, declaration, absent, foreign, present, true)), ['R11']) -%}
  {%- set reserved = package.parquet_direct__normalize(versioned_config({'previous_versions': [{'table': 'orders__current'}]})) -%}
  {%- do assert_equal('the current table is not a previous version',
    error_codes(preflight(relation, reserved, view, current, [{'version': reserved.previous_versions[0], 'relation': relation, 'state': old, 'columns': {}}], false)), ['R05']) -%}
  {%- for state in [{'exists': true, 'is_parquet_direct': false, 'object_type': 'view'}, {'exists': true, 'is_parquet_direct': false, 'object_type': 'table'}] -%}
    {%- do assert_equal('a previous version must be a Parquet Direct table, not a ' ~ state.object_type,
      preflight(relation, declaration, view, current, [{'version': version, 'relation': previous_relation, 'state': state, 'columns': {}}], false),
      ['R14: previous version ANALYTICS.SALES."ORDERS_BEFORE_AMOUNT_SCALE" is a ' ~ state.object_type ~ ', not a Parquet Direct table']) -%}
  {%- endfor -%}

  {%- do assert_equal('a current table created behind the existing view registers no files at creation', preflight(relation, eager, view, absent, present, true),
    ['R15: ANALYTICS.SALES.ORDERS__current would be created behind the existing view ANALYTICS.SALES.ORDERS, which reads its rows before this run changes the view; set refresh_on_create: false so its files are registered after the view changes']) -%}
  {%- do assert_equal('so does a full refresh behind the view', error_codes(preflight(relation, eager, view, current, present, true)), ['R15']) -%}
  {%- do assert_equal('refresh_on_create: false behind the view', preflight(relation, declaration, view, absent, present, true), []) -%}
  {%- do assert_equal('the first transition has no view yet, so it may refresh on create', preflight(relation, eager, absent, absent, present, true), []) -%}
  {%- do assert_equal('an existing current table is not created', preflight(relation, eager, view, current, present, false), []) -%}

  {%- set overlap = "R08: previous version ANALYTICS.SALES.\"ORDERS_BEFORE_AMOUNT_SCALE\" reads base_location 'sales/', which overlaps 'sales/'" -%}
  {%- do assert_equal('a shared base_location stops the run even with refresh_paths', preflight(relation, declaration, view, current, shared, false),
    [overlap ~ "; write the new files under a base_location of their own"]) -%}
  {%- set nested = package.parquet_direct__normalize(versioned_config({'base_location': 'sales/v2/'})) -%}
  {%- do assert_equal('a base_location inside a previous version''s overlaps it', error_codes(preflight(relation, nested, view, absent, shared, true)), ['R08']) -%}
  {%- set moved = package.parquet_direct__normalize(versioned_config({'base_location': 'sales_v2/'})) -%}
  {%- do assert_equal('an existing current table keeps its base_location until it is re-created', preflight(relation, moved, view, current, shared, false),
    [overlap ~ " of the existing ANALYTICS.SALES.ORDERS__current; write the new files under a base_location of their own"]) -%}
  {%- do assert_equal('a re-created current table uses the declared base_location', preflight(relation, moved, view, current, shared, true), []) -%}
  {%- set other_volume = [{'version': version, 'relation': previous_relation, 'state': {'exists': true, 'is_parquet_direct': true, 'object_type': 'iceberg table', 'external_volume': 'OTHER_VOLUME', 'base_location': 'sales/'}, 'columns': {}}] -%}
  {%- do assert_equal('the same base_location on another external volume does not overlap', preflight(relation, declaration, view, current, other_volume, false), []) -%}
  {%- set inherited = package.parquet_direct__normalize(versioned_config({'external_volume': none})) -%}
  {%- do assert_equal('an inherited external volume may be any volume', error_codes(preflight(relation, inherited, absent, absent, other_volume, true)), ['R08']) -%}

  {%- set old_volume = {} -%}
  {%- do old_volume.update(current) -%}
  {%- do old_volume.update({'external_volume': 'OLD_VOLUME'}) -%}
  {%- do assert_equal('an existing current table on another external volume stops the run', preflight(relation, declaration, view, old_volume, present, false),
    ['R12: ANALYTICS.SALES.ORDERS__current uses external volume OLD_VOLUME, but the model declares EXAMPLE_VOLUME; declare OLD_VOLUME, or run with --full-refresh to re-create it on EXAMPLE_VOLUME']) -%}
  {%- do assert_equal('a re-created current table uses the declared external volume', preflight(relation, declaration, view, old_volume, present, true), []) -%}
  {%- set inherited_skipped = package.parquet_direct__normalize(versioned_config({'external_volume': none})) -%}
  {%- do assert_equal('an inherited external volume is not compared', preflight(relation, inherited_skipped, view, old_volume, present, false), []) -%}
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
  {%- set structured = package.parquet_direct__normalize(versioned_config({'columns': [{'name': 'payload', 'data_type': 'object(str varchar, num int, nested array(object(k long)))'}], 'row_access_policy': none, 'constraints': [], 'previous_versions': [{'table': 'orders_old'}]})) -%}
  {%- set structured_item = {'version': structured.previous_versions[0], 'relation': previous_relation, 'state': {'exists': true}, 'columns': {'PAYLOAD': 'OBJECT(str VARCHAR(16777216), num NUMBER(10,0), nested ARRAY(OBJECT(k NUMBER(19,0))))'}} -%}
  {%- set structured_select = package.parquet_direct__previous_version_select(structured, structured_item, 0) -%}
  {%- do assert_equal('a structured type keeps its field names in the CAST, which a UNION matches', [structured_select.errors, structured_select.sql.split('\n')[1]],
    [[], '    cast("PAYLOAD" as OBJECT(str VARCHAR(134217728),num NUMBER(10,0),nested ARRAY(OBJECT(k NUMBER(19,0))))) as "PAYLOAD",']) -%}

  {%- set view_relation = relation.incorporate(type='view') -%}
  {%- set view_statements = package.parquet_direct__view_statements -%}
  {%- set created = view_statements(view_relation, declaration, ['select 1 as "ID"'], {'exists': false}) -%}
  {%- set digest = local_md5(created.statements[0].split('\n', 2)[2]) -%}
  {%- do assert_equal('a new view carries its policies, tags, and the digest of its declaration', created, {'differs': false, 'statements': [
    'create view ANALYTICS.SALES.ORDERS\n'
    ~ package.parquet_direct__view_marker(digest) ~ '\n'
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
  ]}) -%}

  {%- set stored = {'exists': true, 'secure': false, 'text': created.statements[0] ~ '\n/* {"app": "dbt"} */'} -%}
  {%- do assert_equal('a view with the same digest is left alone', view_statements(view_relation, declaration, ['select 1 as "ID"'], stored), {'statements': [], 'differs': false}) -%}
  {%- set replaced = {'exists': true, 'secure': false, 'text': created.statements[0].replace('create view', 'create or replace view')} -%}
  {%- do assert_equal('so is a view the package re-created', view_statements(view_relation, declaration, ['select 1 as "ID"'], replaced).differs, false) -%}
  {%- for name, changed in [
    ('the query', declaration),
    ('a comment', package.parquet_direct__normalize(versioned_config({'comment': 'Old'}))),
    ('a masking policy', package.parquet_direct__normalize(versioned_config({'columns': [{'name': 'id', 'data_type': 'number(19,0)'}, {'name': 'amount', 'data_type': 'number(12,2)', 'masking_policy': {'name': 'governance.policies.mask_amount', 'using': ['amount', 'id']}}]}))),
  ] -%}
    {%- set selects = ['select 2 as "ID"'] if name == 'the query' else ['select 1 as "ID"'] -%}
    {%- do assert_equal('a changed declaration re-creates the view: ' ~ name, view_statements(view_relation, changed, selects, stored).differs, true) -%}
  {%- endfor -%}
  {%- set result = view_statements(view_relation, declaration, ['select 2 as "ID"'], stored) -%}
  {%- set expected = view_statements(view_relation, declaration, ['select 2 as "ID"'], {'exists': false}).statements[0].replace('create view', 'create or replace view').replace("comment = 'Sales'\nas", "comment = 'Sales'\ncopy grants\nas") -%}
  {%- do assert_equal('a view that differs is re-created in one statement with its policies and tags', result, {'statements': [expected], 'differs': true}) -%}
  {%- do assert_equal('a re-created secure view stays secure', view_statements(view_relation, declaration, ['select 2 as "ID"'], {'exists': true, 'secure': true, 'text': stored.text}).statements,
    [expected.replace('create or replace view', 'create or replace secure view')]) -%}
  {%- do assert_equal('a view without the marker differs', view_statements(view_relation, declaration, ['select 1 as "ID"'], {'exists': true, 'secure': false, 'text': 'create view ANALYTICS.SALES.ORDERS as select 1 as id'}).differs, true) -%}

  {%- set nodes = [
    {'resource_type': 'model', 'relation_name': 'ANALYTICS.SALES.ORDERS', 'config': {'meta': {'parquet_direct': {'previous_versions': [{'table': 'orders_old'}]}}}},
    {'resource_type': 'model', 'relation_name': 'ANALYTICS.SALES.PLAIN', 'config': {'meta': {'parquet_direct': {}}}},
  ] -%}
  {%- do assert_equal('the check reads the current table', package.parquet_direct__registration_relation(relation, nodes) | string, 'ANALYTICS.SALES.ORDERS__current') -%}
  {%- set plain = relation.incorporate(path={'identifier': 'PLAIN'}) -%}
  {%- do assert_equal('the check reads a plain table as is', package.parquet_direct__registration_relation(plain, nodes) | string, 'ANALYTICS.SALES.PLAIN') -%}
{% endmacro %}

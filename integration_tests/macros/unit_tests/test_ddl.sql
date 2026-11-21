{#- Effects of defaults and documentation settings, observed in the CREATE statement and the diff. -#}
{% macro test_config_effects() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set relation = test_relation() -%}
  {%- set one_column = {'base_location': 'a/', 'columns': [{'name': 'id', 'data_type': 'int'}]} -%}
  {%- set yaml_columns = {'Id': {'description': 'Identifier'}} -%}
  {%- set create = package.parquet_direct__create_statements -%}
  {%- set normalize = package.parquet_direct__normalize -%}

  {%- do assert_equal('persist_docs writes descriptions as comments',
    create(relation, normalize(one_column, 'Model description', yaml_columns, true, true))[0],
    "create iceberg table ANALYTICS.SALES.ORDERS\n(\n    id int comment 'Identifier'\n)\nbase_location = 'a/'\ncomment = 'Model description'") -%}
  {%- do assert_equal('without persist_docs there are no comments',
    create(relation, normalize(one_column, 'Model description', yaml_columns, false, false))[0],
    "create iceberg table ANALYTICS.SALES.ORDERS\n(\n    id int\n)\nbase_location = 'a/'") -%}
  {%- do assert_equal('comment wins over persist_docs',
    create(relation, normalize({'base_location': 'a/', 'comment': 'Explicit'}, 'Model description', {}, true, true))[0],
    "create iceberg table ANALYTICS.SALES.ORDERS\nbase_location = 'a/'\ncomment = 'Explicit'") -%}

  {%- set extra = {'base_location': 'a/', 'auto_refresh': false, 'columns': [
    {'name': 'ID', 'type': 'NUMBER(10,0)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none},
    {'name': 'EXTRA', 'type': 'VARCHAR(134217728)', 'kind': 'COLUMN', 'nullable': true, 'expression': none, 'comment': none},
  ]} -%}
  {%- set cases = [
    ('inference is on by default without columns', {'base_location': 'a/'}, 0),
    ('pattern-inferred columns do not count as drift', {'base_location': 'a/', 'schema_inference_file_pattern_on_create': 'sample[.]parquet', 'refresh_on_create': false}, 0),
    ('inference is off by default with columns', one_column, 1),
    ('schema evolution turns inference on', with_overrides(one_column, {'enable_schema_evolution': true}), 0),
    ('explicit inference keeps inferred columns', with_overrides(one_column, {'enable_schema_inference': true}), 0),
  ] -%}
  {%- for name, declaration, expected in cases -%}
    {%- set result = package.parquet_direct__diff(relation, normalize(declaration), extra, none_catalog()) -%}
    {%- do assert_equal(name, result.changes | length, expected) -%}
  {%- endfor -%}
{% endmacro %}

{% macro test_ddl() %}
  {%- set normalize = dbt_snowflake_parquet_direct.parquet_direct__normalize -%}
  {%- set create = dbt_snowflake_parquet_direct.parquet_direct__create_statements -%}
  {%- set relation = test_relation() -%}

  {%- set expected -%}
create iceberg table ANALYTICS.SALES.ORDERS
(
    id number(19,0) not null,
    amount number(12,2) with masking policy governance.policies.mask_amount comment 'it\'s money',
    source_file varchar as (METADATA$FILENAME),
    d date as (SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)::DATE),
    primary key ("ID") rely,
    constraint unique_amount unique ("AMOUNT")
)
partition by (d)
external_volume = 'example_volume'
catalog = 'my_catalog'
base_location = 'sales/'
pattern = '.*[.]parquet'
iceberg_version = 3
enable_schema_inference = false
enable_schema_evolution = false
match_by_column_name = CASE_INSENSITIVE
auto_refresh = false
refresh_on_create = true
replace_invalid_characters = false
with row access policy governance.policies.row_access_policy on (id)
with tag (governance.tags.domain = 'sales')
comment = 'Sales'
  {%- endset -%}
  {%- set statements = create(relation, normalize(full_config())) -%}
  {%- do assert_equal('full create statement', statements[0], expected | trim) -%}
  {%- do assert_equal('log_event_level is set after creation', statements[1:], ['alter iceberg table ANALYTICS.SALES.ORDERS set log_event_level = ERROR']) -%}

  {%- do assert_equal('minimal create statement', create(relation, normalize({'base_location': 'sales/'})), ["create iceberg table ANALYTICS.SALES.ORDERS\nbase_location = 'sales/'"]) -%}

  {%- set inferred = normalize({
    'base_location': 'sales/', 'refresh_on_create': false, 'pattern': 'd=2/.*',
    'schema_inference_file_pattern_on_create': 'd=2/a\\.parquet',
    'partition_columns': [{'name': 'source_path', 'data_type': 'varchar', 'expression': 'METADATA$FILENAME'}],
    'constraints': [{'type': 'unique', 'columns': ['id']}],
  }) -%}
  {%- set ddl = "create or replace ICEBERG TABLE PROBE (\n\tID LONG,\n\tATTRIBUTES MAP(STRING, DECIMAL(19,0))\n)\n BASE_LOCATION = 'sales/';" -%}
  {%- set column_list = dbt_snowflake_parquet_direct.parquet_direct__inferred_column_list -%}
  {%- do assert_equal('preserve physical types from GET_DDL', column_list(ddl), 'ID LONG,\n\tATTRIBUTES MAP(STRING, DECIMAL(19,0))') -%}
  {%- do assert_equal('parenthesis inside a quoted probe name is not the column list opening',
    column_list(ddl.replace('TABLE PROBE', 'TABLE "__tmp_dbt_parquet_direct__Sales (daily)"')), column_list(ddl)) -%}
  {%- do assert_equal('missing column list', column_list("create or replace ICEBERG TABLE PROBE BASE_LOCATION = 'sales/';"), '') -%}
  {%- do assert_equal('missing end of column list', column_list('create or replace ICEBERG TABLE PROBE (ID LONG)'), '') -%}
  {%- do assert_equal('inferred columns go before partition columns and constraints, and the table gets its own pattern',
    create(relation, inferred, inferred_columns_sql=column_list(ddl))[0],
    "create iceberg table ANALYTICS.SALES.ORDERS\n(\n    ID LONG,\n\tATTRIBUTES MAP(STRING, DECIMAL(19,0)),\n    source_path varchar as (METADATA$FILENAME),\n    unique (\"ID\")\n)\npartition by (source_path)\nbase_location = 'sales/'\npattern = 'd=2/.*'\nrefresh_on_create = false") -%}
  {%- do assert_equal('probe name prefixes the table name', dbt_snowflake_parquet_direct.parquet_direct__inference_relation(relation) | string, 'ANALYTICS.SALES.__tmp_dbt_parquet_direct__ORDERS') -%}
  {%- set probe_create = dbt_snowflake_parquet_direct.parquet_direct__inference_create_sql -%}
  {%- set probe_comment = "\ncomment = 'Schema inference probe managed by dbt-snowflake-parquet-direct'" -%}
  {%- do assert_equal('probe uses the inference pattern and inherited volume and catalog',
    probe_create(relation, inferred),
    "create iceberg table ANALYTICS.SALES.ORDERS\nbase_location = 'sales/'\npattern = 'd=2/a\\\\.parquet'" ~ probe_comment) -%}
  {%- do assert_equal('probe uses explicit volume and catalog when supplied',
    probe_create(relation, with_overrides(inferred, {'options': with_overrides(inferred.options, {'external_volume': 'vol', 'catalog': 'cat'})})),
    "create iceberg table ANALYTICS.SALES.ORDERS\nexternal_volume = 'vol'\ncatalog = 'cat'\nbase_location = 'sales/'\npattern = 'd=2/a\\\\.parquet'" ~ probe_comment) -%}

  {%- set replaced = create(relation, normalize({'base_location': 'sales/'}), replace=true, copy_grants=true) -%}
  {%- do assert_equal('replace with copy grants', replaced, ["create or replace iceberg table ANALYTICS.SALES.ORDERS\nbase_location = 'sales/'\ncopy grants"]) -%}

  {%- set deferred = create(relation, normalize({'base_location': 'sales/', 'auto_refresh': true, 'refresh_on_create': false, 'columns': [{'name': 'id', 'data_type': 'int'}]})) -%}
  {%- do assert_equal('auto refresh is switched on after creating without refresh', deferred, [
    "create iceberg table ANALYTICS.SALES.ORDERS\n(\n    id int\n)\nbase_location = 'sales/'\nauto_refresh = false\nrefresh_on_create = false",
    'alter iceberg table ANALYTICS.SALES.ORDERS set auto_refresh = true',
  ]) -%}
  {%- set immediate = create(relation, normalize({'base_location': 'sales/', 'auto_refresh': true})) -%}
  {%- do assert_equal('auto refresh at creation', immediate, ["create iceberg table ANALYTICS.SALES.ORDERS\nbase_location = 'sales/'\nauto_refresh = true"]) -%}

  {%- set awkward = normalize({'base_location': 'sales/', 'columns': [{'name': 'a"b', 'data_type': 'int', 'quote': true, 'constraints': [{'type': 'primary_key'}]}]}) -%}
  {%- do assert_equal('double quotes in quoted names are escaped', create(relation, awkward)[0],
    'create iceberg table ANALYTICS.SALES.ORDERS\n(\n    "a""b" int,\n    primary key ("a""b")\n)\nbase_location = \'sales/\'') -%}

  {%- set detailed = normalize({'base_location': 'sales/', 'columns': [
    {'name': 'id', 'data_type': 'int', 'quote': true, 'tag': {'governance.tags.beta': 'x', 'governance.tags.alpha': 'y'}, 'masking_policy': {'name': 'governance.policies.mask_id', 'using': ['id', 'other']}},
    {'name': 'other', 'data_type': 'int', 'constraints': [{'type': 'foreign_key', 'references': {'table': 'analytics.sales.parent', 'columns': ['id']}}]},
  ]}) -%}
  {%- do assert_equal('column options', create(relation, detailed)[0],
    "create iceberg table ANALYTICS.SALES.ORDERS\n(\n    \"id\" int with masking policy governance.policies.mask_id using (id, other) with tag (governance.tags.alpha = 'y', governance.tags.beta = 'x'),\n    other int,\n    foreign key (\"OTHER\") references analytics.sales.parent (\"ID\")\n)\nbase_location = 'sales/'") -%}
{% endmacro %}

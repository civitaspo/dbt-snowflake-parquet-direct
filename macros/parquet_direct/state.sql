{# Read the catalog integration and the existing table from Snowflake (design section 6). Read-only. #}

{% macro parquet_direct__rows(sql) -%}
  {%- set rows = [] -%}
  {%- set table = run_query(sql) -%}
  {%- for row in table.rows -%}
    {%- set item = {} -%}
    {%- for name in table.column_names -%}{%- do item.update({name | lower: row[name]}) -%}{%- endfor -%}
    {%- do rows.append(item) -%}
  {%- endfor -%}
  {{ return(rows) }}
{%- endmacro %}

{% macro parquet_direct__describe_catalog(name) -%}
  {%- set properties = {} -%}
  {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('desc catalog integration ' ~ name) -%}
    {%- do properties.update({row.property | upper: row.property_value}) -%}
  {%- endfor -%}
  {{ return({'name': name, 'catalog_source': (properties.get('CATALOG_SOURCE') or '') | upper, 'table_format': (properties.get('TABLE_FORMAT') or '') | upper}) }}
{%- endmacro %}

{# The catalog integration the table uses: the configured one, or the default resolved from the schema. #}
{% macro parquet_direct__get_catalog(relation, declaration) -%}
  {%- set name = declaration.options.get('catalog') -%}
  {%- if name is none -%}
    {%- set rows = dbt_snowflake_parquet_direct.parquet_direct__rows("show parameters like 'CATALOG' in schema " ~ relation.include(identifier=false)) -%}
    {%- set name = rows[0].value if rows else none -%}
  {%- endif -%}
  {%- if not name -%}{{ return(none) }}{%- endif -%}
  {{ return(dbt_snowflake_parquet_direct.parquet_direct__describe_catalog(name)) }}
{%- endmacro %}

{# Whether the object exists and is a Parquet Direct table, with the fields SHOW ICEBERG TABLES provides. #}
{% macro parquet_direct__get_state(relation) -%}
  {%- set identifier = dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(relation.identifier) if not relation.quote_policy.identifier else relation.identifier -%}
  {%- set shown = dbt_snowflake_parquet_direct.parquet_direct__rows('show iceberg tables like ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(identifier) ~ ' in schema ' ~ relation.include(identifier=false)) -%}
  {#- LIKE is case-insensitive and treats _ as a wildcard, so keep only the exact name. -#}
  {%- set matches = shown | selectattr('name', 'equalto', identifier) | list -%}
  {%- if not matches -%}
    {%- set existing = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
    {%- if existing is none -%}{{ return({'exists': false}) }}{%- endif -%}
    {{ return({'exists': true, 'is_parquet_direct': false, 'object_type': existing.type}) }}
  {%- endif -%}
  {%- set row = matches[0] -%}
  {#- Snowflake-managed Iceberg tables report the catalog SNOWFLAKE, which is not a catalog integration. -#}
  {%- set catalog = dbt_snowflake_parquet_direct.parquet_direct__describe_catalog(row.catalog_name) if row.catalog_name and row.catalog_name | upper != 'SNOWFLAKE' else none -%}
  {{ return({
    'exists': true,
    'is_parquet_direct': catalog is not none and catalog.catalog_source == 'OBJECT_STORE' and catalog.table_format in ['NONE', 'HIVE'],
    'object_type': 'iceberg table',
    'external_volume': row.external_volume_name,
    'catalog': row.catalog_name,
    'base_location': row.base_location,
    'comment': row.comment,
  }) }}
{%- endmacro %}

{# Everything reconciliation compares. Policies, tags, and constraints are read only when the model declares them. #}
{% macro parquet_direct__get_details(relation, declaration, state) -%}
  {%- set details = {} -%}
  {%- do details.update(state) -%}
  {%- set qualified_name = dbt_snowflake_parquet_direct.parquet_direct__sql_string(relation | string) -%}

  {%- set columns = [] -%}
  {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('desc iceberg table ' ~ relation) -%}
    {%- do columns.append({'name': row.name, 'type': row.type, 'kind': row.kind, 'nullable': row['null?'] != 'N', 'expression': row.expression, 'comment': row.comment}) -%}
  {%- endfor -%}
  {%- do details.update({'columns': columns}) -%}

  {%- set parameters = {} -%}
  {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('show parameters in table ' ~ relation) -%}
    {%- do parameters.update({row.key | upper: row.value}) -%}
  {%- endfor -%}
  {%- do details.update({'parameters': parameters}) -%}

  {%- set status = dbt_snowflake_parquet_direct.parquet_direct__rows('select system$parquet_direct_auto_refresh_status(' ~ qualified_name ~ ') as status')[0].status -%}
  {%- do details.update({'auto_refresh': dbt_snowflake_parquet_direct.parquet_direct__parse_auto_refresh_status(status).enabled}) -%}

  {%- do details.update(dbt_snowflake_parquet_direct.parquet_direct__get_governance(relation, declaration, 'table')) -%}

  {%- if declaration.constraints -%}
    {%- set constraints = {} -%}
    {%- for type, command in [('primary_key', 'primary keys'), ('unique', 'unique keys'), ('foreign_key', 'imported keys')] -%}
      {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('show ' ~ command ~ ' in table ' ~ relation) | sort(attribute='key_sequence') -%}
        {%- set name = row.fk_name if type == 'foreign_key' else row.constraint_name -%}
        {%- if name not in constraints -%}
          {%- set references = none -%}
          {%- if type == 'foreign_key' -%}
            {%- set references = {'table': row.pk_database_name ~ '.' ~ row.pk_schema_name ~ '.' ~ row.pk_table_name, 'columns': []} -%}
          {%- endif -%}
          {%- do constraints.update({name: {'type': type, 'name': name, 'columns': [], 'rely': (row.rely | string | lower) == 'true', 'references': references}}) -%}
        {%- endif -%}
        {%- do constraints[name].columns.append(row.fk_column_name if type == 'foreign_key' else row.column_name) -%}
        {%- if type == 'foreign_key' -%}{%- do constraints[name].references.columns.append(row.pk_column_name) -%}{%- endif -%}
      {%- endfor -%}
    {%- endfor -%}
    {%- do details.update({'constraints': constraints.values() | list}) -%}
  {%- endif -%}

  {{ return(details) }}
{%- endmacro %}

{#
  The masking policies, row access policy, and tags set on a table or view (`domain` 'table' or 'view'), read only when
  the declaration has some.
#}
{% macro parquet_direct__get_governance(relation, declaration, domain) -%}
  {%- set details = {} -%}
  {%- set qualified_name = dbt_snowflake_parquet_direct.parquet_direct__sql_string(relation | string) -%}
  {%- set information_schema_name = relation.include(schema=false, identifier=false) ~ '.information_schema' -%}
  {%- set has_policies = declaration.row_access_policy or declaration.columns | selectattr('masking_policy') | list -%}
  {%- if has_policies -%}
    {%- set masking = {} -%}
    {%- do details.update({'masking_policies': masking, 'row_access_policy': none}) -%}
    {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('select * from table(' ~ information_schema_name ~ ".policy_references(ref_entity_name => " ~ qualified_name ~ ", ref_entity_domain => '" ~ domain ~ "'))") -%}
      {%- set name = row.policy_db ~ '.' ~ row.policy_schema ~ '.' ~ row.policy_name -%}
      {%- if row.policy_kind == 'MASKING_POLICY' and row.tag_name is none -%}
        {%- do masking.update({row.ref_column_name: name}) -%}
      {%- elif row.policy_kind == 'ROW_ACCESS_POLICY' -%}
        {%- do details.update({'row_access_policy': {'name': name, 'on': fromjson(row.ref_arg_column_names, [])}}) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- if declaration.tag or declaration.columns | selectattr('tag') | list -%}
    {%- set table_tags = {} -%}
    {#- Tags inherited from the account, database, or schema are not set on the table. -#}
    {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('select * from table(' ~ information_schema_name ~ '.tag_references(' ~ qualified_name ~ ", 'table'))") if (row.level or '') | upper not in ['ACCOUNT', 'DATABASE', 'SCHEMA'] and not row.column_name -%}
      {%- do table_tags.update({row.tag_database ~ '.' ~ row.tag_schema ~ '.' ~ row.tag_name: row.tag_value}) -%}
    {%- endfor -%}
    {%- set column_tags = {} -%}
    {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('select * from table(' ~ information_schema_name ~ '.tag_references_all_columns(' ~ qualified_name ~ ", 'table'))") if 'COLUMN' in (row.level or '') | upper -%}
      {%- if row.column_name not in column_tags -%}{%- do column_tags.update({row.column_name: {}}) -%}{%- endif -%}
      {%- do column_tags[row.column_name].update({row.tag_database ~ '.' ~ row.tag_schema ~ '.' ~ row.tag_name: row.tag_value}) -%}
    {%- endfor -%}
    {%- do details.update({'table_tags': table_tags, 'column_tags': column_tags}) -%}
  {%- endif -%}
  {{ return(details) }}
{%- endmacro %}

{# A view's stored text, comments, and governance, when the view exists. #}
{% macro parquet_direct__get_view(relation, declaration) -%}
  {%- set identifier = dbt_snowflake_parquet_direct.parquet_direct__stored_identifier(relation) -%}
  {%- set shown = dbt_snowflake_parquet_direct.parquet_direct__rows('show views like ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(identifier) ~ ' in schema ' ~ relation.include(identifier=false)) -%}
  {%- set matches = shown | selectattr('name', 'equalto', identifier) | list -%}
  {%- if not matches -%}{{ return({'exists': false}) }}{%- endif -%}
  {%- set column_comments = {} -%}
  {%- for row in dbt_snowflake_parquet_direct.parquet_direct__rows('desc view ' ~ relation) -%}
    {%- do column_comments.update({row.name: row.comment}) -%}
  {%- endfor -%}
  {%- set view = {'exists': true, 'text': matches[0].text, 'comment': matches[0].comment, 'column_comments': column_comments} -%}
  {%- do view.update(dbt_snowflake_parquet_direct.parquet_direct__get_governance(relation, declaration, 'view')) -%}
  {{ return(view) }}
{%- endmacro %}

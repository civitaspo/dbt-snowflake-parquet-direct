{#
  Previous versions (design section 8). With previous_versions, the model's relation is the combined view, the
  current table is <model>__current, and each previous version is a table in the model's schema.
  Everything here is pure except parquet_direct__get_previous_versions, which reads Snowflake.
#}

{% macro parquet_direct__current_relation(relation) -%}
  {{ return(relation.incorporate(path={'identifier': relation.identifier ~ '__current'}, type='table')) }}
{%- endmacro %}

{# `identifier` is stored as written, so the relation quotes it. #}
{% macro parquet_direct__previous_relation(relation, identifier) -%}
  {{ return(relation.incorporate(path={'identifier': identifier}, quote_policy={'identifier': true}, type='table')) }}
{%- endmacro %}

{# The identifier of a relation as Snowflake stores it. #}
{% macro parquet_direct__stored_identifier(relation) -%}
  {{ return(relation.identifier if relation.quote_policy.identifier else dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(relation.identifier)) }}
{%- endmacro %}

{#
  The declaration the current table is built from. Masking policies, row access policies, tags, and comments belong
  to the combined view, whose columns have the declared types, so the current table leaves them as they are.
#}
{% macro parquet_direct__table_declaration(declaration) -%}
  {%- if not declaration.previous_versions -%}{{ return(declaration) }}{%- endif -%}
  {%- set columns = [] -%}
  {%- for column in declaration.columns -%}
    {%- set copy = {} -%}
    {%- do copy.update(column) -%}
    {%- do copy.update({'masking_policy': none, 'tag': {}, 'comment': none}) -%}
    {%- do columns.append(copy) -%}
  {%- endfor -%}
  {%- set table = {} -%}
  {%- do table.update(declaration) -%}
  {%- do table.update({
    'columns': columns,
    'partition_columns': columns | selectattr('kind', 'equalto', 'PARTITION') | list,
    'comment': none,
    'tag': {},
    'row_access_policy': none,
  }) -%}
  {{ return(table) }}
{%- endmacro %}

{# Each previous version with its relation, its state, and its columns ({NAME: type}). Reads Snowflake. #}
{% macro parquet_direct__get_previous_versions(relation, declaration) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set previous = [] -%}
  {%- for version in declaration.previous_versions -%}
    {%- set previous_relation = package.parquet_direct__previous_relation(relation, version.identifier) -%}
    {%- set state = package.parquet_direct__get_state(previous_relation) -%}
    {%- set columns = {} -%}
    {%- if state.exists -%}
      {%- set describe = 'desc iceberg table ' if state.object_type == 'iceberg table' else 'desc view ' if state.object_type == 'view' else 'desc table ' -%}
      {%- for row in package.parquet_direct__rows(describe ~ previous_relation) -%}
        {%- do columns.update({row.name: row.type}) -%}
      {%- endfor -%}
    {%- endif -%}
    {%- do previous.append({'version': version, 'relation': previous_relation, 'state': state, 'columns': columns}) -%}
  {%- endfor -%}
  {{ return(previous) }}
{%- endmacro %}

{#
  Pre-flight checks for previous versions. `public_state` is the model's relation, `current_state` the current table,
  and `creating` whether this run creates the current table. Returns a list of "Rxx: ..." messages.
#}
{% macro parquet_direct__versions_preflight(relation, declaration, public_state, current_state, previous, creating) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set errors = [] -%}
  {%- set current_relation = package.parquet_direct__current_relation(relation) -%}
  {%- set reserved = [package.parquet_direct__stored_identifier(relation), package.parquet_direct__stored_identifier(current_relation)] -%}
  {%- set public_is_table = public_state.exists and public_state.object_type != 'view' -%}
  {%- set missing = [] -%}
  {%- for item in previous -%}
    {%- if item.version.identifier in reserved -%}
      {%- do errors.append("R05: previous version table " ~ item.version.table ~ " cannot be " ~ relation ~ " or its current table " ~ current_relation) -%}
    {%- elif not item.state.exists -%}
      {%- do missing.append(item) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if missing -%}
    {#- The table that holds the files of the newest missing version is the public table or the current table. -#}
    {%- set source = relation if public_is_table else current_relation if current_state.exists else none -%}
    {%- for item in missing -%}
      {%- set hint = '; rename the table that holds its files first: alter iceberg table ' ~ source ~ ' rename to ' ~ item.relation if source and loop.last else '' -%}
      {%- do errors.append("R06: previous version table " ~ item.relation ~ " does not exist" ~ hint) -%}
    {%- endfor -%}
  {%- elif public_is_table -%}
    {%- do errors.append("R07: " ~ relation ~ " is a " ~ public_state.object_type ~ ", not the combined view; rename it to a table listed in previous_versions, or drop it") -%}
  {%- endif -%}
  {%- if creating and declaration.refresh_on_create -%}
    {%- set location = declaration.base_location.rstrip('/') ~ '/' -%}
    {%- for item in previous if item.state.exists and item.state.base_location -%}
      {%- set other = item.state.base_location.rstrip('/') ~ '/' -%}
      {%- if location.startswith(other) or other.startswith(location) -%}
        {%- do errors.append("R08: previous version " ~ item.relation ~ " reads base_location '" ~ item.state.base_location ~ "', which overlaps '" ~ declaration.base_location ~ "'; set refresh_on_create: false so the current table registers only refresh_paths") -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(errors) }}
{%- endmacro %}

{#
  The SELECT that maps one previous version onto the declared columns: a declared expression, else the same-named
  column if its type is the declared type or a widening of it, else NULL for a column the version lacks.
  Returns {'sql': ..., 'errors': ["R09: ..."]}.
#}
{% macro parquet_direct__previous_version_select(declaration, item, position) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set items = [] -%}
  {%- set errors = [] -%}
  {%- for column in declaration.columns -%}
    {%- set target = package.parquet_direct__normalize_type(column.data_type) -%}
    {%- set expression = item.version.columns.get(column.identifier) -%}
    {%- set actual = item.columns.get(column.identifier) -%}
    {%- if expression -%}
      {%- set source = '(' ~ expression ~ ')' -%}
    {%- elif actual is none -%}
      {%- set source = 'null' -%}
    {%- else -%}
      {%- set actual_type = package.parquet_direct__normalize_type(actual) -%}
      {%- if actual_type != target and not package.parquet_direct__is_widening(actual_type, target) -%}
        {%- do errors.append("R09: column " ~ column.identifier ~ " is " ~ actual_type ~ " in " ~ item.relation ~ " but " ~ target ~ " in the model; set previous_versions[" ~ position ~ "].columns." ~ column.name ~ " to an expression that converts it") -%}
      {%- endif -%}
      {%- set source = package.parquet_direct__quote_identifier(column.identifier) -%}
    {%- endif -%}
    {%- do items.append('cast(' ~ source ~ ' as ' ~ target ~ ') as ' ~ package.parquet_direct__quote_identifier(column.identifier)) -%}
  {%- endfor -%}
  {{ return({'sql': 'select\n    ' ~ items | join(',\n    ') ~ '\nfrom ' ~ item.relation, 'errors': errors}) }}
{%- endmacro %}

{# CREATE OR REPLACE VIEW for the combined view. Policies and tags are part of the statement, so a replacement never leaves the view without them. #}
{% macro parquet_direct__view_statement(relation, declaration, previous_selects) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set columns = [] -%}
  {%- set identifiers = [] -%}
  {%- for column in declaration.columns -%}
    {%- set sql = column.sql -%}
    {%- if column.masking_policy -%}
      {%- set sql = sql ~ ' with masking policy ' ~ column.masking_policy.name -%}
      {%- if column.masking_policy.using -%}{%- set sql = sql ~ ' using (' ~ column.masking_policy.using | join(', ') ~ ')' -%}{%- endif -%}
    {%- endif -%}
    {%- if column.tag -%}{%- set sql = sql ~ ' with tag (' ~ package.parquet_direct__tag_list(column.tag) ~ ')' -%}{%- endif -%}
    {%- if column.comment is not none -%}{%- set sql = sql ~ ' comment ' ~ package.parquet_direct__sql_string(column.comment) -%}{%- endif -%}
    {%- do columns.append(sql) -%}
    {%- do identifiers.append(package.parquet_direct__quote_identifier(column.identifier)) -%}
  {%- endfor -%}
  {%- set lines = ['create or replace view ' ~ relation ~ ' copy grants', '(\n    ' ~ columns | join(',\n    ') ~ '\n)'] -%}
  {%- if declaration.row_access_policy -%}
    {%- do lines.append('with row access policy ' ~ declaration.row_access_policy.name ~ ' on (' ~ declaration.row_access_policy.on | join(', ') ~ ')') -%}
  {%- endif -%}
  {%- if declaration.tag -%}
    {%- do lines.append('with tag (' ~ package.parquet_direct__tag_list(declaration.tag) ~ ')') -%}
  {%- endif -%}
  {%- if declaration.comment is not none -%}
    {%- do lines.append('comment = ' ~ package.parquet_direct__sql_string(declaration.comment)) -%}
  {%- endif -%}
  {%- set current = 'select ' ~ identifiers | join(', ') ~ '\nfrom ' ~ package.parquet_direct__current_relation(relation) -%}
  {%- do lines.append('as\n' ~ (previous_selects + [current]) | join('\nunion all by name\n')) -%}
  {{ return(lines | join('\n')) }}
{%- endmacro %}

{#
  The relation the registration check reads for a model: the current table when the model has previous versions.
  `nodes` are the project's graph nodes; the relation is returned unchanged unless exactly one model matches.
#}
{% macro parquet_direct__registration_relation(relation, nodes) -%}
  {%- set matches = [] -%}
  {%- for node in nodes if node.resource_type == 'model' and node.relation_name == relation | string -%}
    {%- do matches.append(node) -%}
  {%- endfor -%}
  {%- if matches | length == 1 and ((matches[0].config.meta or {}).get('parquet_direct') or {}).get('previous_versions') -%}
    {{ return(dbt_snowflake_parquet_direct.parquet_direct__current_relation(relation)) }}
  {%- endif -%}
  {{ return(relation) }}
{%- endmacro %}

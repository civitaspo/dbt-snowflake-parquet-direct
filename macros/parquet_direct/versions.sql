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
  {%- if public_state.exists and not public_is_table and (current_relation | string | upper) not in (public_state.text or '') | upper -%}
    {%- do errors.append("R10: view " ~ relation ~ " does not read " ~ current_relation ~ ", so previous_versions did not create it; drop or rename it") -%}
  {%- endif -%}
  {#- A refresh of every file would read the previous versions' files, whose types the current table cannot register. -#}
  {%- set reasons = [] -%}
  {%- if creating and declaration.refresh_on_create -%}
    {%- do reasons.append('set refresh_on_create: false so creating the current table registers no files') -%}
  {%- endif -%}
  {%- if not declaration.refresh_paths and not declaration.skip_refresh_if_refresh_paths_empty -%}
    {%- do reasons.append('set refresh_paths, or skip_refresh_if_refresh_paths_empty: true, so the refresh never reads every file') -%}
  {%- endif -%}
  {%- set location = declaration.base_location.rstrip('/') ~ '/' -%}
  {%- for item in previous if reasons and item.state.exists and item.state.base_location -%}
    {%- set other = item.state.base_location.rstrip('/') ~ '/' -%}
    {%- if location.startswith(other) or other.startswith(location) -%}
      {%- do errors.append("R08: previous version " ~ item.relation ~ " reads base_location '" ~ item.state.base_location ~ "', which overlaps '" ~ declaration.base_location ~ "'; " ~ reasons | join(', and ')) -%}
    {%- endif -%}
  {%- endfor -%}
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

{#
  The statements that create or change the combined view. `view` is what parquet_direct__get_view read.
  - A new view is created with its policies and tags in the statement, so it is never readable without them.
  - An existing view is changed in place: CREATE OR ALTER VIEW when its query or a declared comment differs, and
    ALTER VIEW for the declared policies and tags. Grants and undeclared policies and tags stay; Snowflake does not
    let CREATE OR ALTER set policies or tags.
  Returns {'create': [...], 'changes': [...], 'reapply': [...]}, where changes are shaped like parquet_direct__diff's.
#}
{% macro parquet_direct__view_statements(relation, declaration, previous_selects, view) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set governed = [] -%}
  {%- set commented = [] -%}
  {%- set identifiers = [] -%}
  {%- for column in declaration.columns -%}
    {%- set comment = ' comment ' ~ package.parquet_direct__sql_string(column.comment) if column.comment is not none else '' -%}
    {%- set sql = column.sql -%}
    {%- if column.masking_policy -%}
      {%- set sql = sql ~ ' with masking policy ' ~ column.masking_policy.name -%}
      {%- if column.masking_policy.using -%}{%- set sql = sql ~ ' using (' ~ column.masking_policy.using | join(', ') ~ ')' -%}{%- endif -%}
    {%- endif -%}
    {%- if column.tag -%}{%- set sql = sql ~ ' with tag (' ~ package.parquet_direct__tag_list(column.tag) ~ ')' -%}{%- endif -%}
    {%- do governed.append(sql ~ comment) -%}
    {%- do commented.append(column.sql ~ comment) -%}
    {%- do identifiers.append(package.parquet_direct__quote_identifier(column.identifier)) -%}
  {%- endfor -%}
  {%- set table_options = [] -%}
  {%- if declaration.row_access_policy -%}
    {%- do table_options.append('with row access policy ' ~ declaration.row_access_policy.name ~ ' on (' ~ declaration.row_access_policy.on | join(', ') ~ ')') -%}
  {%- endif -%}
  {%- if declaration.tag -%}
    {%- do table_options.append('with tag (' ~ package.parquet_direct__tag_list(declaration.tag) ~ ')') -%}
  {%- endif -%}
  {%- set comment = ['comment = ' ~ package.parquet_direct__sql_string(declaration.comment)] if declaration.comment is not none else [] -%}
  {%- set current = 'select ' ~ identifiers | join(', ') ~ '\nfrom ' ~ package.parquet_direct__current_relation(relation) -%}
  {%- set query = 'as\n' ~ (previous_selects + [current]) | join('\nunion all by name\n') -%}
  {%- set definition = (['create or alter view ' ~ relation, '(\n    ' ~ commented | join(',\n    ') ~ '\n)'] + comment + [query]) | join('\n') -%}
  {%- if not view.exists -%}
    {%- set create = (['create view ' ~ relation, '(\n    ' ~ governed | join(',\n    ') ~ '\n)'] + table_options + comment + [query]) | join('\n') -%}
    {{ return({'create': [create], 'changes': [], 'reapply': []}) }}
  {%- endif -%}
  {#-
    Snowflake stores the statement that last changed the view, as written, apart from a leading comment. A CREATE OR
    ALTER that changes nothing keeps the earlier text, so only the query at the end of the text is compared.
  -#}
  {%- set stored = modules.re.sub('(\\s*/\\*((?!\\*/).)*\\*/)+\\s*$', '', view.text or '', flags=modules.re.DOTALL) -%}
  {%- set reasons = [] -%}
  {%- if not (' ' ~ stored.split() | join(' ')).endswith(' ' ~ query.split() | join(' ')) -%}
    {%- do reasons.append('query') -%}
  {%- endif -%}
  {%- if declaration.comment is not none and declaration.comment != (view.comment or '') -%}
    {%- do reasons.append('comment') -%}
  {%- endif -%}
  {%- for column in declaration.columns if column.comment is not none and column.comment != ((view.column_comments or {}).get(column.identifier) or '') -%}
    {%- do reasons.append(column.identifier ~ ' comment') -%}
  {%- endfor -%}
  {%- set changes = [] -%}
  {%- if reasons -%}
    {%- do changes.append({'key': 'view', 'description': 'view definition changed: ' ~ reasons | join(', '), 'alterable': true, 'statements': [definition]}) -%}
  {%- endif -%}
  {%- set governance = package.parquet_direct__governance_changes('alter view ' ~ relation, declaration, view) -%}
  {%- for change in governance.changes -%}{%- do change.update({'description': 'view ' ~ change.description}) -%}{%- endfor -%}
  {{ return({'create': [], 'changes': changes + governance.changes, 'reapply': governance.reapply}) }}
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

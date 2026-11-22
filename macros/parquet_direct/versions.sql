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

{# The comment that marks a table as a model's current table, so a table that another model or process made is never taken over. #}
{% macro parquet_direct__current_table_comment() -%}
  {{ return('Current table of a combined view built from previous_versions by dbt-snowflake-parquet-direct') }}
{%- endmacro %}

{#
  The comment after the view's name in every statement that creates the combined view, with the digest of the
  declaration it applies. Snowflake keeps it in the view's stored text.
#}
{% macro parquet_direct__view_marker(digest) -%}
  {{ return('/* combined view of previous_versions, managed by dbt-snowflake-parquet-direct, declaration ' ~ digest ~ ' */') }}
{%- endmacro %}

{# The declaration digest in a view's stored text, or none when the marker does not follow the view's name. #}
{% macro parquet_direct__view_digest(text) -%}
  {%- set name = '(?:"(?:[^"]|"")*"|[^\\s"./(]+)(?:\\.(?:"(?:[^"]|"")*"|[^\\s"./(]+))*' -%}
  {%- set match = modules.re.match('^\\s*create\\s+(?:or\\s+replace\\s+)?(?:secure\\s+)?view\\s+' ~ name ~ '\\s*/\\* combined view of previous_versions, managed by dbt-snowflake-parquet-direct, declaration ([0-9a-f]+) \\*/', text or '', modules.re.I) -%}
  {{ return(match.group(1) if match else none) }}
{%- endmacro %}

{# The declared column name, upper-cased, that a previous_versions columns key names: the name as written, or in double quotes. #}
{% macro parquet_direct__column_key(name) -%}
  {{ return(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(name) | upper) }}
{%- endmacro %}

{# The identifier of a relation as Snowflake stores it. #}
{% macro parquet_direct__stored_identifier(relation) -%}
  {{ return(relation.identifier if relation.quote_policy.identifier else dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(relation.identifier)) }}
{%- endmacro %}

{#
  The declaration the current table is built from. Masking policies, row access policies, tags, and comments belong
  to the combined view, whose columns have the declared types, so the current table leaves them as they are. Its
  comment marks it as the model's current table.
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
    'comment': dbt_snowflake_parquet_direct.parquet_direct__current_table_comment(),
    'tag': {},
    'row_access_policy': none,
  }) -%}
  {{ return(table) }}
{%- endmacro %}

{# One previous version read from `relation`: its state and its columns ({NAME: type}). Reads Snowflake. #}
{% macro parquet_direct__read_previous_version(version, relation) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set state = package.parquet_direct__get_state(relation) -%}
  {%- set columns = {} -%}
  {%- if state.exists -%}
    {%- set describe = 'desc iceberg table ' if state.object_type == 'iceberg table' else 'desc view ' if state.object_type == 'view' else 'desc table ' -%}
    {%- for row in package.parquet_direct__rows(describe ~ relation) -%}
      {%- do columns.update({row.name: row.type}) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return({'version': version, 'relation': relation, 'state': state, 'columns': columns}) }}
{%- endmacro %}

{# Each previous version with its relation, its state, and its columns. Reads Snowflake. #}
{% macro parquet_direct__get_previous_versions(relation, declaration) -%}
  {%- set previous = [] -%}
  {%- for version in declaration.previous_versions -%}
    {%- set previous_relation = dbt_snowflake_parquet_direct.parquet_direct__previous_relation(relation, version.identifier) -%}
    {%- do previous.append(dbt_snowflake_parquet_direct.parquet_direct__read_previous_version(version, previous_relation)) -%}
  {%- endfor -%}
  {{ return(previous) }}
{%- endmacro %}

{#
  The table to rename to the newest previous version, the last in the list, when that version is missing: the model's
  Parquet Direct table on the first change, else this model's current table. An older version that is missing is a
  wrong name or a dropped table, and a table that marks itself as another model's current table is never selected.
  Returns the relation, or none.
#}
{% macro parquet_direct__rename_source(relation, public_state, current_state, previous) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set current_relation = package.parquet_direct__current_relation(relation) -%}
  {%- set marker = package.parquet_direct__current_table_comment() -%}
  {%- set reserved = [package.parquet_direct__stored_identifier(relation), package.parquet_direct__stored_identifier(current_relation)] -%}
  {%- if not previous or (previous | last).state.exists or (previous | last).version.identifier in reserved -%}{{ return(none) }}{%- endif -%}
  {%- if public_state.exists and public_state.object_type != 'view' and public_state.is_parquet_direct and public_state.comment != marker -%}
    {{ return(relation.incorporate(type='table')) }}
  {%- elif current_state.exists and current_state.comment == marker -%}
    {{ return(current_relation) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}

{#
  Pre-flight checks for previous versions. `public_state` is the model's relation, `current_state` the current table,
  and `creating` whether this run creates the current table. Returns errors and the effective refresh_on_create.
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
      {%- do errors.append("R06: previous version table " ~ item.relation ~ " does not exist") -%}
    {%- elif not item.state.is_parquet_direct -%}
      {%- do errors.append("R14: previous version " ~ item.relation ~ " is a " ~ item.state.object_type ~ ", not a Parquet Direct table") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if public_is_table and not missing -%}
    {%- do errors.append("R07: " ~ relation ~ " is a " ~ public_state.object_type ~ ", not the combined view; rename it to a table listed in previous_versions, or drop it") -%}
  {%- endif -%}
  {%- if public_state.exists and not public_is_table and package.parquet_direct__view_digest(public_state.text) is none -%}
    {%- do errors.append("R10: view " ~ relation ~ " lacks the comment that previous_versions writes after its name, so previous_versions did not create it; drop or rename it") -%}
  {%- endif -%}
  {%- set marker = package.parquet_direct__current_table_comment() -%}
  {%- if current_state.exists and current_state.comment != marker -%}
    {%- do errors.append("R11: " ~ current_relation ~ " exists without the comment '" ~ marker ~ "', so previous_versions did not create it; rename it if another model or process uses it, or set that comment if it holds this model's files") -%}
  {%- endif -%}
  {%- set declared_volume = package.parquet_direct__normalize_identifier(declaration.options.external_volume) if declaration.options.get('external_volume') is not none else none -%}
  {#- An existing current table keeps its volume even under on_configuration_change: continue, and its refresh would read the old volume. -#}
  {%- if current_state.exists and not creating and declared_volume and current_state.external_volume and current_state.external_volume != declared_volume -%}
    {%- do errors.append("R12: " ~ current_relation ~ " uses external volume " ~ current_state.external_volume ~ ", but the model declares " ~ declared_volume ~ "; declare " ~ current_state.external_volume ~ ", or run with --full-refresh to re-create it on " ~ declared_volume) -%}
  {%- endif -%}
  {#-
    An overlapping location requires scoped refreshes. Base locations are relative to an external volume, so locations
    on different volumes never overlap here. An omitted external_volume is inherited and unknown, so it may be any volume.
    An existing current table keeps its base location until it is re-created, as with on_configuration_change: continue.
  -#}
  {%- set locations = [(declared_volume, declaration.base_location, '')] -%}
  {%- set overlaps = [] -%}
  {%- if current_state.exists and not creating and current_state.base_location and current_state.base_location.rstrip('/') != declaration.base_location.rstrip('/') -%}
    {%- do locations.append((current_state.external_volume, current_state.base_location, ' of the existing ' ~ current_relation)) -%}
  {%- endif -%}
  {%- for item in previous if item.state.exists and item.state.base_location -%}
    {%- set other = item.state.base_location.rstrip('/') ~ '/' -%}
    {%- for volume, location, owner in locations -%}
      {%- set path = location.rstrip('/') ~ '/' -%}
      {%- set same_volume = not volume or not item.state.external_volume or volume == item.state.external_volume -%}
      {%- if same_volume and (path.startswith(other) or other.startswith(path)) -%}
        {%- do overlaps.append("R08: previous version " ~ item.relation ~ " reads base_location '" ~ item.state.base_location ~ "', which overlaps '" ~ location ~ "'" ~ owner ~ "; set refresh_paths to the partitions written with the new types, because a refresh of the whole location reads the previous version's files") -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endfor -%}
  {%- if not declaration.refresh_paths -%}{%- do errors.extend(overlaps) -%}{%- endif -%}
  {{ return({'errors': errors, 'refresh_on_create': declaration.refresh_on_create and not (creating and overlaps)}) }}
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
    {%- do items.append('cast(' ~ source ~ ' as ' ~ package.parquet_direct__normalize_type(column.data_type, keep_field_names=true) ~ ') as ' ~ package.parquet_direct__quote_identifier(column.identifier)) -%}
  {%- endfor -%}
  {{ return({'sql': 'select\n    ' ~ items | join(',\n    ') ~ '\nfrom ' ~ item.relation, 'errors': errors}) }}
{%- endmacro %}

{#
  The statement that creates or re-creates the combined view, with its policies and tags in the statement, so it is
  never readable without them. `view` is what parquet_direct__get_view read. The view's marker holds the digest of
  the declaration, so an existing view is compared with what the package wrote, not read back:
  - a view that does not exist is created;
  - a view with the same digest is left as it is;
  - any other view is re-created with CREATE OR REPLACE VIEW ... COPY GRANTS, which keeps grants and drops policies
    and tags the model does not declare. A secure view stays secure.
  Returns {'statements': [...], 'differs': whether an existing view is re-created}.
#}
{% macro parquet_direct__view_statements(relation, declaration, previous_selects, view) -%}
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
  {%- set options = ['(\n    ' ~ columns | join(',\n    ') ~ '\n)'] -%}
  {%- if declaration.row_access_policy -%}
    {%- do options.append('with row access policy ' ~ declaration.row_access_policy.name ~ ' on (' ~ declaration.row_access_policy.on | join(', ') ~ ')') -%}
  {%- endif -%}
  {%- if declaration.tag -%}
    {%- do options.append('with tag (' ~ package.parquet_direct__tag_list(declaration.tag) ~ ')') -%}
  {%- endif -%}
  {%- if declaration.comment is not none -%}
    {%- do options.append('comment = ' ~ package.parquet_direct__sql_string(declaration.comment)) -%}
  {%- endif -%}
  {%- set current = 'select ' ~ identifiers | join(', ') ~ '\nfrom ' ~ package.parquet_direct__current_relation(relation) -%}
  {%- set query = 'as\n' ~ (previous_selects + [current]) | join('\nunion all by name\n') -%}
  {%- set digest = local_md5((options + [query]) | join('\n')) -%}
  {%- set marker = package.parquet_direct__view_marker(digest) -%}
  {%- if not view.exists -%}
    {{ return({'statements': [(['create view ' ~ relation, marker] + options + [query]) | join('\n')], 'differs': false}) }}
  {%- endif -%}
  {%- if package.parquet_direct__view_digest(view.text) == digest -%}
    {{ return({'statements': [], 'differs': false}) }}
  {%- endif -%}
  {%- set secure = 'secure ' if view.secure else '' -%}
  {{ return({'statements': [(['create or replace ' ~ secure ~ 'view ' ~ relation, marker] + options + ['copy grants', query]) | join('\n')], 'differs': true}) }}
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

{#
  Compare the declaration with the existing table (design sections 3, 4, and 6).
  `state` comes from dbt_snowflake_parquet_direct.parquet_direct__get_state and dbt_snowflake_parquet_direct.parquet_direct__get_details; `catalog` from dbt_snowflake_parquet_direct.parquet_direct__get_catalog.
  Returns {'changes': [{key, description, alterable, statements}], 'notes': [string], 'reapply': [statement]}. Issues no SQL.
  `reapply` holds statements that must run on every apply because Snowflake cannot report the state they set.
  Create-time options are never compared, and omitted options are left as they are.
#}

{# A scalar type token as DESC ICEBERG TABLE prints it, so declared aliases compare equal. #}
{% macro parquet_direct__normalize_scalar_type(token) -%}
  {%- set text = modules.re.sub('\\s+', '', token) -%}
  {%- set match = modules.re.match('^([A-Z_][A-Z0-9_]*)(?:\\((.*)\\))?$', text) -%}
  {%- if not match -%}{{ return(text) }}{%- endif -%}
  {%- set name, parameters = match.group(1), match.group(2) -%}
  {%- if name in ['INT', 'INTEGER'] -%}{{ return('NUMBER(10,0)') }}
  {#- shortcut: BIGINT is assumed to map to Iceberg long like LONG; confirm in the integration tests. -#}
  {%- elif name in ['LONG', 'BIGINT'] -%}{{ return('NUMBER(19,0)') }}
  {%- elif name in ['NUMBER', 'DECIMAL', 'NUMERIC'] -%}
    {%- if not parameters -%}{{ return('NUMBER(38,0)') }}{%- endif -%}
    {{ return('NUMBER(' ~ parameters ~ ',0)' if ',' not in parameters else 'NUMBER(' ~ parameters ~ ')') }}
  {%- elif name in ['STRING', 'TEXT', 'VARCHAR'] -%}{{ return('VARCHAR(134217728)') }}
  {%- elif name in ['FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'REAL'] -%}{{ return('FLOAT') }}
  {%- elif name in ['TIMESTAMP', 'TIMESTAMP_NTZ', 'DATETIME'] -%}{{ return('TIMESTAMP_NTZ(' ~ (parameters or '6') ~ ')') }}
  {%- elif name in ['TIMESTAMP_LTZ', 'TIME'] -%}{{ return(name ~ '(' ~ (parameters or '6') ~ ')') }}
  {%- elif name in ['BINARY', 'VARBINARY'] and not parameters -%}{{ return('BINARY(8388608)') }}
  {%- endif -%}
  {{ return(text) }}
{%- endmacro %}

{#
  A type as DESC ICEBERG TABLE prints it. Nested types such as OBJECT(n INT) are normalized element by element,
  and field names are folded like type names on both sides, so the two forms still compare equal.
#}
{% macro parquet_direct__normalize_type(data_type) -%}
  {%- set text = modules.re.sub('DOUBLE\\s+PRECISION', 'DOUBLE', data_type | upper) -%}
  {%- set out = {'text': '', 'word': false} -%}
  {%- for token in modules.re.findall('[A-Z_][A-Z0-9_]*(?:\\s*\\([0-9,\\s]*\\))?|[(),]|[^\\s(),]+', text) -%}
    {%- if token in ['(', ')', ','] -%}
      {%- do out.update({'text': out.text ~ token, 'word': false}) -%}
    {%- else -%}
      {%- do out.update({'text': out.text ~ (' ' if out.word else '') ~ dbt_snowflake_parquet_direct.parquet_direct__normalize_scalar_type(token), 'word': true}) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(out.text) }}
{%- endmacro %}

{#
  Whether schema evolution can turn the normalized type `declared` into `actual`: every element matches except
  NUMBER(p1,s) becoming NUMBER(p2,s) with p2 > p1, at any nesting depth.
#}
{% macro parquet_direct__is_widening(declared, actual) -%}
  {%- set re = modules.re -%}
  {%- set pattern = 'NUMBER\\(\\d+,\\d+\\)|[(),]|[^\\s(),]+' -%}
  {%- set before = re.findall(pattern, declared) -%}
  {%- set after = re.findall(pattern, actual) -%}
  {%- if before | length != after | length -%}{{ return(false) }}{%- endif -%}
  {%- set result = {'widened': false, 'valid': true} -%}
  {%- for old in before -%}
    {%- set new = after[loop.index0] -%}
    {%- if old != new -%}
      {%- set old_number = re.match('^NUMBER\\((\\d+),(\\d+)\\)$', old) -%}
      {%- set new_number = re.match('^NUMBER\\((\\d+),(\\d+)\\)$', new) -%}
      {%- if old_number and new_number and old_number.group(2) == new_number.group(2) and old_number.group(1) | int < new_number.group(1) | int -%}
        {%- do result.update({'widened': true}) -%}
      {%- else -%}
        {%- do result.update({'valid': false}) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(result.valid and result.widened) }}
{%- endmacro %}

{# Whether the parenthesis at `start` in `text` closes at its last character, so it wraps everything after `start`. #}
{% macro parquet_direct__closes_at_end(text, start) -%}
  {%- set depth = {'value': 0, 'closed_early': false} -%}
  {%- for character in text[start:-1] -%}
    {%- do depth.update({'value': depth.value + (1 if character == '(' else -1 if character == ')' else 0)}) -%}
    {%- if depth.value == 0 -%}{%- do depth.update({'closed_early': true}) -%}{%- endif -%}
  {%- endfor -%}
  {{ return(text.endswith(')') and not depth.closed_early) }}
{%- endmacro %}

{#
  An expression in a comparable form, matching how Snowflake stores it (observed on 2026-11-15):
  - case and whitespace are folded outside string literals and quoted identifiers, which are compared verbatim;
  - wrapping parentheses are removed;
  - a trailing `x::TYPE` is read as CAST(x AS TYPE), the form Snowflake stores;
  - an outermost cast to `column_type` is removed, because Snowflake drops a cast to the type the expression already
    returns, and a cast to the column's own type does not change the column's value either way.
  shortcut: casts elsewhere in the expression are not rewritten; extend this when the integration tests show another form.
#}
{% macro parquet_direct__normalize_expression(expression, column_type=none) -%}
  {%- set re = modules.re -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set target_type = package.parquet_direct__normalize_type(column_type) if column_type else none -%}
  {%- set literals = [] -%}
  {%- set masked = {'value': ''} -%}
  {%- for part in re.split("('(?:[^'\\\\]|\\\\.|'')*'|\"(?:[^\"]|\"\")*\")", expression or '') -%}
    {%- if loop.index0 is odd -%}
      {%- do masked.update({'value': masked.value ~ '\x01' ~ literals | length ~ '\x01'}) -%}
      {%- do literals.append(part) -%}
    {%- else -%}
      {%- do masked.update({'value': masked.value ~ re.sub('\\s+', ' ', part | upper)}) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set text = {'value': masked.value | trim} -%}
  {%- for _ in range(8) -%}
    {%- set value = text.value -%}
    {%- set trailing_cast = re.match('^(.*?)\\s*::\\s*([A-Z_][A-Z0-9_]*(?:\\s*\\([0-9, ]*\\))?)$', value) -%}
    {%- set whole_cast = re.match('^CAST\\s*\\((.*)\\s+AS\\s+([A-Z_][A-Z0-9_]*(?:\\s*\\([0-9, ]*\\))?)\\s*\\)$', value) -%}
    {%- if trailing_cast -%}
      {%- do text.update({'value': 'CAST(' ~ trailing_cast.group(1) ~ ' AS ' ~ re.sub('\\s+', '', trailing_cast.group(2)) ~ ')'}) -%}
    {%- elif whole_cast and target_type and package.parquet_direct__closes_at_end(value, value.find('(')) and package.parquet_direct__normalize_type(whole_cast.group(2)) == target_type -%}
      {%- do text.update({'value': whole_cast.group(1) | trim}) -%}
    {%- elif value.startswith('(') and package.parquet_direct__closes_at_end(value, 0) -%}
      {%- do text.update({'value': value[1:-1] | trim}) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set result = {'value': re.sub('\\(\\s+', '(', re.sub('\\s+\\)', ')', text.value))} -%}
  {%- for literal in literals -%}
    {%- do result.update({'value': result.value.replace('\x01' ~ loop.index0 ~ '\x01', literal)}) -%}
  {%- endfor -%}
  {{ return(result.value) }}
{%- endmacro %}

{% macro parquet_direct__constraint_key(constraint) -%}
  {%- set references = '' -%}
  {%- if constraint.references -%}
    {%- set columns = [] -%}
    {%- for name in constraint.references.columns -%}{%- do columns.append(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(name)) -%}{%- endfor -%}
    {%- set references = dbt_snowflake_parquet_direct.parquet_direct__qualify(constraint.references.table) ~ '(' ~ columns | join(',') ~ ')' -%}
  {%- endif -%}
  {{ return(constraint.type ~ '(' ~ constraint.columns | join(',') ~ ')' ~ references ~ (' rely' if constraint.rely else '')) }}
{%- endmacro %}

{% macro parquet_direct__diff(relation, declaration, state, catalog) -%}
  {%- set changes = [] -%}
  {%- set notes = [] -%}
  {%- set reapply = [] -%}
  {%- set options = declaration.options -%}
  {%- set alter = 'alter iceberg table ' ~ relation -%}

  {%- for key, actual in [('external_volume', state.external_volume), ('catalog', state.catalog)] -%}
    {%- if options.get(key) is not none and dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(options.get(key)) != actual -%}
      {%- do changes.append({'key': key, 'description': key ~ ": '" ~ actual ~ "' -> '" ~ options.get(key) ~ "'", 'alterable': false, 'statements': []}) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if declaration.base_location.rstrip('/') != (state.base_location or '').rstrip('/') -%}
    {%- do changes.append({'key': 'base_location', 'description': "base_location: '" ~ state.base_location ~ "' -> '" ~ declaration.base_location ~ "'", 'alterable': false, 'statements': []}) -%}
  {%- endif -%}
  {%- set parameters = state.parameters or {} -%}
  {%- if options.get('iceberg_version') is not none and options.iceberg_version | string != parameters.get('ICEBERG_VERSION') -%}
    {%- do changes.append({'key': 'iceberg_version', 'description': 'iceberg_version: ' ~ parameters.get('ICEBERG_VERSION') ~ ' -> ' ~ options.iceberg_version, 'alterable': false, 'statements': []}) -%}
  {%- endif -%}
  {%- if declaration.auto_refresh != state.auto_refresh -%}
    {%- do changes.append({'key': 'auto_refresh', 'description': 'auto_refresh: ' ~ state.auto_refresh | lower ~ ' -> ' ~ declaration.auto_refresh | lower, 'alterable': true, 'statements': [alter ~ ' set auto_refresh = ' ~ dbt_snowflake_parquet_direct.parquet_direct__bool_sql(declaration.auto_refresh)]}) -%}
  {%- endif -%}
  {%- if options.get('replace_invalid_characters') is not none and dbt_snowflake_parquet_direct.parquet_direct__bool_sql(options.replace_invalid_characters) != (parameters.get('REPLACE_INVALID_CHARACTERS') or '') | lower -%}
    {%- do changes.append({'key': 'replace_invalid_characters', 'description': 'replace_invalid_characters: ' ~ parameters.get('REPLACE_INVALID_CHARACTERS') ~ ' -> ' ~ options.replace_invalid_characters | lower, 'alterable': true, 'statements': [alter ~ ' set replace_invalid_characters = ' ~ dbt_snowflake_parquet_direct.parquet_direct__bool_sql(options.replace_invalid_characters)]}) -%}
  {%- endif -%}
  {%- if options.get('log_event_level') is not none and options.log_event_level | upper != (parameters.get('LOG_EVENT_LEVEL') or '') | upper -%}
    {%- do changes.append({'key': 'log_event_level', 'description': 'log_event_level: ' ~ parameters.get('LOG_EVENT_LEVEL') ~ ' -> ' ~ options.log_event_level | upper, 'alterable': true, 'statements': [alter ~ ' set log_event_level = ' ~ options.log_event_level | upper]}) -%}
  {%- endif -%}
  {%- if declaration.comment is not none and declaration.comment != (state.comment or '') -%}
    {%- do changes.append({'key': 'comment', 'description': 'comment changed', 'alterable': true, 'statements': [alter ~ ' set comment = ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(declaration.comment)]}) -%}
  {%- endif -%}
  {%- set row_access_policy = declaration.row_access_policy -%}
  {%- if row_access_policy -%}
    {%- set on = [] -%}
    {%- for name in row_access_policy.on -%}{%- do on.append(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(name)) -%}{%- endfor -%}
    {%- set current = state.row_access_policy -%}
    {%- if not current or current.name != dbt_snowflake_parquet_direct.parquet_direct__qualify(row_access_policy.name) or current.on != on -%}
      {%- set add = 'add row access policy ' ~ row_access_policy.name ~ ' on (' ~ row_access_policy.on | join(', ') ~ ')' -%}
      {%- set statement = alter ~ ' ' ~ ('drop row access policy ' ~ current.name ~ ', ' if current else '') ~ add -%}
      {%- do changes.append({'key': 'row_access_policy', 'description': 'row_access_policy: ' ~ (current.name if current else 'none') ~ ' -> ' ~ row_access_policy.name, 'alterable': true, 'statements': [statement]}) -%}
    {%- endif -%}
  {%- endif -%}
  {%- set tags = {} -%}
  {%- for name, value in declaration.tag | dictsort if (state.table_tags or {}).get(dbt_snowflake_parquet_direct.parquet_direct__qualify(name)) != value -%}
    {%- do tags.update({name: value}) -%}
  {%- endfor -%}
  {%- if tags -%}
    {%- do changes.append({'key': 'tag', 'description': 'tag: ' ~ tags.keys() | join(', '), 'alterable': true, 'statements': [alter ~ ' set tag ' ~ dbt_snowflake_parquet_direct.parquet_direct__tag_list(tags)]}) -%}
  {%- endif -%}

  {%- set actual_columns = {} -%}
  {%- for column in state.columns or [] -%}{%- do actual_columns.update({column.name: column}) -%}{%- endfor -%}
  {%- set declared = declaration.columns | map(attribute='identifier') | list -%}
  {%- set primary_key_columns = [] -%}
  {%- for constraint in declaration.constraints if constraint.type == 'primary_key' -%}
    {%- do primary_key_columns.extend(constraint.columns) -%}
  {%- endfor -%}
  {%- for column in declaration.columns -%}
    {%- set actual = actual_columns.get(column.identifier) -%}
    {%- set column_alter = alter ~ ' alter column ' ~ column.sql -%}
    {%- if actual is none -%}
      {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' is declared but missing from the table', 'alterable': false, 'statements': []}) -%}
    {%- else -%}
      {%- if actual.kind != column.kind -%}
        {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' kind ' ~ actual.kind ~ ' -> ' ~ column.kind, 'alterable': false, 'statements': []}) -%}
      {%- endif -%}
      {%- set declared_type = dbt_snowflake_parquet_direct.parquet_direct__normalize_type(column.data_type) -%}
      {%- set actual_type = dbt_snowflake_parquet_direct.parquet_direct__normalize_type(actual.type) -%}
      {%- if declared_type != actual_type -%}
        {%- if declaration.schema_evolution and dbt_snowflake_parquet_direct.parquet_direct__is_widening(declared_type, actual_type) -%}
          {%- do notes.append('Column ' ~ column.identifier ~ ' was widened by schema evolution: ' ~ declared_type ~ ' -> ' ~ actual_type) -%}
        {%- else -%}
          {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' type ' ~ actual_type ~ ' -> ' ~ declared_type, 'alterable': false, 'statements': []}) -%}
        {%- endif -%}
      {%- endif -%}
      {%- if column.expression and dbt_snowflake_parquet_direct.parquet_direct__normalize_expression(column.expression, column.data_type) != dbt_snowflake_parquet_direct.parquet_direct__normalize_expression(actual.expression, column.data_type) -%}
        {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' expression changed', 'alterable': false, 'statements': []}) -%}
      {%- endif -%}
      {#- Snowflake makes primary key columns NOT NULL, so they are expected to be not null without not_null: true. -#}
      {%- set expected_not_null = column.not_null or column.identifier in primary_key_columns -%}
      {%- if expected_not_null == actual.nullable -%}
        {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' not_null ' ~ (not actual.nullable) | lower ~ ' -> ' ~ expected_not_null | lower, 'alterable': false, 'statements': []}) -%}
      {%- endif -%}
      {%- if column.comment is not none and column.comment != (actual.comment or '') -%}
        {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' comment changed', 'alterable': true, 'statements': [column_alter ~ ' comment ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(column.comment)]}) -%}
      {%- endif -%}
      {%- set policy = column.masking_policy -%}
      {%- if policy -%}
        {%- set using = ' using (' ~ policy.using | join(', ') ~ ')' if policy.using else '' -%}
        {#- FORCE replaces an existing policy in one statement, so the column is never unmasked in between. -#}
        {%- set statement = column_alter ~ ' set masking policy ' ~ policy.name ~ using ~ ' force' -%}
        {%- if (state.masking_policies or {}).get(column.identifier) != dbt_snowflake_parquet_direct.parquet_direct__qualify(policy.name) -%}
          {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' masking_policy -> ' ~ policy.name, 'alterable': true, 'statements': [statement]}) -%}
        {%- else -%}
          {#- POLICY_REFERENCES reports no USING columns for masking policies, so adding, changing, or removing using is invisible; re-apply instead. -#}
          {%- do reapply.append(statement) -%}
        {%- endif -%}
      {%- endif -%}
      {%- set column_tags = {} -%}
      {%- for name, value in column.tag | dictsort if ((state.column_tags or {}).get(column.identifier) or {}).get(dbt_snowflake_parquet_direct.parquet_direct__qualify(name)) != value -%}
        {%- do column_tags.update({name: value}) -%}
      {%- endfor -%}
      {%- if column_tags -%}
        {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.identifier ~ ' tag: ' ~ column_tags.keys() | join(', '), 'alterable': true, 'statements': [column_alter ~ ' set tag ' ~ dbt_snowflake_parquet_direct.parquet_direct__tag_list(column_tags)]}) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for column in state.columns or [] if column.name not in declared -%}
    {%- if column.kind == 'PARTITION' and catalog.table_format == 'HIVE' -%}
      {%- do notes.append('Column ' ~ column.name ~ ' is a Hive partition column inferred by Snowflake') -%}
    {%- elif column.kind == 'COLUMN' and (declaration.schema_inference or declaration.schema_evolution) -%}
      {%- do notes.append('Column ' ~ column.name ~ ' was added by schema inference or evolution') -%}
    {%- else -%}
      {%- do changes.append({'key': 'columns', 'description': 'columns: ' ~ column.name ~ ' exists in the table but is not declared', 'alterable': false, 'statements': []}) -%}
    {%- endif -%}
  {%- endfor -%}

  {#- Constraints are managed only when the model declares some. -#}
  {%- if declaration.constraints -%}
    {#- A declared name must match; an omitted name accepts the name Snowflake generated. Named ones match first. -#}
    {%- set matched = [] -%}
    {%- set missing = [] -%}
    {%- for named in [true, false] -%}
      {%- for constraint in declaration.constraints if (constraint.name is not none) == named -%}
        {%- set key = dbt_snowflake_parquet_direct.parquet_direct__constraint_key(constraint) -%}
        {%- set name = dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(constraint.name) if named else none -%}
        {%- set candidates = [] -%}
        {%- for actual in state.constraints or [] if actual.name not in matched and (name is none or actual.name == name) and dbt_snowflake_parquet_direct.parquet_direct__constraint_key(actual) == key -%}
          {%- do candidates.append(actual.name) -%}
        {%- endfor -%}
        {%- if candidates -%}{%- do matched.append(candidates[0]) -%}{%- else -%}{%- do missing.append(constraint) -%}{%- endif -%}
      {%- endfor -%}
    {%- endfor -%}
    {%- set statements = [] -%}
    {%- for constraint in state.constraints or [] if constraint.name not in matched -%}
      {%- do statements.append(alter ~ ' drop constraint ' ~ dbt_snowflake_parquet_direct.parquet_direct__quote_identifier(constraint.name)) -%}
    {%- endfor -%}
    {%- for constraint in missing -%}
      {%- do statements.append(alter ~ ' add ' ~ dbt_snowflake_parquet_direct.parquet_direct__constraint_sql(constraint)) -%}
    {%- endfor -%}
    {%- if statements -%}
      {%- do changes.append({'key': 'constraints', 'description': 'constraints changed', 'alterable': true, 'statements': statements}) -%}
    {%- endif -%}
  {%- endif -%}

  {{ return({'changes': changes, 'notes': notes, 'reapply': reapply}) }}
{%- endmacro %}

{# The error raised when ALTER cannot apply the declaration (design section 6, stage 3). #}
{% macro parquet_direct__recreation_error(relation, changes, model_name, full_refresh_disabled) -%}
  {%- set lines = ['Parquet Direct table ' ~ relation ~ ' has changes that ALTER cannot apply:'] -%}
  {%- for change in changes if not change.alterable -%}{%- do lines.append('  - ' ~ change.description) -%}{%- endfor -%}
  {%- set alterable = changes | selectattr('alterable') | list -%}
  {%- if alterable -%}
    {%- do lines.append('Changes that ALTER can apply (not applied):') -%}
    {%- for change in alterable -%}{%- do lines.append('  - ' ~ change.description) -%}{%- endfor -%}
  {%- endif -%}
  {%- do lines.append('Run `dbt run --full-refresh -s ' ~ model_name ~ '` to re-create the table.') -%}
  {%- do lines.append('Re-creating re-registers every file (data registration charges apply),') -%}
  {%- do lines.append('drops the table history, and makes streams on the table stale.') -%}
  {%- if full_refresh_disabled -%}
    {%- do lines.append('This model sets full_refresh: false; remove it before running --full-refresh.') -%}
  {%- endif -%}
  {{ return(lines | join('\n')) }}
{%- endmacro %}

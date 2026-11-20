{# Static validation (design section 6, stage 1). Returns a list of "Sxx: ..." messages; issues no SQL. #}

{% macro parquet_direct__known_keys() -%}
  {{ return({
    'top': [
      'external_volume', 'catalog', 'base_location', 'pattern', 'iceberg_version',
      'enable_schema_inference', 'enable_schema_evolution', 'match_by_column_name',
      'auto_refresh', 'refresh_on_create', 'replace_invalid_characters', 'log_event_level',
      'columns', 'virtual_columns', 'partition_columns', 'constraints',
      'row_access_policy', 'tag', 'comment',
      'refresh_paths', 'skip_refresh_if_refresh_paths_empty', 'registration_check', 'previous_versions',
    ],
    'column': ['name', 'data_type', 'expression', 'not_null', 'constraints', 'masking_policy', 'tag', 'comment', 'quote'],
    'registration_check': ['on_skipped', 'on_unhealthy', 'lookback_hours', 'ignore_file_patterns', 'max_reported_files'],
    'constraint': ['type', 'name', 'columns', 'rely', 'references'],
    'inline_constraint': ['type', 'rely', 'references'],
    'references': ['table', 'columns'],
    'row_access_policy': ['name', 'on'],
    'masking_policy': ['name', 'using'],
    'previous_version': ['table', 'columns'],
  }) }}
{%- endmacro %}

{% macro parquet_direct__check_keys(errors, value, allowed, where) -%}
  {%- for key in value -%}
    {%- if key not in allowed -%}
      {%- do errors.append("S01: unknown key '" ~ key ~ "' in " ~ where ~ ". Valid keys: " ~ allowed | join(', ')) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}

{% macro parquet_direct__check_string_list(errors, value, where, allow_empty=true) -%}
  {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_list(value) or (not allow_empty and value | length == 0) -%}
    {%- do errors.append("S02: " ~ where ~ " must be a " ~ ('' if allow_empty else 'non-empty ') ~ "list of strings") -%}
  {%- else -%}
    {%- for item in value -%}
      {%- if item is not string -%}
        {%- do errors.append("S02: " ~ where ~ " must contain only strings") -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{%- endmacro %}

{% macro parquet_direct__check_tags(errors, value, where) -%}
  {%- if value is not mapping -%}
    {%- do errors.append("S02: " ~ where ~ " must be a mapping of tag names to string values") -%}
  {%- else -%}
    {%- for key, tag_value in value.items() -%}
      {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_three_part(key) -%}
        {%- do errors.append("S02: " ~ where ~ " key '" ~ key ~ "' must be a three-part name (database.schema.tag)") -%}
      {%- endif -%}
      {%- if tag_value is not string -%}
        {%- do errors.append("S02: " ~ where ~ "." ~ key ~ " must be a string") -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{%- endmacro %}

{% macro parquet_direct__check_constraint(errors, value, where, inline) -%}
  {%- if value is not mapping -%}
    {%- do errors.append("S02: " ~ where ~ " must be a mapping") -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set keys = dbt_snowflake_parquet_direct.parquet_direct__known_keys() -%}
  {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, value, keys.inline_constraint if inline else keys.constraint, where) -%}
  {%- if value.get('type') not in ['primary_key', 'unique', 'foreign_key'] -%}
    {%- do errors.append("S02: " ~ where ~ ".type must be one of primary_key, unique, foreign_key") -%}
  {%- endif -%}
  {%- if not inline -%}
    {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, value.get('columns'), where ~ ".columns", allow_empty=false) -%}
    {%- if value.get('name') is not none and value.name is not string -%}
      {%- do errors.append("S02: " ~ where ~ ".name must be a string") -%}
    {%- endif -%}
  {%- endif -%}
  {%- if value.get('rely') is not none and value.rely is not boolean -%}
    {%- do errors.append("S02: " ~ where ~ ".rely must be a boolean") -%}
  {%- endif -%}
  {%- set references = value.get('references') -%}
  {%- if value.get('type') == 'foreign_key' -%}
    {%- if references is not mapping -%}
      {%- do errors.append("S02: " ~ where ~ ".references must be a mapping with table and columns for a foreign_key") -%}
    {%- else -%}
      {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, references, keys.references, where ~ ".references") -%}
      {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_three_part(references.get('table')) -%}
        {%- do errors.append("S02: " ~ where ~ ".references.table must be a three-part name (database.schema.table)") -%}
      {%- endif -%}
      {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, references.get('columns'), where ~ ".references.columns", allow_empty=false) -%}
    {%- endif -%}
  {%- elif references is not none -%}
    {%- do errors.append("S02: " ~ where ~ ".references is only valid for a foreign_key") -%}
  {%- endif -%}
{%- endmacro %}

{% macro parquet_direct__check_column(errors, column, where, kind) -%}
  {%- set keys = dbt_snowflake_parquet_direct.parquet_direct__known_keys() -%}
  {%- if column is not mapping -%}
    {%- do errors.append("S02: " ~ where ~ " must be a mapping") -%}
    {{ return(none) }}
  {%- endif -%}
  {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, column, keys.column, where) -%}
  {%- for key in ['name', 'data_type'] -%}
    {%- if column.get(key) is not string or column.get(key) | trim == '' -%}
      {%- do errors.append("S02: " ~ where ~ "." ~ key ~ " is required and must be a string") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for key in ['expression', 'comment'] -%}
    {%- if column.get(key) is not none and column.get(key) is not string -%}
      {%- do errors.append("S02: " ~ where ~ "." ~ key ~ " must be a string") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for key in ['not_null', 'quote'] -%}
    {%- if column.get(key) is not none and column.get(key) is not boolean -%}
      {%- do errors.append("S02: " ~ where ~ "." ~ key ~ " must be a boolean") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if column.get('name') is string and not column.get('quote') and not modules.re.match('^[A-Za-z_][A-Za-z0-9_$]*$', column.name) -%}
    {%- do errors.append("S02: " ~ where ~ ".name '" ~ column.name ~ "' is not a valid unquoted identifier; set quote: true") -%}
  {%- endif -%}
  {%- if column.get('tag') is not none -%}
    {%- do dbt_snowflake_parquet_direct.parquet_direct__check_tags(errors, column.tag, where ~ ".tag") -%}
  {%- endif -%}
  {%- set policy = column.get('masking_policy') -%}
  {%- if policy is mapping -%}
    {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, policy, keys.masking_policy, where ~ ".masking_policy") -%}
    {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_three_part(policy.get('name')) -%}
      {%- do errors.append("S02: " ~ where ~ ".masking_policy.name must be a three-part name (database.schema.policy)") -%}
    {%- endif -%}
    {%- if policy.get('using') is not none -%}
      {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, policy.using, where ~ ".masking_policy.using", allow_empty=false) -%}
    {%- endif -%}
  {%- elif policy is not none and not dbt_snowflake_parquet_direct.parquet_direct__is_three_part(policy) -%}
    {%- do errors.append("S02: " ~ where ~ ".masking_policy must be a three-part name (database.schema.policy) or a mapping with name and using") -%}
  {%- endif -%}
  {%- if column.get('constraints') is not none -%}
    {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_list(column.constraints) -%}
      {%- do errors.append("S02: " ~ where ~ ".constraints must be a list") -%}
    {%- else -%}
      {%- for constraint in column.constraints -%}
        {%- do dbt_snowflake_parquet_direct.parquet_direct__check_constraint(errors, constraint, where ~ ".constraints[" ~ loop.index0 ~ "]", true) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {#- S09 -#}
  {%- if kind == 'columns' and column.get('expression') is not none -%}
    {%- do errors.append("S09: " ~ where ~ " is a data column and cannot have an expression; use virtual_columns") -%}
  {%- elif kind != 'columns' and column.get('expression') is none -%}
    {%- do errors.append("S09: " ~ where ~ " requires an expression") -%}
  {%- endif -%}
  {#- S10 -#}
  {%- if kind == 'partition_columns' and column.get('expression') is string and 'METADATA$FILENAME' not in column.expression | upper -%}
    {%- do errors.append("S10: " ~ where ~ ".expression must reference METADATA$FILENAME") -%}
  {%- endif -%}
{%- endmacro %}

{#- model_sql is the compiled model body, where config() has already rendered to nothing. -#}
{% macro parquet_direct__validate(declaration, dbt_config={}, model_sql='') -%}
  {%- set errors = [] -%}
  {%- set keys = dbt_snowflake_parquet_direct.parquet_direct__known_keys() -%}
  {%- if declaration is not mapping -%}
    {%- do errors.append("S02: meta.parquet_direct must be a mapping") -%}
    {{ return(errors) }}
  {%- endif -%}

  {#- S01, S02: keys and types -#}
  {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, declaration, keys.top, 'meta.parquet_direct') -%}
  {%- for key in ['external_volume', 'catalog', 'base_location', 'pattern', 'comment', 'log_event_level'] -%}
    {%- if declaration.get(key) is not none and declaration.get(key) is not string -%}
      {%- do errors.append("S02: " ~ key ~ " must be a string") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for key in ['enable_schema_inference', 'enable_schema_evolution', 'auto_refresh', 'refresh_on_create', 'replace_invalid_characters', 'skip_refresh_if_refresh_paths_empty'] -%}
    {%- if declaration.get(key) is not none and declaration.get(key) is not boolean -%}
      {%- do errors.append("S02: " ~ key ~ " must be a boolean") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if declaration.get('iceberg_version') is not none and declaration.iceberg_version not in [2, 3] -%}
    {%- do errors.append("S02: iceberg_version must be 2 or 3") -%}
  {%- endif -%}
  {%- if declaration.get('match_by_column_name') is not none and (declaration.match_by_column_name is not string or declaration.match_by_column_name | lower not in ['case_insensitive', 'case_sensitive']) -%}
    {%- do errors.append("S02: match_by_column_name must be case_insensitive or case_sensitive") -%}
  {%- endif -%}
  {%- if declaration.get('log_event_level') is string and declaration.log_event_level | upper not in ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL', 'OFF'] -%}
    {%- do errors.append("S02: log_event_level must be one of TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF") -%}
  {%- endif -%}
  {%- if declaration.get('tag') is not none -%}
    {%- do dbt_snowflake_parquet_direct.parquet_direct__check_tags(errors, declaration.tag, 'tag') -%}
  {%- endif -%}
  {%- set row_access_policy = declaration.get('row_access_policy') -%}
  {%- if row_access_policy is not none -%}
    {%- if row_access_policy is not mapping -%}
      {%- do errors.append("S02: row_access_policy must be a mapping with name and on") -%}
    {%- else -%}
      {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, row_access_policy, keys.row_access_policy, 'row_access_policy') -%}
      {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_three_part(row_access_policy.get('name')) -%}
        {%- do errors.append("S02: row_access_policy.name must be a three-part name (database.schema.policy)") -%}
      {%- endif -%}
      {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, row_access_policy.get('on'), 'row_access_policy.on', allow_empty=false) -%}
    {%- endif -%}
  {%- endif -%}
  {%- if declaration.get('refresh_paths') is not none -%}
    {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, declaration.refresh_paths, 'refresh_paths') -%}
  {%- endif -%}
  {%- set check = declaration.get('registration_check') -%}
  {%- if check is not none -%}
    {%- if check is not mapping -%}
      {%- do errors.append("S02: registration_check must be a mapping") -%}
    {%- else -%}
      {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, check, keys.registration_check, 'registration_check') -%}
      {%- for key in ['on_skipped', 'on_unhealthy'] -%}
        {%- if check.get(key) is not none and check.get(key) not in ['error', 'warn', 'ignore'] -%}
          {%- do errors.append("S02: registration_check." ~ key ~ " must be error, warn, or ignore") -%}
        {%- endif -%}
      {%- endfor -%}
      {%- for key in ['lookback_hours', 'max_reported_files'] -%}
        {%- if check.get(key) is not none and (check.get(key) is not integer or check.get(key) < 1) -%}
          {%- do errors.append("S02: registration_check." ~ key ~ " must be a positive integer") -%}
        {%- endif -%}
      {%- endfor -%}
      {%- if check.get('ignore_file_patterns') is not none -%}
        {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, check.ignore_file_patterns, 'registration_check.ignore_file_patterns') -%}
        {%- if dbt_snowflake_parquet_direct.parquet_direct__is_list(check.ignore_file_patterns) -%}
          {%- do errors.extend(dbt_snowflake_parquet_direct.parquet_direct__ignore_pattern_errors(check.ignore_file_patterns, 'registration_check.ignore_file_patterns')) -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}

  {#- Columns and constraints (S02, S09, S10) -#}
  {%- set names = [] -%}
  {%- for kind in ['columns', 'virtual_columns', 'partition_columns'] -%}
    {%- set entries = declaration.get(kind) -%}
    {%- if entries is not none and not dbt_snowflake_parquet_direct.parquet_direct__is_list(entries) -%}
      {%- do errors.append("S02: " ~ kind ~ " must be a list") -%}
    {%- elif entries is not none -%}
      {%- for column in entries -%}
        {%- do dbt_snowflake_parquet_direct.parquet_direct__check_column(errors, column, kind ~ "[" ~ loop.index0 ~ "]", kind) -%}
        {%- if column is mapping and column.get('name') is string -%}
          {%- do names.append(column.name | upper) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if declaration.get('constraints') is not none -%}
    {%- if not dbt_snowflake_parquet_direct.parquet_direct__is_list(declaration.constraints) -%}
      {%- do errors.append("S02: constraints must be a list") -%}
    {%- else -%}
      {%- for constraint in declaration.constraints -%}
        {%- do dbt_snowflake_parquet_direct.parquet_direct__check_constraint(errors, constraint, "constraints[" ~ loop.index0 ~ "]", false) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}

  {#- S03 -#}
  {%- set base_location = declaration.get('base_location') -%}
  {%- if base_location is not string or base_location | trim == '' -%}
    {%- do errors.append("S03: base_location is required") -%}
  {%- elif base_location.startswith('/') or '..' in base_location.split('/') or '://' in base_location or not base_location.endswith('/') -%}
    {%- do errors.append("S03: base_location '" ~ base_location ~ "' must be a directory relative to the external volume: no leading '/', no '..', no scheme, and a trailing '/'") -%}
  {%- endif -%}

  {%- set columns = declaration.get('columns') if dbt_snowflake_parquet_direct.parquet_direct__is_list(declaration.get('columns')) else [] -%}
  {%- set inference = declaration.get('enable_schema_inference') -%}
  {%- set evolution = declaration.get('enable_schema_evolution') -%}
  {%- set auto_refresh = declaration.get('auto_refresh') == true -%}

  {#- S04 to S08 -#}
  {%- if evolution == true and inference == false -%}
    {%- do errors.append("S04: enable_schema_evolution: true requires enable_schema_inference to be true or omitted") -%}
  {%- endif -%}
  {%- if declaration.get('refresh_on_create') == false and columns | length == 0 -%}
    {%- do errors.append("S05: refresh_on_create: false requires columns") -%}
  {%- endif -%}
  {%- if inference == false and columns | length == 0 -%}
    {%- do errors.append("S06: enable_schema_inference: false requires columns") -%}
  {%- endif -%}
  {%- if auto_refresh and (declaration.get('refresh_paths') or declaration.get('skip_refresh_if_refresh_paths_empty') is not none) -%}
    {%- do errors.append("S07: refresh_paths and skip_refresh_if_refresh_paths_empty cannot be set with auto_refresh: true, because Snowflake rejects manual refresh while auto refresh is on") -%}
  {%- endif -%}
  {%- if dbt_snowflake_parquet_direct.parquet_direct__is_list(declaration.get('refresh_paths')) -%}
    {%- set seen = [] -%}
    {%- for path in declaration.refresh_paths if path is string -%}
      {%- if path | trim == '' or '..' in path.split('/') or path in seen -%}
        {%- do errors.append("S08: refresh_paths entry '" ~ path ~ "' is empty, contains '..', or is duplicated") -%}
      {%- endif -%}
      {%- do seen.append(path) -%}
    {%- endfor -%}
  {%- endif -%}

  {#- S11 -#}
  {%- set counted = [] -%}
  {%- for name in names -%}
    {%- if name in counted -%}
      {%- do errors.append("S11: column name '" ~ name ~ "' is declared more than once (names are compared case-insensitively)") -%}
    {%- endif -%}
    {%- do counted.append(name) -%}
  {%- endfor -%}

  {#- S12: only checkable when no columns are inferred -#}
  {%- set effective_inference = inference if inference is not none else (columns | length == 0 or evolution == true) -%}
  {%- if not effective_inference -%}
    {%- set referenced = [] -%}
    {%- if dbt_snowflake_parquet_direct.parquet_direct__is_list(declaration.get('constraints')) -%}
      {%- for constraint in declaration.constraints if constraint is mapping and dbt_snowflake_parquet_direct.parquet_direct__is_list(constraint.get('columns')) -%}
        {%- for name in constraint.columns -%}{%- do referenced.append(name) -%}{%- endfor -%}
      {%- endfor -%}
    {%- endif -%}
    {%- if row_access_policy is mapping and dbt_snowflake_parquet_direct.parquet_direct__is_list(row_access_policy.get('on')) -%}
      {%- for name in row_access_policy.on -%}{%- do referenced.append(name) -%}{%- endfor -%}
    {%- endif -%}
    {%- for name in referenced if name is string and name | replace('"', '') | upper not in names -%}
      {%- do errors.append("S12: column '" ~ name ~ "' is referenced by constraints or row_access_policy but not declared") -%}
    {%- endfor -%}
  {%- endif -%}

  {#- S16, S17: previous versions -#}
  {%- set previous_versions = declaration.get('previous_versions') -%}
  {%- if previous_versions is not none and not dbt_snowflake_parquet_direct.parquet_direct__is_list(previous_versions) -%}
    {%- do errors.append("S02: previous_versions must be a list") -%}
  {%- elif previous_versions -%}
    {%- if columns | length == 0 -%}
      {%- do errors.append("S16: previous_versions requires columns, which define the combined view") -%}
    {%- endif -%}
    {%- if auto_refresh -%}
      {%- do errors.append("S17: previous_versions cannot be set with auto_refresh: true, because auto refresh next to a previous version's files is not verified") -%}
    {%- endif -%}
    {%- set tables = [] -%}
    {%- for version in previous_versions -%}
      {%- set where = 'previous_versions[' ~ loop.index0 ~ ']' -%}
      {%- if version is not mapping -%}
        {%- do errors.append("S02: " ~ where ~ " must be a mapping with table and columns") -%}
      {%- else -%}
        {%- do dbt_snowflake_parquet_direct.parquet_direct__check_keys(errors, version, keys.previous_version, where) -%}
        {%- set table = version.get('table') -%}
        {%- if table is not string or not modules.re.match('^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")$', table) -%}
          {%- do errors.append("S02: " ~ where ~ ".table must be a table name in the model's schema, without database, schema, or embedded double quotes") -%}
        {%- elif dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(table) in tables -%}
          {%- do errors.append("S16: previous version table " ~ table ~ " is listed more than once") -%}
        {%- else -%}
          {%- do tables.append(dbt_snowflake_parquet_direct.parquet_direct__normalize_identifier(table)) -%}
        {%- endif -%}
        {%- set expressions = version.get('columns') -%}
        {%- if expressions is not none and expressions is not mapping -%}
          {%- do errors.append("S02: " ~ where ~ ".columns must be a mapping of column names to SQL expressions") -%}
        {%- elif expressions is mapping -%}
          {%- set keys = {} -%}
          {%- for name, expression in expressions.items() -%}
            {%- if expression is not string or expression | trim == '' -%}
              {%- do errors.append("S02: " ~ where ~ ".columns." ~ name ~ " must be a non-empty SQL expression") -%}
            {%- endif -%}
            {%- set key = dbt_snowflake_parquet_direct.parquet_direct__column_key(name) if name is string else none -%}
            {%- if key not in names -%}
              {%- do errors.append("S16: " ~ where ~ ".columns key '" ~ name ~ "' is not a declared column") -%}
            {%- elif key in keys -%}
              {%- do errors.append("S16: " ~ where ~ ".columns keys '" ~ keys[key] ~ "' and '" ~ name ~ "' name the same column") -%}
            {%- else -%}
              {%- do keys.update({key: name}) -%}
            {%- endif -%}
          {%- endfor -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {#- S13 -#}
  {%- if not auto_refresh and check is mapping and check.get('lookback_hours') is not none -%}
    {%- do errors.append("S13: registration_check.lookback_hours applies only with auto_refresh: true") -%}
  {%- endif -%}

  {#- S14 -#}
  {%- set re = modules.re -%}
  {%- set body = re.sub('(?s)/[*].*?[*]/', '', model_sql or '') -%}
  {%- set body = re.sub('--[^\\n]*', '', body) -%}
  {%- if body | trim != '' -%}
    {%- do errors.append("S14: the model body must contain only config() and comments, because parquet_direct does not use SQL") -%}
  {%- endif -%}

  {#- S15. transient is not checked: projects often set +transient for every model. -#}
  {%- for key in ['cluster_by', 'unique_key', 'incremental_strategy'] -%}
    {%- if dbt_config.get(key) is not none -%}
      {%- do errors.append("S15: " ~ key ~ " is not supported by parquet_direct") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if dbt_config.get('contract') is not none and dbt_config.contract.enforced -%}
    {%- do errors.append("S15: contract.enforced is not supported by parquet_direct") -%}
  {%- endif -%}

  {{ return(errors) }}
{%- endmacro %}

{# Pre-flight checks (design section 6, stage 2) over what was read from Snowflake. Returns {'errors': [...], 'warnings': [...]}. #}
{% macro parquet_direct__preflight(declaration, catalog, state, full_refresh) -%}
  {%- set errors = [] -%}
  {%- set warnings = [] -%}
  {%- if catalog is none -%}
    {%- do errors.append("R01: no catalog integration is configured or set as the default for the schema") -%}
  {%- elif catalog.catalog_source != 'OBJECT_STORE' or catalog.table_format not in ['NONE', 'HIVE'] -%}
    {%- do errors.append("R01: catalog integration " ~ catalog.name ~ " has CATALOG_SOURCE = " ~ catalog.catalog_source ~ " and TABLE_FORMAT = " ~ catalog.table_format ~ "; Parquet Direct requires OBJECT_STORE with NONE or HIVE") -%}
  {%- elif catalog.table_format == 'HIVE' and declaration.partition_columns -%}
    {%- do errors.append("R02: catalog integration " ~ catalog.name ~ " uses TABLE_FORMAT = HIVE, which infers partition columns; remove partition_columns") -%}
  {%- endif -%}
  {%- if state.exists and not state.is_parquet_direct -%}
    {%- set hint = "; a combined view stays a view, so removing previous_versions is not supported" if state.object_type == 'view' else '' -%}
    {%- do errors.append("R03: an object that is not a Parquet Direct table (" ~ state.object_type ~ ") already exists; parquet_direct never replaces it" ~ hint) -%}
  {%- elif state.exists and full_refresh and not declaration.refresh_on_create -%}
    {%- do warnings.append("R04: re-creating with refresh_on_create: false registers only the files under refresh_paths until the next refresh") -%}
  {%- endif -%}
  {%- set current_table_comment = dbt_snowflake_parquet_direct.parquet_direct__current_table_comment() -%}
  {%- if state.exists and state.comment == current_table_comment and declaration.comment != current_table_comment -%}
    {%- do errors.append("R13: the table is the current table of a combined view built from previous_versions, so only that model changes it; use another alias") -%}
  {%- endif -%}
  {{ return({'errors': errors, 'warnings': warnings}) }}
{%- endmacro %}

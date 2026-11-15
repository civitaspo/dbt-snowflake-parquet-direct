{# Manual refresh and skipped-file detection (design section 7). #}

{% macro parquet_direct__refresh_statements(relation, declaration) -%}
  {%- if declaration.auto_refresh -%}{{ return([]) }}{%- endif -%}
  {%- set statements = [] -%}
  {%- for path in declaration.refresh_paths -%}
    {%- do statements.append('alter iceberg table ' ~ relation ~ ' refresh ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(path)) -%}
  {%- endfor -%}
  {%- if not declaration.refresh_paths and not declaration.skip_refresh_if_refresh_paths_empty -%}
    {%- do statements.append('alter iceberg table ' ~ relation ~ ' refresh') -%}
  {%- endif -%}
  {{ return(statements) }}
{%- endmacro %}

{% macro parquet_direct__history_sql(relation, since_sql) -%}
  {{ return(
    'select file_name, operation_status, message, operation_time\n'
    ~ 'from table(' ~ relation.include(schema=false, identifier=false) ~ '.information_schema.iceberg_table_file_registration_history(\n'
    ~ '    table_name => ' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(relation | string) ~ ',\n'
    ~ '    start_time => ' ~ since_sql ~ '\n'
    ~ '))\n'
    ~ 'order by operation_time'
  ) }}
{%- endmacro %}

{# Parse SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS, which returns JSON when auto refresh is on and plain text when it is off. #}
{% macro parquet_direct__parse_auto_refresh_status(text) -%}
  {%- set status = fromjson(text or '', none) -%}
  {%- if status is not mapping -%}{{ return({'enabled': false, 'message': text}) }}{%- endif -%}
  {{ return({
    'enabled': true,
    'execution_state': status.get('executionState'),
    'pending_file_count': status.get('pendingFileCount'),
    'channel_error': status.get('channelErrorMessage'),
    'reason': status.get('invalidExecutionStateReason'),
  }) }}
{%- endmacro %}

{#
  What each OPERATION_STATUS means for the table. Failed statuses leave the table out of step with storage.
  The documentation lists REGISTER_FAILED and UNREGISTER_FAILED; SKIPPED is what the November 2026 evaluation observed.
  REGISTER_SKIPPED (already registered) and REGISTER_SKIPPED_PATTERN_MISMATCH change nothing.
#}
{% macro parquet_direct__status_categories() -%}
  {{ return({
    'REGISTERED_NEW': 'registered',
    'REGISTERED_UPDATE': 'registered',
    'UNREGISTERED': 'unregistered',
    'SKIPPED': 'failed',
    'REGISTER_FAILED': 'failed',
    'UNREGISTER_FAILED': 'failed',
  }) }}
{%- endmacro %}

{#
  Summarize registration history rows (ordered by operation_time) into counts and skipped files.
  A skipped file is one whose latest status is a failed status; a later registration or unregistration resolves it.
#}
{% macro parquet_direct__summarize_history(rows, base_location, refresh_paths=[], ignore_file_patterns=[]) -%}
  {%- set categories = dbt_snowflake_parquet_direct.parquet_direct__status_categories() -%}
  {%- set counts = {'registered': 0, 'unregistered': 0, 'failed': 0} -%}
  {%- set latest = {} -%}
  {%- set by_path = {} -%}
  {%- for path in refresh_paths -%}{%- do by_path.update({path: {'registered': 0, 'unregistered': 0, 'failed': 0}}) -%}{%- endfor -%}
  {%- for row in rows -%}
    {%- do latest.update({row.file_name: row}) -%}
    {%- set category = categories.get(row.operation_status) -%}
    {%- if category -%}
      {%- do counts.update({category: counts[category] + 1}) -%}
      {%- set relative = row.file_name[base_location | length:] if row.file_name.startswith(base_location) else row.file_name -%}
      {%- for path in refresh_paths if relative.startswith(path.lstrip('/')) -%}
        {%- do by_path[path].update({category: by_path[path][category] + 1}) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set skipped = [] -%}
  {%- set ignored = [] -%}
  {%- for file_name, row in latest.items() if categories.get(row.operation_status) == 'failed' -%}
    {%- set is_ignored = [] -%}
    {%- for pattern in ignore_file_patterns if modules.re.search(pattern, file_name) -%}{%- do is_ignored.append(pattern) -%}{%- endfor -%}
    {%- do (ignored if is_ignored else skipped).append(row) -%}
  {%- endfor -%}
  {{ return({'counts': counts, 'by_path': by_path, 'skipped': skipped, 'ignored': ignored}) }}
{%- endmacro %}

{% macro parquet_direct__skipped_message(relation, skipped, max_reported_files) -%}
  {%- set lines = [skipped | length ~ ' skipped file(s) in ' ~ relation ~ ' failed to register or unregister, so the table does not match storage:'] -%}
  {%- for row in skipped[:max_reported_files] -%}
    {%- do lines.append('  - ' ~ row.file_name ~ ' (' ~ row.operation_status ~ '): ' ~ row.message) -%}
  {%- endfor -%}
  {%- if skipped | length > max_reported_files -%}
    {%- do lines.append('  ... and ' ~ (skipped | length - max_reported_files) ~ ' more') -%}
  {%- endif -%}
  {{ return(lines | join('\n')) }}
{%- endmacro %}

{#
  Decide what the registration check reports. `status_text` is the SYSTEM$PARQUET_DIRECT_AUTO_REFRESH_STATUS output
  (none under manual refresh) and `rows` the history rows. Returns {'summary': ..., 'findings': [{severity, message}]},
  where severity is error, warn, ignore, or info.
#}
{% macro parquet_direct__registration_findings(relation, declaration, status_text, rows) -%}
  {%- set check = declaration.registration_check -%}
  {%- set findings = [] -%}
  {%- if declaration.auto_refresh -%}
    {%- set status = dbt_snowflake_parquet_direct.parquet_direct__parse_auto_refresh_status(status_text) -%}
    {%- if not status.enabled -%}
      {%- do findings.append({'severity': check.on_unhealthy, 'message': 'Auto refresh is not enabled for ' ~ relation ~ ': ' ~ status.message}) -%}
    {%- elif status.execution_state != 'RUNNING' -%}
      {%- do findings.append({'severity': check.on_unhealthy, 'message': 'Auto refresh of ' ~ relation ~ ' is ' ~ status.execution_state ~ ': ' ~ (status.channel_error or status.reason or 'no reason given')}) -%}
    {%- else -%}
      {%- do findings.append({'severity': 'info', 'message': 'Auto refresh of ' ~ relation ~ ' is RUNNING with ' ~ status.pending_file_count ~ ' pending file(s); zero does not prove every file is registered.'}) -%}
    {%- endif -%}
  {%- endif -%}
  {%- set summary = dbt_snowflake_parquet_direct.parquet_direct__summarize_history(rows, declaration.base_location, declaration.refresh_paths, check.ignore_file_patterns) -%}
  {%- for path, counts in summary.by_path.items() -%}
    {%- do findings.append({'severity': 'info', 'message': 'Refresh path ' ~ path ~ ': ' ~ counts.registered ~ ' registered, ' ~ counts.unregistered ~ ' unregistered, ' ~ counts.failed ~ ' failed'}) -%}
  {%- endfor -%}
  {%- if summary.ignored -%}
    {%- do findings.append({'severity': 'info', 'message': summary.ignored | length ~ ' skipped file(s) in ' ~ relation ~ ' match ignore_file_patterns'}) -%}
  {%- endif -%}
  {%- if summary.skipped -%}
    {%- do findings.append({'severity': check.on_skipped, 'message': dbt_snowflake_parquet_direct.parquet_direct__skipped_message(relation, summary.skipped, check.max_reported_files)}) -%}
  {%- endif -%}
  {{ return({'summary': summary, 'findings': findings}) }}
{%- endmacro %}

{# Run the registration check since `since_sql`: log and warn every finding, then raise all errors together. #}
{% macro parquet_direct__check_registration(relation, declaration, since_sql) -%}
  {%- set status_text = none -%}
  {%- if declaration.auto_refresh -%}
    {%- set status_text = dbt_snowflake_parquet_direct.parquet_direct__rows('select system$parquet_direct_auto_refresh_status(' ~ dbt_snowflake_parquet_direct.parquet_direct__sql_string(relation | string) ~ ') as status')[0].status -%}
  {%- endif -%}
  {%- set rows = dbt_snowflake_parquet_direct.parquet_direct__rows(dbt_snowflake_parquet_direct.parquet_direct__history_sql(relation, since_sql)) -%}
  {%- set result = dbt_snowflake_parquet_direct.parquet_direct__registration_findings(relation, declaration, status_text, rows) -%}
  {%- for finding in result.findings if finding.severity != 'error' -%}
    {%- do dbt_snowflake_parquet_direct.parquet_direct__report(finding.severity, finding.message) -%}
  {%- endfor -%}
  {%- set errors = result.findings | selectattr('severity', 'equalto', 'error') | map(attribute='message') | list -%}
  {%- if errors -%}
    {{ exceptions.raise_compiler_error(errors | join('\n')) }}
  {%- endif -%}
  {{ return(result.summary) }}
{%- endmacro %}

{% macro parquet_direct__lookback_error(value) -%}
  {{ return(none if value is integer and value > 0 else 'lookback_hours must be a positive integer, got ' ~ value) }}
{%- endmacro %}

{# Errors in the arguments of the parquet_direct_registration test, checked before its SQL is built. #}
{% macro parquet_direct__test_argument_errors(lookback_hours, ignore_file_patterns) -%}
  {%- set errors = [] -%}
  {%- do dbt_snowflake_parquet_direct.parquet_direct__check_string_list(errors, ignore_file_patterns, 'ignore_file_patterns') -%}
  {%- if not errors -%}
    {%- do errors.extend(dbt_snowflake_parquet_direct.parquet_direct__ignore_pattern_errors(ignore_file_patterns, 'ignore_file_patterns')) -%}
  {%- endif -%}
  {%- set lookback_error = dbt_snowflake_parquet_direct.parquet_direct__lookback_error(lookback_hours) -%}
  {%- if lookback_error -%}{%- do errors.append(lookback_error) -%}{%- endif -%}
  {{ return(errors) }}
{%- endmacro %}

{#
  The SQL of the parquet_direct_registration test: one row per skipped file in the window, plus one row when the
  auto refresh mode differs from `expect_auto_refresh` or auto refresh is on but not RUNNING.
  ignore_file_patterns use Snowflake regular expressions (POSIX ERE, searched anywhere in the file name).
#}
{% macro parquet_direct__registration_test_sql(relation, lookback_hours, ignore_file_patterns, expect_auto_refresh) -%}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set name = package.parquet_direct__sql_string(relation | string) -%}
  {%- set failed = [] -%}
  {%- for status, category in package.parquet_direct__status_categories().items() if category == 'failed' -%}
    {%- do failed.append(package.parquet_direct__sql_string(status)) -%}
  {%- endfor -%}
  {%- set ignore = [] -%}
  {%- for pattern in ignore_file_patterns -%}
    {%- do ignore.append('\n  and regexp_instr(file_name, ' ~ package.parquet_direct__sql_string(pattern) ~ ') = 0') -%}
  {%- endfor -%}
  {%- if expect_auto_refresh -%}
    {%- set unhealthy = "status is null or status:executionState::string <> 'RUNNING'" -%}
  {%- else -%}
    {%- set unhealthy = 'status is not null' -%}
  {%- endif -%}
  {{ return(
    'with history as (\n'
    ~ '    select file_name, operation_status, message, operation_time,\n'
    ~ '        row_number() over (partition by file_name order by operation_time desc) as recency_rank\n'
    ~ '    from table(' ~ relation.include(schema=false, identifier=false) ~ '.information_schema.iceberg_table_file_registration_history(\n'
    ~ '        table_name => ' ~ name ~ ',\n'
    ~ "        start_time => dateadd('hour', -" ~ lookback_hours ~ ', current_timestamp())\n'
    ~ '    ))\n'
    ~ '),\n\n'
    ~ 'auto_refresh as (\n'
    ~ '    select raw, try_parse_json(raw) as status\n'
    ~ '    from (select system$parquet_direct_auto_refresh_status(' ~ name ~ ') as raw)\n'
    ~ ')\n\n'
    ~ "select 'skipped_file' as failure, file_name as detail, operation_status || ': ' || coalesce(message, '') as message, operation_time\n"
    ~ 'from history\n'
    ~ 'where recency_rank = 1\n'
    ~ '  and operation_status in (' ~ failed | join(', ') ~ ')' ~ ignore | join('') ~ '\n\n'
    ~ 'union all\n\n'
    ~ "select 'auto_refresh_" ~ ('unhealthy' if expect_auto_refresh else 'unexpected') ~ "' as failure,\n"
    ~ "    coalesce(status:executionState::string, 'NOT_ENABLED') as detail,\n"
    ~ '    coalesce(status:channelErrorMessage::string, status:invalidExecutionStateReason::string, raw) as message,\n'
    ~ '    null as operation_time\n'
    ~ 'from auto_refresh\n'
    ~ 'where ' ~ unhealthy
  ) }}
{%- endmacro %}

{#
  The auto refresh mode the parquet_direct_registration test expects. An explicit `auto_refresh` must be a boolean.
  Otherwise it comes from the one model whose relation_name is exactly `relation_name`; quoted names keep their
  case, so "Sales" and "SALES" stay distinct. Returns {'value': bool or none, 'error': string or none}.
#}
{% macro parquet_direct__expected_auto_refresh(auto_refresh, relation_name, nodes) -%}
  {%- if auto_refresh is not none -%}
    {%- if auto_refresh is not boolean -%}
      {{ return({'value': none, 'error': 'auto_refresh must be true, false, or omitted, got ' ~ auto_refresh}) }}
    {%- endif -%}
    {{ return({'value': auto_refresh, 'error': none}) }}
  {%- endif -%}
  {%- set matches = [] -%}
  {%- for node in nodes if node.resource_type == 'model' and node.relation_name == relation_name -%}
    {%- do matches.append(node) -%}
  {%- endfor -%}
  {%- if matches | length != 1 -%}
    {{ return({'value': none, 'error': 'found ' ~ matches | length ~ ' models for ' ~ relation_name ~ '; set the auto_refresh argument'}) }}
  {%- endif -%}
  {{ return({'value': ((matches[0].config.meta or {}).get('parquet_direct') or {}).get('auto_refresh') == true, 'error': none}) }}
{%- endmacro %}

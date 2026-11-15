{% macro test_refresh_statements() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set relation = test_relation() -%}
  {%- set refresh = package.parquet_direct__refresh_statements -%}
  {%- set normalize = package.parquet_direct__normalize -%}

  {%- do assert_equal('one statement per refresh path',
    refresh(relation, normalize({'base_location': 'a/', 'refresh_paths': ['d=1/', "it's.parquet"]})),
    ["alter iceberg table DB.SCH.T refresh 'd=1/'", "alter iceberg table DB.SCH.T refresh 'it\\'s.parquet'"]) -%}
  {%- do assert_equal('empty refresh_paths are skipped by default', refresh(relation, normalize({'base_location': 'a/'})), []) -%}
  {%- do assert_equal('empty refresh_paths refresh everything when not skipped',
    refresh(relation, normalize({'base_location': 'a/', 'skip_refresh_if_refresh_paths_empty': false})),
    ['alter iceberg table DB.SCH.T refresh']) -%}
  {%- do assert_equal('no manual refresh under auto refresh', refresh(relation, normalize({'base_location': 'a/', 'auto_refresh': true})), []) -%}
{% endmacro %}

{% macro test_history() %}
  {%- set package = dbt_snowflake_parquet_direct -%}

  {%- set expected_sql -%}
select file_name, operation_status, message, operation_time
from table(DB.information_schema.iceberg_table_file_registration_history(
    table_name => 'DB.SCH.T',
    start_time => to_timestamp_ltz('2026-11-15T00:00:00+00:00')
))
order by operation_time
  {%- endset -%}
  {%- do assert_equal('history sql', package.parquet_direct__history_sql(test_relation(), "to_timestamp_ltz('2026-11-15T00:00:00+00:00')"), expected_sql | trim) -%}

  {%- set rows = [
    {'file_name': 'sales/d=1/a.parquet', 'operation_status': 'REGISTERED_NEW', 'message': 'File registered successfully.', 'operation_time': '01'},
    {'file_name': 'sales/d=1/bad.parquet', 'operation_status': 'SKIPPED', 'message': 'Invalid: File is too small for Parquet.', 'operation_time': '02'},
    {'file_name': 'sales/d=2/fixed.parquet', 'operation_status': 'SKIPPED', 'message': 'Column registration failed.', 'operation_time': '03'},
    {'file_name': 'sales/d=2/fixed.parquet', 'operation_status': 'REGISTERED_NEW', 'message': 'File registered successfully.', 'operation_time': '04'},
    {'file_name': 'sales/d=2/gone.parquet', 'operation_status': 'SKIPPED', 'message': 'Column registration failed.', 'operation_time': '05'},
    {'file_name': 'sales/d=2/gone.parquet', 'operation_status': 'UNREGISTERED', 'message': 'File unregistered successfully.', 'operation_time': '06'},
    {'file_name': 'sales/d=2/_tmp/x.parquet', 'operation_status': 'SKIPPED', 'message': 'Invalid', 'operation_time': '07'},
    {'file_name': 'sales/d=1/failed.parquet', 'operation_status': 'REGISTER_FAILED', 'message': 'Column registration failed.', 'operation_time': '08'},
    {'file_name': 'sales/d=1/stuck.parquet', 'operation_status': 'UNREGISTER_FAILED', 'message': 'Access denied', 'operation_time': '09'},
    {'file_name': 'sales/d=1/retried.parquet', 'operation_status': 'REGISTER_FAILED', 'message': 'x', 'operation_time': '10'},
    {'file_name': 'sales/d=1/retried.parquet', 'operation_status': 'REGISTERED_UPDATE', 'message': 'ok', 'operation_time': '11'},
    {'file_name': 'sales/d=1/a.parquet', 'operation_status': 'REGISTER_SKIPPED', 'message': 'already registered', 'operation_time': '12'},
    {'file_name': 'sales/d=1/other.parquet', 'operation_status': 'REGISTER_SKIPPED_PATTERN_MISMATCH', 'message': 'no match', 'operation_time': '13'},
  ] -%}
  {%- set summary = package.parquet_direct__summarize_history(rows, 'sales/', ['d=1/', '/d=2/', 'd=3/'], ['_tmp/']) -%}
  {%- do assert_equal('event counts', summary.counts, {'registered': 3, 'unregistered': 1, 'failed': 7}) -%}
  {%- do assert_equal('only unresolved failures are skipped files', summary.skipped | map(attribute='file_name') | list, ['sales/d=1/bad.parquet', 'sales/d=1/failed.parquet', 'sales/d=1/stuck.parquet']) -%}
  {%- do assert_equal('ignored skipped files', summary.ignored | map(attribute='file_name') | list, ['sales/d=2/_tmp/x.parquet']) -%}
  {%- do assert_equal('counts per refresh path', summary.by_path, {
    'd=1/': {'registered': 2, 'unregistered': 0, 'failed': 4},
    '/d=2/': {'registered': 1, 'unregistered': 1, 'failed': 3},
    'd=3/': {'registered': 0, 'unregistered': 0, 'failed': 0},
  }) -%}
  {%- set anchored = package.parquet_direct__summarize_history(rows, 'sales/', [], ['^sales/d=[0-9]+/(_tmp|bad)']) -%}
  {%- do assert_equal('anchored portable pattern', anchored.ignored | map(attribute='file_name') | list, ['sales/d=1/bad.parquet', 'sales/d=2/_tmp/x.parquet']) -%}
  {%- do assert_equal('empty history', package.parquet_direct__summarize_history([], 'sales/').skipped, []) -%}

  {%- set skipped = [
    {'file_name': 'a.parquet', 'operation_status': 'SKIPPED', 'message': 'm1'}, {'file_name': 'b.parquet', 'operation_status': 'REGISTER_FAILED', 'message': 'm2'}, {'file_name': 'c.parquet', 'operation_status': 'SKIPPED', 'message': 'm3'},
  ] -%}
  {%- do assert_equal('skipped message is truncated', package.parquet_direct__skipped_message(test_relation(), skipped, 2),
    "3 skipped file(s) in DB.SCH.T failed to register or unregister, so the table does not match storage:\n  - a.parquet (SKIPPED): m1\n  - b.parquet (REGISTER_FAILED): m2\n  ... and 1 more") -%}

  {%- set running = package.parquet_direct__parse_auto_refresh_status('{"executionState": "RUNNING", "pendingFileCount": 1, "notificationChannelName": "arn"}') -%}
  {%- do assert_equal('auto refresh status', running, {'enabled': true, 'execution_state': 'RUNNING', 'pending_file_count': 1, 'channel_error': none, 'reason': none}) -%}
  {%- set disabled = package.parquet_direct__parse_auto_refresh_status("Auto refresh is not enabled for table: 'DB.SCH.T'") -%}
  {%- do assert_equal('auto refresh disabled', disabled.enabled, false) -%}
{% endmacro %}

{% macro test_registration_findings() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set relation = test_relation() -%}
  {%- set manual = package.parquet_direct__normalize({'base_location': 'sales/', 'refresh_paths': ['d=1/']}) -%}
  {%- set auto = package.parquet_direct__normalize({'base_location': 'sales/', 'auto_refresh': true}) -%}
  {%- set lenient = package.parquet_direct__normalize({'base_location': 'sales/', 'auto_refresh': true, 'registration_check': {'on_skipped': 'warn', 'on_unhealthy': 'ignore'}}) -%}
  {%- set skipped = [{'file_name': 'sales/d=1/bad.parquet', 'operation_status': 'SKIPPED', 'message': 'Invalid', 'operation_time': '01'}] -%}
  {%- set running = '{"executionState": "RUNNING", "pendingFileCount": 0}' -%}
  {%- set stalled = '{"executionState": "STALLED", "channelErrorMessage": "Access denied"}' -%}

  {%- set cases = [
    ('clean manual refresh', manual, none, [], []),
    ('skipped files fail by default', manual, none, skipped, ['error']),
    ('auto refresh running', auto, running, [], []),
    ('auto refresh stalled fails by default', auto, stalled, [], ['error']),
    ('auto refresh turned off fails by default', auto, "Auto refresh is not enabled for table: 'DB.SCH.T'", [], ['error']),
    ('severities follow registration_check', lenient, stalled, skipped, ['ignore', 'warn']),
  ] -%}
  {%- for name, declaration, status, rows, expected in cases -%}
    {%- set findings = package.parquet_direct__registration_findings(relation, declaration, status, rows).findings -%}
    {%- do assert_equal(name, findings | rejectattr('severity', 'equalto', 'info') | map(attribute='severity') | list, expected) -%}
  {%- endfor -%}

  {%- set stalled_findings = package.parquet_direct__registration_findings(relation, auto, stalled, []).findings -%}
  {%- do assert_equal('the unhealthy message names the state and the reason', 'STALLED: Access denied' in stalled_findings[0].message, true) -%}
  {%- set path_findings = package.parquet_direct__registration_findings(relation, manual, none, skipped).findings -%}
  {%- do assert_equal('per-path counts are reported', path_findings[0], {'severity': 'info', 'message': 'Refresh path d=1/: 0 registered, 0 unregistered, 1 failed'}) -%}
{% endmacro %}

{% macro test_registration_test_sql() %}
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set auto = package.parquet_direct__registration_test_sql(test_relation(), 24, ['_tmp/'], true) -%}
  {%- set manual = package.parquet_direct__registration_test_sql(test_relation(), 6, [], false) -%}
  {%- do assert_equal('every failed status is reported',
    "operation_status in ('SKIPPED', 'REGISTER_FAILED', 'UNREGISTER_FAILED')" in auto, true) -%}
  {%- do assert_equal('ignored files are excluded', "regexp_instr(file_name, '_tmp/') = 0" in auto, true) -%}
  {%- do assert_equal('auto refresh that is off or not RUNNING fails',
    auto.endswith("where status is null or status:executionState::string <> 'RUNNING'"), true) -%}
  {%- do assert_equal('auto refresh that is on for a manual table fails', manual.endswith('where status is not null'), true) -%}
  {%- do assert_equal('lookback window', "dateadd('hour', -6, current_timestamp())" in manual, true) -%}

  {%- set cases = [(1, none), (24, none), (0, 'lookback_hours must be a positive integer, got 0'), (-3, 'lookback_hours must be a positive integer, got -3'), ('6', 'lookback_hours must be a positive integer, got 6'), (true, 'lookback_hours must be a positive integer, got True')] -%}
  {%- for value, expected in cases -%}
    {%- do assert_equal('lookback_hours ' ~ value, package.parquet_direct__lookback_error(value), expected) -%}
  {%- endfor -%}
{% endmacro %}

{% macro test_expected_auto_refresh() %}
  {%- set expected = dbt_snowflake_parquet_direct.parquet_direct__expected_auto_refresh -%}
  {%- set nodes = [
    {'resource_type': 'model', 'relation_name': 'DB.SCH."Sales"', 'config': {'meta': {'parquet_direct': {'auto_refresh': true}}}},
    {'resource_type': 'model', 'relation_name': 'DB.SCH."SALES"', 'config': {'meta': {'parquet_direct': {}}}},
    {'resource_type': 'test', 'relation_name': 'DB.SCH.T', 'config': {'meta': {}}},
    {'resource_type': 'model', 'relation_name': 'DB.SCH.TWIN', 'config': {'meta': {}}},
    {'resource_type': 'model', 'relation_name': 'DB.SCH.TWIN', 'config': {'meta': {}}},
  ] -%}
  {%- do assert_equal('quoted name keeps its case (auto)', expected(none, 'DB.SCH."Sales"', nodes), {'value': true, 'error': none}) -%}
  {%- do assert_equal('quoted name keeps its case (manual)', expected(none, 'DB.SCH."SALES"', nodes), {'value': false, 'error': none}) -%}
  {%- do assert_equal('explicit argument wins', expected(false, 'DB.SCH."Sales"', nodes), {'value': false, 'error': none}) -%}
  {%- do assert_equal('string argument is rejected', expected('false', 'DB.SCH."Sales"', nodes).error, 'auto_refresh must be true, false, or omitted, got false') -%}
  {%- do assert_equal('unknown relation needs the argument', expected(none, 'DB.SCH.T', nodes).error, 'found 0 models for DB.SCH.T; set the auto_refresh argument') -%}
  {%- do assert_equal('ambiguous relation needs the argument', expected(none, 'DB.SCH.TWIN', nodes).value, none) -%}
{% endmacro %}

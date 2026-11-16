{#
  Declares a Snowflake Parquet Direct table. See docs/design.md.
  Statements run outside transactions because ALTER ICEBERG TABLE ... REFRESH cannot run inside one.
#}
{% materialization parquet_direct, adapter='snowflake' %}
  {%- set raw_options = config.meta_get('parquet_direct', {}) -%}
  {%- set dbt_config = {
    'cluster_by': config.get('cluster_by'),
    'unique_key': config.get('unique_key'),
    'incremental_strategy': config.get('incremental_strategy'),
    'contract': config.get('contract'),
  } -%}
  {%- set errors = dbt_snowflake_parquet_direct.parquet_direct__validate(raw_options, dbt_config, sql) -%}
  {%- if errors -%}
    {{ exceptions.raise_compiler_error('Invalid parquet_direct configuration for ' ~ model.unique_id ~ ':\n  - ' ~ errors | join('\n  - ')) }}
  {%- endif -%}
  {%- set declaration = dbt_snowflake_parquet_direct.parquet_direct__normalize(raw_options, model.description, model.columns, config.persist_relation_docs(), config.persist_column_docs()) -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set full_refresh_mode = should_full_refresh() -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set catalog = dbt_snowflake_parquet_direct.parquet_direct__get_catalog(target_relation, declaration) -%}
  {%- set state = dbt_snowflake_parquet_direct.parquet_direct__get_state(target_relation) -%}
  {%- set preflight = dbt_snowflake_parquet_direct.parquet_direct__preflight(declaration, catalog, state, full_refresh_mode) -%}
  {%- if preflight.errors -%}
    {{ exceptions.raise_compiler_error('Cannot build ' ~ target_relation ~ ':\n  - ' ~ preflight.errors | join('\n  - ')) }}
  {%- endif -%}
  {%- for warning in preflight.warnings -%}{%- do exceptions.warn(warning) -%}{%- endfor -%}

  {#- Registrations made by CREATE or REFRESH in this run are checked from this point on. -#}
  {%- set started_at = run_query("select to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM') as started_at").columns[0].values()[0] -%}
  {%- set action = {'name': 'unchanged', 'statements': 0} -%}

  {%- if not state.exists or full_refresh_mode -%}
    {%- for statement in dbt_snowflake_parquet_direct.parquet_direct__create_statements(target_relation, declaration, replace=state.exists, copy_grants=state.exists and config.get('copy_grants', false)) -%}
      {%- do run_query(statement) -%}
    {%- endfor -%}
    {%- do action.update({'name': 'replaced' if state.exists else 'created'}) -%}
  {%- else -%}
    {%- set details = dbt_snowflake_parquet_direct.parquet_direct__get_details(target_relation, declaration, state) -%}
    {%- set diff = dbt_snowflake_parquet_direct.parquet_direct__diff(target_relation, declaration, details, catalog) -%}
    {%- for note in diff.notes -%}{%- do log(note, info=true) -%}{%- endfor -%}
    {%- set mode = (config.get('on_configuration_change') or 'apply') | string | lower -%}
    {%- if diff.changes and mode == 'fail' -%}
      {{ exceptions.raise_compiler_error('on_configuration_change is fail and ' ~ target_relation ~ ' differs from its declaration:\n  - ' ~ diff.changes | map(attribute='description') | join('\n  - ')) }}
    {%- elif diff.changes and mode == 'continue' -%}
      {%- for change in diff.changes -%}
        {%- do exceptions.warn('Not applied to ' ~ target_relation ~ ' (on_configuration_change: continue): ' ~ change.description) -%}
      {%- endfor -%}
      {#- Refresh and check the table as it is, because an unapplied auto_refresh change decides whether manual refresh is allowed. -#}
      {%- do declaration.update({'auto_refresh': details.auto_refresh}) -%}
    {%- elif diff.changes | rejectattr('alterable') | list -%}
      {{ exceptions.raise_compiler_error(dbt_snowflake_parquet_direct.parquet_direct__recreation_error(target_relation, diff.changes, model.name, config.get('full_refresh') == false)) }}
    {%- else -%}
      {%- for change in diff.changes -%}
        {%- for statement in change.statements -%}
          {%- do run_query(statement) -%}
          {%- do action.update({'statements': action.statements + 1}) -%}
        {%- endfor -%}
      {%- endfor -%}
      {%- if diff.changes -%}{%- do action.update({'name': 'altered'}) -%}{%- endif -%}
      {%- for statement in diff.reapply -%}{%- do run_query(statement) -%}{%- endfor -%}
      {%- if diff.reapply -%}
        {%- do log('Re-applied ' ~ diff.reapply | length ~ ' masking polic(ies) on ' ~ target_relation ~ ', because Snowflake does not report their USING columns', info=true) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}

  {#- Grants come before refresh so a refresh or registration failure never leaves a new table without them. -#}
  {%- set existing_relation = target_relation if state.exists else none -%}
  {%- do apply_grants(target_relation, config.get('grants'), should_revoke=should_revoke(existing_relation, full_refresh_mode=full_refresh_mode)) -%}

  {%- for statement in dbt_snowflake_parquet_direct.parquet_direct__refresh_statements(target_relation, declaration) -%}
    {%- do run_query(statement) -%}
  {%- endfor -%}

  {%- set since_sql = "dateadd('hour', -" ~ declaration.registration_check.lookback_hours ~ ", current_timestamp())" if declaration.auto_refresh else "to_timestamp_ltz('" ~ started_at ~ "', 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')" -%}
  {%- set summary = dbt_snowflake_parquet_direct.parquet_direct__check_registration(target_relation, declaration, since_sql) -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {%- set message = action.name ~ ('' if action.name != 'altered' else ' (' ~ action.statements ~ ' statements)')
      ~ '; registered ' ~ summary.counts.registered ~ ', unregistered ' ~ summary.counts.unregistered
      ~ ', skipped ' ~ summary.skipped | length -%}
  {%- do store_raw_result(name='main', message=message, code='SUCCESS', rows_affected='0') -%}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

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
  {%- set package = dbt_snowflake_parquet_direct -%}
  {%- set view_declaration = package.parquet_direct__normalize(raw_options, model.description, model.columns, config.persist_relation_docs(), config.persist_column_docs()) -%}
  {#- With previous versions, the model's relation is the combined view and the table is the current table. -#}
  {%- set has_previous_versions = view_declaration.previous_versions | length > 0 -%}
  {%- set declaration = package.parquet_direct__table_declaration(view_declaration) -%}
  {%- set public_relation = this.incorporate(type='view') if has_previous_versions else this.incorporate(type='table') -%}
  {%- set target_relation = package.parquet_direct__current_relation(this) if has_previous_versions else public_relation -%}
  {%- set full_refresh_mode = should_full_refresh() -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set catalog = dbt_snowflake_parquet_direct.parquet_direct__get_catalog(target_relation, declaration) -%}
  {%- set state = dbt_snowflake_parquet_direct.parquet_direct__get_state(target_relation) -%}
  {%- set preflight = dbt_snowflake_parquet_direct.parquet_direct__preflight(declaration, catalog, state, full_refresh_mode) -%}
  {%- set public_state = {'exists': false} -%}
  {%- set previous_selects = [] -%}
  {%- if has_previous_versions -%}
    {%- set public_state = package.parquet_direct__get_state(public_relation) -%}
    {%- set previous = package.parquet_direct__get_previous_versions(this, view_declaration) -%}
    {%- do preflight.errors.extend(package.parquet_direct__versions_preflight(this, view_declaration, public_state, state, previous, not state.exists or full_refresh_mode)) -%}
    {%- if not preflight.errors -%}
      {%- for item in previous -%}
        {%- set select = package.parquet_direct__previous_version_select(view_declaration, item, loop.index0) -%}
        {%- do preflight.errors.extend(select.errors) -%}
        {%- do previous_selects.append(select.sql) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {%- if preflight.errors -%}
    {{ exceptions.raise_compiler_error('Cannot build ' ~ public_relation ~ ':\n  - ' ~ preflight.errors | join('\n  - ')) }}
  {%- endif -%}
  {#- Compiling each previous version's SELECT catches a wrong expression before anything is created. -#}
  {%- for select in previous_selects -%}{%- do run_query('explain using text ' ~ select) -%}{%- endfor -%}
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

  {#-
    The combined view and grants come before refresh, so a refresh or registration failure never leaves the model
    without them. Grants go to the model's relation; the current table behind a combined view gets none.
  -#}
  {%- if has_previous_versions -%}
    {#- The view always copies grants, so existing grants carry over like an in-place change. -#}
    {%- do run_query(package.parquet_direct__view_statement(public_relation, view_declaration, previous_selects)) -%}
    {%- set existing_relation = public_relation if public_state.exists else none -%}
    {%- do apply_grants(public_relation, config.get('grants'), should_revoke=should_revoke(existing_relation, full_refresh_mode=false)) -%}
  {%- else -%}
    {%- set existing_relation = target_relation if state.exists else none -%}
    {%- do apply_grants(target_relation, config.get('grants'), should_revoke=should_revoke(existing_relation, full_refresh_mode=full_refresh_mode)) -%}
  {%- endif -%}

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

  {{ return({'relations': [public_relation]}) }}
{% endmaterialization %}

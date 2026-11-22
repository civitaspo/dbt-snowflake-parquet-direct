{# Files whose is_active changed from an integer to a boolean to a string, in separate or shared directories. pytest drives the move to previous_versions with vars. #}
{%- set is_active = {'name': 'is_active', 'data_type': var('is_active_type', 'long'), 'comment': 'whether the customer is active'} -%}
{%- if var('is_active_masking_policy', none) -%}{%- do is_active.update({'masking_policy': {'name': var('is_active_masking_policy')}}) -%}{%- endif -%}
{{ config(
    materialized='parquet_direct',
    alias=var('versioned_alias', 'versioned_table'),
    on_configuration_change=var('on_configuration_change', 'apply'),
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('base_prefix') ~ '/' ~ var('versioned_directory', 'versioned_integer') ~ '/',
        'columns': [
            {'name': 'id', 'data_type': 'long'},
            is_active,
        ],
        'partition_columns': [{'name': 'd', 'data_type': 'varchar', 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)"}],
        'auto_refresh': var('versioned_auto_refresh', false),
        'refresh_paths': var('refresh_paths', []),
        'previous_versions': var('previous_versions', []),
        'comment': 'customers',
    }},
) }}

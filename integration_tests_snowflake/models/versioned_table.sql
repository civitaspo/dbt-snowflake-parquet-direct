{# Files whose is_active changed from an integer to a boolean. pytest drives the move to previous_versions with vars. #}
{{ config(
    materialized='parquet_direct',
    on_configuration_change=var('on_configuration_change', 'apply'),
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('base_prefix') ~ '/versioned/',
        'columns': [
            {'name': 'id', 'data_type': 'long'},
            {'name': 'is_active', 'data_type': var('is_active_type', 'long'), 'comment': 'whether the customer is active'},
        ],
        'partition_columns': [{'name': 'd', 'data_type': 'varchar', 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)"}],
        'refresh_on_create': var('refresh_on_create', true),
        'refresh_paths': var('refresh_paths', []),
        'previous_versions': var('previous_versions', []),
        'comment': 'customers',
    }},
) }}

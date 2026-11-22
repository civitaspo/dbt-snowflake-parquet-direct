{# Manual refresh on a TABLE_FORMAT = NONE catalog. pytest varies the vars to drive each scenario. #}
{{ config(
    materialized='parquet_direct',
    full_refresh=var('full_refresh_setting', none),
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('base_prefix') ~ '/' ~ var('data_directory', 'manual') ~ '/',
        'columns': [
            {'name': 'id', 'data_type': var('id_type', 'int'), 'comment': 'identifier'},
            {'name': 'message', 'data_type': 'string'},
        ],
        'virtual_columns': [
            {'name': 'source_file', 'data_type': 'varchar', 'expression': 'METADATA$FILENAME'},
            {'name': 'file_name', 'data_type': 'varchar', 'expression': "SPLIT_PART(METADATA$FILENAME, '/', -1)::VARCHAR"},
        ],
        'partition_columns': [{'name': 'd', 'data_type': 'varchar', 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)"}],
        'refresh_on_create': var('refresh_on_create', true),
        'refresh_paths': var('refresh_paths', []),
        'comment': var('table_comment', 'v1'),
        'registration_check': {'on_skipped': var('on_skipped', 'error'), 'ignore_file_patterns': var('ignore_file_patterns', [])},
    }},
) }}

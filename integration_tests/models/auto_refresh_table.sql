{# Auto refresh. Needs S3 event notifications to Snowflake, so it is disabled unless auto_refresh_table_enabled is true. #}
{{ config(
    materialized='parquet_direct',
    enabled=var('auto_refresh_table_enabled', false),
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('base_prefix') ~ '/auto/',
        'columns': [{'name': 'id', 'data_type': 'int'}, {'name': 'message', 'data_type': 'string'}],
        'auto_refresh': true,
        'refresh_on_create': false,
        'registration_check': {'lookback_hours': 1},
    }},
) }}

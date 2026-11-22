{{ config(
    enabled=var('inference_test_enabled', false),
    alias=var('inference_test_alias', 'inferred_table'),
    materialized='parquet_direct',
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('base_prefix') ~ '/inferred/',
        'partition_columns': [{'name': 'd', 'data_type': 'varchar', 'expression': "SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'd=', 2), '/', 1)"}],
        'refresh_on_create': false,
        'refresh_paths': var('refresh_paths', []),
        'schema_inference_file_pattern_on_create': var('inference_pattern', none),
    }},
) }}

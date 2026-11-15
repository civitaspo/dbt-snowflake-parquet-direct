{# TABLE_FORMAT = HIVE catalog without declared columns: columns and the partition column are inferred. #}
{{ config(
    materialized='parquet_direct',
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_hive'),
        'base_location': var('base_prefix') ~ '/hive/',
    }},
) }}

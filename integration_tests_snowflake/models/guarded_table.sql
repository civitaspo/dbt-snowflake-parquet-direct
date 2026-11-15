{#
  Failure scenarios, enabled one at a time by pytest:
  - An invalid base_location must fail static validation before any SQL runs.
  - occupied_table: the name is taken by a view, which parquet_direct must never replace.
#}
{{ config(
    materialized='parquet_direct',
    enabled=var('guarded_table_enabled', false),
    alias=var('guarded_alias', 'guarded_table_enabled'),
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('guarded_base_location', var('base_prefix') ~ '/guarded/'),
        'columns': [{'name': 'id', 'data_type': 'int'}],
    }},
) }}

{# Governance settings and constraints. pytest creates the tag and policies in the test schema first. #}
{{ config(
    materialized='parquet_direct',
    enabled=var('governed_table_enabled', false),
    meta={'parquet_direct': {
        'external_volume': var('external_volume'),
        'catalog': var('catalog_none'),
        'base_location': var('base_prefix') ~ '/governed/',
        'columns': [
            {'name': 'id', 'data_type': 'int', 'constraints': [{'type': 'primary_key', 'rely': true}]},
            {'name': 'message', 'data_type': 'string', 'masking_policy': var('masking_policy_name', ''), 'tag': {var('tag_name', ''): 'column'}},
        ],
        'row_access_policy': {'name': var('row_access_policy_name', ''), 'on': ['id']},
        'tag': {var('tag_name', ''): var('tag_value', 'v1')},
    }},
) }}

-- models/staging/stg_fields.sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}

with raw as (
    select *
    from {{ source('crm_raw', 'fields') }}
),

cleaned as (
    select
        id,
        lower(trim(field_key)) as field_key,
        lower(trim(name)) as name,
        field_value_options,
        MD5(
            concat(
                ifNull(lower(trim(field_key)), ''),
                '|',
                ifNull(lower(trim(name)), '')
            )
        ) as row_hash,
        now() as load_timestamp
    from raw
    where id is not null
),

final as (
    select c.*
    from cleaned c

    {% if is_incremental() %}
        left join {{ this }} t
            on c.id = t.id
        where
            t.id is null
            or c.row_hash != t.row_hash
    {% endif %}
)

select
    id,
    field_key,
    name,
    field_value_options,
    row_hash,
    load_timestamp
from final

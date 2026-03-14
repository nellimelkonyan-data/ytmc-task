-- models/staging/stg_activity_types.sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}

with raw as (
    select * 
    from {{ source('crm_raw', 'activity_types') }}
),

cleaned as (
    select
        id,
        lower(trim(name)) as name,
        active,
        lower(trim(type)) as type,
        MD5(
            concat(
                ifNull(lower(trim(name)), ''), '|',
                ifNull(cast(active as String), ''), '|',
                ifNull(lower(trim(type)), '')
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
    name,
    active,
    type,
    row_hash,
    load_timestamp
from final

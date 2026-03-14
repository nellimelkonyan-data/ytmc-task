{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['deal_id', 'change_time', 'changed_field_key', 'stage_id']
) }}

with src as (

    select
        deal_id,
        cast(change_time as Nullable(DateTime)) as change_time,
        toUInt32(new_value) as stage_id,
        changed_field_key,
        now() as load_timestamp
    from {{ source('crm_raw', 'deal_changes') }}
    where changed_field_key = 'stage_id'
      and deal_id is not null
      and change_time is not null
      and new_value is not null

),

filtered as (

    select *
    from src

    {% if is_incremental() %}
        where change_time >= (
            select coalesce(max(change_time), toDateTime('1900-01-01 00:00:00'))
            from {{ this }}
        )
    {% endif %}

)

select
    deal_id,
    change_time,
    stage_id,
    changed_field_key,
    load_timestamp
from filtered

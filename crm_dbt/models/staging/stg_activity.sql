{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='activity_id'
) }}

with ranked as (

    select
        activity_id,
        type,
        assigned_to_user,
        deal_id,
        done,
        due_to,
        row_number() over (
            partition by activity_id
            order by due_to desc
        ) as rn
    from {{ source('crm_raw', 'activity') }}
    where activity_id is not null

),

latest as (

    select
        activity_id,
        type,
        assigned_to_user,
        deal_id,
        done,
        due_to,
        MD5(
            concat(
                ifNull(cast(activity_id as String), ''), '|',
                ifNull(cast(type as String), ''), '|',
                ifNull(cast(assigned_to_user as String), ''), '|',
                ifNull(cast(deal_id as String), ''), '|',
                ifNull(cast(done as String), ''), '|',
                ifNull(cast(due_to as String), '')
            )
        ) as row_hash,
        now() as load_timestamp
    from ranked
    where rn = 1

),

final as (

    select l.*
    from latest l

    {% if is_incremental() %}
        left join {{ this }} t
            on l.activity_id = t.activity_id
        where
            t.activity_id is null
            or l.row_hash != t.row_hash
    {% endif %}

)

select
    activity_id,
    type,
    assigned_to_user,
    deal_id,
    done,
    due_to,
    row_hash,
    load_timestamp
from final
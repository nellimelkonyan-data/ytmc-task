{{ config( materialized='view') }}

select
    activity_id,
    type,
    assigned_to_user as user_id,
    deal_id,
    done,
    due_to
from {{ source('crm_staging', 'stg_activity') }}
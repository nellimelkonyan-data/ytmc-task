{{ config(materialized='view') }}

select
    deal_id,
    change_time,
    stage_id
from {{ source('crm_staging', 'stg_deal_changes') }}
where changed_field_key = 'stage_id'
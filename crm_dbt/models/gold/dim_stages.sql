{{ config(materialized='table') }}

select
    stage_id,
    stage_name
from {{ source('crm_staging', 'stg_stages') }}
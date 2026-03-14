-- models/staging/stg_stages.sql
{{ config(materialized='table') }}

with raw as (
    select * from {{ source('crm_raw', 'stages') }}
)

select
    stage_id,
    lower(stage_name) as stage_name,
    now() as load_timestamp
from raw
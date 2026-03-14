{{ config(materialized='view') }}

select
    id as user_id,
    name,
    email,
    modified
from {{ source('crm_staging', 'stg_users') }}
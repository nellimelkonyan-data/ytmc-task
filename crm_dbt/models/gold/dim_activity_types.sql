-- models/gold/dim_activity_types.sql
{{ config(materialized='view') }}

select
    id as activity_type_id,
    name as activity_type_name,
    active,
    type as activity_category
from {{ source('crm_staging', 'stg_activity_types') }}
-- models/staging/stg_users.sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}

with raw as (
    select * from {{ source('crm_raw', 'users') }}
),

cleaned as (
    select
        id,
        lower(trim(name)) as name,
        lower(trim(email)) as email,
        cast(modified as Nullable(DateTime)) as modified,
        now() as load_timestamp
    from raw
    where id is not null
),

filtered as (
    select *
    from cleaned

    {% if is_incremental() %}
        where modified >= (
            select coalesce(max(modified), toDateTime('1900-01-01 00:00:00'))
            from {{ this }}
        )
    {% endif %}
)

select *
from filtered

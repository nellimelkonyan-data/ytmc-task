-- models/staging/stg_field_options.sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['field_id', 'option_id']
) }}

with fields as (

    select
        id as field_id,
        lower(trim(field_key)) as field_key,
        lower(trim(name)) as field_name,
        ifNull(field_value_options, '[]') as field_value_options
    from {{ source('crm_raw', 'fields') }}
    where id is not null

),

exploded as (

    select
        field_id,
        field_key,
        field_name,
        arrayJoin(JSONExtractArrayRaw(field_value_options)) as option_json
    from fields

),

final as (

    select
        field_id,
        field_key,
        field_name,
        JSONExtractString(option_json, 'id') as option_id,
        JSONExtractString(option_json, 'label') as option_label,
        now() as load_timestamp
    from exploded

)

select *
from final

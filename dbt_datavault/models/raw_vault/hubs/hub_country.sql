-- depends_on: {{ ref('stg_customers') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with staged as (
    select distinct
        hk_country,
        country,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select
    hk_country,
    country,
    load_datetime,
    record_source
from staged

{% if is_incremental() %}
where hk_country not in (select hk_country from {{ this }})
{% endif %}

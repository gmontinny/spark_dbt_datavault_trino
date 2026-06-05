-- depends_on: {{ ref('stg_customers') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with staged as (
    select distinct
        hk_customer_country,
        hk_customer,
        hk_country,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select * from staged

{% if is_incremental() %}
where hk_customer_country not in (select hk_customer_country from {{ this }})
{% endif %}

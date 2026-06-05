-- depends_on: {{ ref('stg_customers') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with staged as (
    select distinct
        hk_customer_category,
        hk_customer,
        hk_category,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select * from staged

{% if is_incremental() %}
where hk_customer_category not in (select hk_customer_category from {{ this }})
{% endif %}

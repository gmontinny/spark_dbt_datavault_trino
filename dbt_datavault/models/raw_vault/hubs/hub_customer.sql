-- depends_on: {{ ref('stg_customers') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with staged as (
    select distinct
        hk_customer,
        customer_id,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select
    hk_customer,
    customer_id,
    load_datetime,
    record_source
from staged

{% if is_incremental() %}
where hk_customer not in (select hk_customer from {{ this }})
{% endif %}

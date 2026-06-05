-- depends_on: {{ ref('stg_customers') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with staged as (
    select
        hk_customer,
        hd_customer_details,
        age,
        gender,
        income_bracket,
        loyalty_tier,
        device_type,
        preferred_payment_method,
        newsletter_subscribed,
        referral_source,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select * from staged s

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.hk_customer = s.hk_customer
      and t.hd_customer_details = s.hd_customer_details
)
{% endif %}

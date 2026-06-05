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
        hd_customer_metrics,
        total_orders,
        total_spent_usd,
        avg_order_value_usd,
        discount_usage_rate,
        return_rate,
        satisfaction_score,
        customer_lifetime_days,
        days_since_last_purchase,
        avg_session_duration_min,
        avg_pages_per_session,
        churn,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select * from staged s

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.hk_customer = s.hk_customer
      and t.hd_customer_metrics = s.hd_customer_metrics
)
{% endif %}

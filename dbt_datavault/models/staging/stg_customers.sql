{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from hive.raw_vault.raw_customers
),

staged as (
    select
        -- Business Keys
        customer_id,
        country,
        preferred_category,

        -- Hash Keys (Data Vault pattern)
        cast(to_hex(sha256(cast(customer_id as varbinary))) as varchar) as hk_customer,
        cast(to_hex(sha256(cast(country as varbinary))) as varchar) as hk_country,
        cast(to_hex(sha256(cast(preferred_category as varbinary))) as varchar) as hk_category,
        cast(to_hex(sha256(cast(concat(customer_id, '||', country) as varbinary))) as varchar) as hk_customer_country,
        cast(to_hex(sha256(cast(concat(customer_id, '||', preferred_category) as varbinary))) as varchar) as hk_customer_category,

        -- Hash Diff (detectar mudanças nos satellites)
        cast(to_hex(sha256(cast(concat(
            coalesce(cast(age as varchar), ''),
            coalesce(gender, ''),
            coalesce(income_bracket, ''),
            coalesce(loyalty_tier, ''),
            coalesce(device_type, ''),
            coalesce(preferred_payment_method, '')
        ) as varbinary))) as varchar) as hd_customer_details,

        cast(to_hex(sha256(cast(concat(
            coalesce(cast(total_orders as varchar), ''),
            coalesce(cast(total_spent_usd as varchar), ''),
            coalesce(cast(avg_order_value_usd as varchar), ''),
            coalesce(cast(discount_usage_rate as varchar), ''),
            coalesce(cast(return_rate as varchar), ''),
            coalesce(cast(satisfaction_score as varchar), '')
        ) as varbinary))) as varchar) as hd_customer_metrics,

        -- Attributes
        age,
        gender,
        region,
        income_bracket,
        signup_date,
        last_purchase_date,
        customer_lifetime_days,
        days_since_last_purchase,
        total_orders,
        total_spent_usd,
        avg_order_value_usd,
        preferred_payment_method,
        device_type,
        loyalty_tier,
        discount_usage_rate,
        return_rate,
        newsletter_subscribed,
        referral_source,
        avg_session_duration_min,
        avg_pages_per_session,
        satisfaction_score,
        churn,

        -- Metadata
        load_datetime,
        record_source
    from source
)

select * from staged

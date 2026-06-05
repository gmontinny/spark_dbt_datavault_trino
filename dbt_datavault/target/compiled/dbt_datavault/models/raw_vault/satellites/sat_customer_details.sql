-- depends_on: "hive"."raw_vault"."stg_customers"



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
    from "hive"."raw_vault"."stg_customers"
)

select * from staged s


where not exists (
    select 1 from "hive"."raw_vault"."sat_customer_details" t
    where t.hk_customer = s.hk_customer
      and t.hd_customer_details = s.hd_customer_details
)

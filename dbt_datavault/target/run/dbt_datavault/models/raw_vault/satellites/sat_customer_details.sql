insert into "hive"."raw_vault"."sat_customer_details" ("hk_customer", "hd_customer_details", "age", "gender", "income_bracket", "loyalty_tier", "device_type", "preferred_payment_method", "newsletter_subscribed", "referral_source", "load_datetime", "record_source")
    (
        select "hk_customer", "hd_customer_details", "age", "gender", "income_bracket", "loyalty_tier", "device_type", "preferred_payment_method", "newsletter_subscribed", "referral_source", "load_datetime", "record_source"
        from "hive"."raw_vault"."sat_customer_details__dbt_tmp"
    )


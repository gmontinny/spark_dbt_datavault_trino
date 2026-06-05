insert into "hive"."raw_vault"."sat_customer_metrics" ("hk_customer", "hd_customer_metrics", "total_orders", "total_spent_usd", "avg_order_value_usd", "discount_usage_rate", "return_rate", "satisfaction_score", "customer_lifetime_days", "days_since_last_purchase", "avg_session_duration_min", "avg_pages_per_session", "churn", "load_datetime", "record_source")
    (
        select "hk_customer", "hd_customer_metrics", "total_orders", "total_spent_usd", "avg_order_value_usd", "discount_usage_rate", "return_rate", "satisfaction_score", "customer_lifetime_days", "days_since_last_purchase", "avg_session_duration_min", "avg_pages_per_session", "churn", "load_datetime", "record_source"
        from "hive"."raw_vault"."sat_customer_metrics__dbt_tmp"
    )


insert into "hive"."raw_vault"."hub_customer" ("hk_customer", "customer_id", "load_datetime", "record_source")
    (
        select "hk_customer", "customer_id", "load_datetime", "record_source"
        from "hive"."raw_vault"."hub_customer__dbt_tmp"
    )


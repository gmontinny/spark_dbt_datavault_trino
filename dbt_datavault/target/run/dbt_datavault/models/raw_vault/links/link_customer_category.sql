insert into "hive"."raw_vault"."link_customer_category" ("hk_customer_category", "hk_customer", "hk_category", "load_datetime", "record_source")
    (
        select "hk_customer_category", "hk_customer", "hk_category", "load_datetime", "record_source"
        from "hive"."raw_vault"."link_customer_category__dbt_tmp"
    )


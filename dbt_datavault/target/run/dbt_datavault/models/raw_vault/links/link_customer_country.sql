insert into "hive"."raw_vault"."link_customer_country" ("hk_customer_country", "hk_customer", "hk_country", "load_datetime", "record_source")
    (
        select "hk_customer_country", "hk_customer", "hk_country", "load_datetime", "record_source"
        from "hive"."raw_vault"."link_customer_country__dbt_tmp"
    )


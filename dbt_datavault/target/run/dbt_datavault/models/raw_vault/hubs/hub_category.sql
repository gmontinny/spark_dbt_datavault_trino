insert into "hive"."raw_vault"."hub_category" ("hk_category", "preferred_category", "load_datetime", "record_source")
    (
        select "hk_category", "preferred_category", "load_datetime", "record_source"
        from "hive"."raw_vault"."hub_category__dbt_tmp"
    )


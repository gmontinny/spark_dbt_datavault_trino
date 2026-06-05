insert into "hive"."raw_vault"."hub_country" ("hk_country", "country", "load_datetime", "record_source")
    (
        select "hk_country", "country", "load_datetime", "record_source"
        from "hive"."raw_vault"."hub_country__dbt_tmp"
    )


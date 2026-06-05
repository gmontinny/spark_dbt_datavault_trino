-- depends_on: "hive"."raw_vault"."stg_customers"



with staged as (
    select distinct
        hk_customer_country,
        hk_customer,
        hk_country,
        load_datetime,
        record_source
    from "hive"."raw_vault"."stg_customers"
)

select * from staged


where hk_customer_country not in (select hk_customer_country from "hive"."raw_vault"."link_customer_country")

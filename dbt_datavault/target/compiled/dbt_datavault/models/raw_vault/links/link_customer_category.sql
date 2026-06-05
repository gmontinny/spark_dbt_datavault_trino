-- depends_on: "hive"."raw_vault"."stg_customers"



with staged as (
    select distinct
        hk_customer_category,
        hk_customer,
        hk_category,
        load_datetime,
        record_source
    from "hive"."raw_vault"."stg_customers"
)

select * from staged


where hk_customer_category not in (select hk_customer_category from "hive"."raw_vault"."link_customer_category")

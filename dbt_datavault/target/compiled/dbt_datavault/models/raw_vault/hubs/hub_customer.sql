-- depends_on: "hive"."raw_vault"."stg_customers"



with staged as (
    select distinct
        hk_customer,
        customer_id,
        load_datetime,
        record_source
    from "hive"."raw_vault"."stg_customers"
)

select
    hk_customer,
    customer_id,
    load_datetime,
    record_source
from staged


where hk_customer not in (select hk_customer from "hive"."raw_vault"."hub_customer")

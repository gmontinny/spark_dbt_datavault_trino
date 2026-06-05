-- depends_on: "hive"."raw_vault"."stg_customers"



with staged as (
    select distinct
        hk_category,
        preferred_category,
        load_datetime,
        record_source
    from "hive"."raw_vault"."stg_customers"
)

select
    hk_category,
    preferred_category,
    load_datetime,
    record_source
from staged


where hk_category not in (select hk_category from "hive"."raw_vault"."hub_category")

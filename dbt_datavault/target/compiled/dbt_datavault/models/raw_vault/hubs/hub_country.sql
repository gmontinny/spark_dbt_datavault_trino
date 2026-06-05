-- depends_on: "hive"."raw_vault"."stg_customers"



with staged as (
    select distinct
        hk_country,
        country,
        load_datetime,
        record_source
    from "hive"."raw_vault"."stg_customers"
)

select
    hk_country,
    country,
    load_datetime,
    record_source
from staged


where hk_country not in (select hk_country from "hive"."raw_vault"."hub_country")

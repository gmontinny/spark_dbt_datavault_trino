-- depends_on: {{ ref('stg_customers') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with staged as (
    select distinct
        hk_category,
        preferred_category,
        load_datetime,
        record_source
    from {{ ref('stg_customers') }}
)

select
    hk_category,
    preferred_category,
    load_datetime,
    record_source
from staged

{% if is_incremental() %}
where hk_category not in (select hk_category from {{ this }})
{% endif %}

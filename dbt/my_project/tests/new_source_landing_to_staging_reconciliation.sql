-- Accepted -> staged, per event day: eligible landing rows (latest version,
-- quantity > 0, unit_price > 0) must equal staged rows in count and total_sales.
{% if var('publish_new_source') %}
with expected as (
    select event_day, count(*) as n, coalesce(sum(round((quantity * unit_price - coalesce(discount, 0))::numeric, 2)), 0) as total
    from (
        select distinct on (source_key) source_key, event_time::date as event_day, quantity, unit_price, discount
        from {{ source('new_source', 'sales_landing') }}
        order by source_key, source_version desc
    ) l
    where quantity > 0 and unit_price > 0
    group by 1
),
actual as (
    select invoice_date::date as event_day, count(*) as n, coalesce(sum(total_sales), 0) as total
    from {{ ref('stg_new_source_sales') }}
    group by 1
)
select coalesce(e.event_day, a.event_day) as event_day, e.n as expected_n, a.n as actual_n, e.total as expected_total, a.total as actual_total
from expected e
full outer join actual a on e.event_day = a.event_day
where e.n is distinct from a.n or e.total is distinct from a.total
{% else %}
select 1 where false
{% endif %}

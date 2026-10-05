-- Staged -> report: sales_report must equal legacy staging plus new-source
-- staging in row count and totals (legacy totals unchanged).
with expected as (
    select count(*) as n, coalesce(sum(total_sales), 0) as sales, coalesce(sum(total_revenue), 0) as revenue
    from (
        select total_sales, total_revenue from {{ ref('stg_online_sales') }}
        union all
        select total_sales, total_revenue from {{ ref('stg_new_source_sales') }}
    ) s
),
actual as (
    select count(*) as n, coalesce(sum(total_sales), 0) as sales, coalesce(sum(total_revenue), 0) as revenue
    from {{ ref('sales_report') }}
)
select e.n, a.n as report_n, e.sales, a.sales as report_sales, e.revenue, a.revenue as report_revenue
from expected e cross join actual a
where e.n <> a.n or e.sales <> a.sales or e.revenue <> a.revenue

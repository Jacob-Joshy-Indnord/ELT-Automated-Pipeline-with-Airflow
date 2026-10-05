-- Aggregate reports must account for every row in sales_report.
with r as (select count(*) n, coalesce(sum(total_sales), 0) s from {{ ref('sales_report') }})
select 'category' as report from r, (select sum(order_count) n, sum(total_sales) s from {{ ref('category_sales_report') }}) c where c.n <> r.n or c.s <> r.s
union all
select 'channel' from r, (select sum(order_count) n, sum(total_sales) s from {{ ref('channel_sales_report') }}) c where c.n <> r.n or c.s <> r.s
union all
select 'product' from r, (select sum(order_count) n, sum(total_sales) s from {{ ref('product_sales_report') }}) c where c.n <> r.n or c.s <> r.s

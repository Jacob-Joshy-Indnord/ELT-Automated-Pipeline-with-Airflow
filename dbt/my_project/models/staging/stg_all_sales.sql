{{ config(materialized='table') }}

-- Combines the legacy and new-source feeds upstream of sales_report.
select
    'legacy' as feed,
    description, country, quantity, unit_price, discount, payment_method,
    shipping_cost, total_sales, total_revenue, category, sales_channel,
    is_returned, order_priority, invoice_month, invoice_year, invoice_quarter
from {{ ref('stg_online_sales') }}

union all

select
    'new_source' as feed,
    description, country, quantity, unit_price, discount, payment_method,
    shipping_cost, total_sales, total_revenue, category, sales_channel,
    is_returned, order_priority, invoice_month, invoice_year, invoice_quarter
from {{ ref('stg_new_source_sales') }}

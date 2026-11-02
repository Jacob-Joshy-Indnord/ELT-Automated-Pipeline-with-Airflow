{{ config(materialized='table') }}

-- Both feeds, upstream of sales_report. Legacy rows come from stg_online_sales
-- unchanged; stg_online_sales itself is not modified.
SELECT
    'legacy' AS source_system,
    description, country, quantity, unit_price, discount, payment_method,
    shipping_cost, total_sales, total_revenue, category, sales_channel,
    is_returned, order_priority, invoice_month, invoice_year, invoice_quarter
FROM {{ ref('stg_online_sales') }}

UNION ALL

SELECT
    'new_source' AS source_system,
    description, country, quantity, unit_price, discount, payment_method,
    shipping_cost, total_sales, total_revenue, category, sales_channel,
    is_returned, order_priority, invoice_month, invoice_year, invoice_quarter
FROM {{ ref('stg_new_source_sales') }}

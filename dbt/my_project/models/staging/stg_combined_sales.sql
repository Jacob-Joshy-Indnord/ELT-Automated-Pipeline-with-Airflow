{{ config(materialized='table') }}

-- Both feeds upstream of sales_report. Legacy rows are passed through unchanged.
SELECT
    'legacy' AS source_system,
    invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id,
    country, discount, payment_method, shipping_cost, category, sales_channel, return_status,
    shipment_provider, warehouse_location, order_priority,
    total_sales, total_revenue, is_returned, invoice_year, invoice_month, invoice_quarter
FROM {{ ref('stg_online_sales') }}

UNION ALL

SELECT
    'new_source' AS source_system,
    invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id,
    country, discount, payment_method, shipping_cost, category, sales_channel, return_status,
    shipment_provider, warehouse_location, order_priority,
    total_sales, total_revenue, is_returned, invoice_year, invoice_month, invoice_quarter
FROM {{ ref('stg_new_online_sales') }}

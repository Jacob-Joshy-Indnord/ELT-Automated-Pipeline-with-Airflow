{{ config(
    materialized = 'table',
    unique_key = 'id'
)}}

SELECT 
    description,
    country,
    quantity,
    unit_price,
    discount,
    payment_method,
    shipping_cost,
    total_sales,
    total_revenue,
    category,
    sales_channel,
    is_returned,
    order_priority,
    invoice_month,
    invoice_year,
    invoice_quarter
FROM {{ ref('stg_all_sales') }}
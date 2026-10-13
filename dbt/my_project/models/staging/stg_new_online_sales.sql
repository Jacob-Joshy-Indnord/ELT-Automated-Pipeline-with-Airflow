{{ config(materialized='table') }}

-- New source mapped onto the stg_online_sales field semantics (PROVISIONAL contract:
-- UTC, USD, no conversion). Latest version of each source_key wins; the same
-- quantity > 0 / unit_price > 0 exclusions and calculations as stg_online_sales apply.
WITH latest AS (
    SELECT DISTINCT ON (source_key) *
    FROM {{ source('landing_new', 'new_sales_source') }}
    ORDER BY source_key, version DESC
)

SELECT
    source_key,
    version,
    load_batch_id,
    source_key AS invoice_no,
    stock_code,
    description,
    quantity,
    event_time AS invoice_date,
    unit_price,
    customer_id,
    country,
    discount,
    payment_method,
    shipping_cost,
    category,
    sales_channel,
    return_status,
    shipment_provider,
    warehouse_location,
    order_priority,

    ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2) AS total_sales,
    ROUND(((quantity * unit_price - COALESCE(discount, 0)) + COALESCE(shipping_cost, 0))::numeric, 2) AS total_revenue,
    CASE WHEN lower(return_status) = 'returned' THEN 1 ELSE 0 END AS is_returned,
    EXTRACT(year from event_time) AS invoice_year,
    EXTRACT(month from event_time) AS invoice_month,
    EXTRACT(quarter from event_time) AS invoice_quarter

FROM latest
WHERE quantity > 0
    AND unit_price > 0

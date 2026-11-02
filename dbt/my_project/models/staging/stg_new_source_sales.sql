{{ config(materialized='table') }}

-- Normalizes the second feed (new_source.sales_landing) into the stg_online_sales
-- field semantics. Provisional assumptions (docs/new-source-contract.md):
-- event_time is UTC, amounts are already in the legacy currency, and the
-- highest source_version per source_key wins.
-- Publication can be paused with --vars '{new_source_enabled: false}'.
WITH latest AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_version DESC) AS version_rank
    FROM {{ source('new_source', 'sales_landing') }}
)

SELECT
    source_key,
    source_version,
    description,
    quantity,
    event_time AS invoice_date,
    unit_price,
    country,
    discount,
    payment_method,
    shipping_cost,
    category,
    sales_channel,
    return_status,
    order_priority,

    ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2) AS total_sales,
    ROUND(((quantity * unit_price - COALESCE(discount, 0)) + COALESCE(shipping_cost, 0))::numeric, 2) AS total_revenue,
    CASE WHEN lower(return_status) = 'returned' THEN 1 ELSE 0 END AS is_returned,
    EXTRACT(year from event_time) AS invoice_year,
    EXTRACT(month from event_time) AS invoice_month,
    EXTRACT(quarter from event_time) AS invoice_quarter

FROM latest
WHERE version_rank = 1
    AND quantity > 0
    AND unit_price > 0
    AND {{ var('new_source_enabled', true) }}

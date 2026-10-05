{{ config(materialized='table') }}

-- Maps new_source.sales_landing onto the stg_online_sales field semantics.
-- Latest accepted version per source key wins; currency/timezone conversion is
-- the identity under the provisional contract (USD, UTC) and is applied by the
-- loader. Same exclusions and formulas as stg_online_sales.
with latest as (
    select distinct on (source_key) *
    from {{ source('new_source', 'sales_landing') }}
    order by source_key, source_version desc
)

select
    source_key,
    invoice_no,
    stock_code,
    description,
    quantity,
    event_time as invoice_date,
    unit_price,
    customer_id,
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

from latest
where {{ var('publish_new_source') }}
    and quantity > 0
    and unit_price > 0

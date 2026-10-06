-- total_sales / total_revenue must follow the stg_online_sales rules.
SELECT source_key
FROM {{ ref('stg_new_source_sales') }}
WHERE total_sales <> ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2)
   OR total_revenue <> ROUND((quantity * unit_price - COALESCE(discount, 0) + COALESCE(shipping_cost, 0))::numeric, 2)
   OR is_returned NOT IN (0, 1)

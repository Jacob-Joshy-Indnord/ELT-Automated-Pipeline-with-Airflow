-- total_sales / total_revenue must follow the existing rules (discount is an amount; shipping added to revenue).
SELECT * FROM {{ ref('stg_new_online_sales') }}
WHERE total_sales <> ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2)
   OR total_revenue <> ROUND((quantity * unit_price - COALESCE(discount, 0) + COALESCE(shipping_cost, 0))::numeric, 2)

-- Fails for any staged row whose monetary fields deviate from the legacy formulas.
select source_key
from {{ ref('stg_new_source_sales') }}
where total_sales <> round((quantity * unit_price - coalesce(discount, 0))::numeric, 2)
   or total_revenue <> round((quantity * unit_price - coalesce(discount, 0) + coalesce(shipping_cost, 0))::numeric, 2)
   or total_revenue - total_sales <> round(coalesce(shipping_cost, 0)::numeric, 2)

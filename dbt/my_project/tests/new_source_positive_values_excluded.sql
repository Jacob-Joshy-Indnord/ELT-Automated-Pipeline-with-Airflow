-- Rows with quantity <= 0 or unit_price <= 0 must never reach staging.
SELECT * FROM {{ ref('stg_new_source_sales') }}
WHERE quantity <= 0 OR unit_price <= 0

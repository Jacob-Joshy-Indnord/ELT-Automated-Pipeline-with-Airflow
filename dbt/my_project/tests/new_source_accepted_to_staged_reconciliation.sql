-- Accepted landing rows (latest version per key, positive qty/price) must all be
-- staged: counts and sales totals reconcile. Skipped while publication is paused.
{% if var('new_source_enabled', true) %}
WITH expected AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2)), 0) AS sales
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_version DESC) AS rn
        FROM {{ source('new_source', 'sales_landing') }}
    ) l
    WHERE rn = 1 AND quantity > 0 AND unit_price > 0
),
staged AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS sales FROM {{ ref('stg_new_source_sales') }}
)
SELECT e.n AS expected_n, s.n AS staged_n, e.sales AS expected_sales, s.sales AS staged_sales
FROM expected e CROSS JOIN staged s
WHERE e.n <> s.n OR e.sales <> s.sales
{% else %}
SELECT 1 WHERE FALSE
{% endif %}

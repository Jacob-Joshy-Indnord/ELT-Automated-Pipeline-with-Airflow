-- Accepted (eligible, latest version) -> staged -> report must reconcile in count and total_sales,
-- and legacy rows in the report must equal legacy staging.
WITH accepted AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(ROUND((quantity * unit_price - COALESCE(discount, 0))::numeric, 2)), 0) AS total
    FROM (
        SELECT DISTINCT ON (source_key) *
        FROM {{ source('landing_new', 'new_sales_source') }}
        ORDER BY source_key, version DESC
    ) l
    WHERE quantity > 0 AND unit_price > 0
),
staged AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS total FROM {{ ref('stg_new_online_sales') }}
),
legacy_staged AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS total FROM {{ ref('stg_online_sales') }}
),
report AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS total FROM {{ ref('sales_report') }}
)
SELECT 'accepted_vs_staged' AS failed_check
FROM accepted, staged WHERE accepted.n <> staged.n OR accepted.total <> staged.total
UNION ALL
SELECT 'staged_vs_report'
FROM report, staged, legacy_staged
WHERE report.n <> staged.n + legacy_staged.n OR report.total <> staged.total + legacy_staged.total

-- sales_report must equal legacy + new-source staging in counts and totals, and
-- the legacy part must be unchanged from stg_online_sales.
WITH legacy AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS sales, COALESCE(SUM(total_revenue), 0) AS revenue
    FROM {{ ref('stg_online_sales') }}
),
new_src AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS sales, COALESCE(SUM(total_revenue), 0) AS revenue
    FROM {{ ref('stg_new_source_sales') }}
),
report AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS sales, COALESCE(SUM(total_revenue), 0) AS revenue
    FROM {{ ref('sales_report') }}
),
all_legacy AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(total_sales), 0) AS sales
    FROM {{ ref('stg_all_sales') }} WHERE source_system = 'legacy'
)
SELECT r.n AS report_n, l.n AS legacy_n, w.n AS new_n
FROM report r, legacy l, new_src w, all_legacy a
WHERE r.n <> l.n + w.n
   OR r.sales <> l.sales + w.sales
   OR r.revenue <> l.revenue + w.revenue
   OR a.n <> l.n OR a.sales <> l.sales

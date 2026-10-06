{{ config(
    materialized = 'table'
)}}

-- Monthly sales split by source for the "Sales by source" chart.
-- Sales in sales_report that did not come from the original source
-- (stg_online_sales) are labelled 'New source', so the split keeps working
-- when another source is added upstream of sales_report.
WITH report AS (
    SELECT invoice_year, invoice_month, SUM(total_sales) AS sales
    FROM {{ ref('sales_report') }}
    GROUP BY invoice_year, invoice_month
),

original AS (
    SELECT invoice_year, invoice_month, SUM(total_sales) AS sales
    FROM {{ ref('stg_online_sales') }}
    GROUP BY invoice_year, invoice_month
),

split AS (
    SELECT
        r.invoice_year,
        r.invoice_month,
        'Original source' AS source,
        COALESCE(o.sales, 0) AS total_sales
    FROM report r
    LEFT JOIN original o
        ON o.invoice_year = r.invoice_year AND o.invoice_month = r.invoice_month

    UNION ALL

    SELECT
        r.invoice_year,
        r.invoice_month,
        'New source' AS source,
        r.sales - COALESCE(o.sales, 0) AS total_sales
    FROM report r
    LEFT JOIN original o
        ON o.invoice_year = r.invoice_year AND o.invoice_month = r.invoice_month
)

SELECT invoice_year, invoice_month, source, total_sales
FROM split
WHERE total_sales <> 0
ORDER BY invoice_year, invoice_month, source

-- Aggregate reports must carry the same totals as sales_report.
SELECT 'category' AS report
WHERE (SELECT COALESCE(SUM(total_sales), 0) FROM {{ ref('category_sales_report') }})
   <> (SELECT COALESCE(SUM(total_sales), 0) FROM {{ ref('sales_report') }})
UNION ALL
SELECT 'channel'
WHERE (SELECT COALESCE(SUM(total_sales), 0) FROM {{ ref('channel_sales_report') }})
   <> (SELECT COALESCE(SUM(total_sales), 0) FROM {{ ref('sales_report') }})
UNION ALL
SELECT 'product'
WHERE (SELECT COALESCE(SUM(total_sales), 0) FROM {{ ref('product_sales_report') }})
   <> (SELECT COALESCE(SUM(total_sales), 0) FROM {{ ref('sales_report') }})

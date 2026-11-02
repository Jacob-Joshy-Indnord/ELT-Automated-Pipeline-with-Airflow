-- Source-scoped uniqueness: each (source_key, source_version) lands once.
SELECT source_key, source_version, COUNT(*) AS n
FROM {{ source('new_source', 'sales_landing') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1

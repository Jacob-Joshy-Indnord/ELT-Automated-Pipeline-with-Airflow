-- Zero-row and late-data policy for the batch under test (var new_source_run_date, default latest batch).
-- Returns a row (= failure) when no batch was loaded, an empty batch violates the zero-row policy,
-- or accepted + rejected does not add up to received. Late rows are quarantined in new_sales_rejects.
WITH batch AS (
    SELECT *
    FROM {{ source('landing_new', 'load_batches') }}
    {% if var('new_source_run_date') %}
    WHERE run_date = '{{ var("new_source_run_date") }}'::date
    {% endif %}
    ORDER BY run_date DESC
    LIMIT 1
)
SELECT 'no batch loaded' AS failed_check WHERE NOT EXISTS (SELECT 1 FROM batch)
UNION ALL
SELECT 'zero-row batch' FROM batch
WHERE rows_received = 0 AND '{{ var("new_source_zero_row_policy") }}' = 'fail'
UNION ALL
SELECT 'accepted + rejected <> received' FROM batch
WHERE rows_accepted + rows_rejected <> rows_received

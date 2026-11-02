-- Rejected records must trace to a batch and match its rejected_count.
SELECT r.batch_id FROM {{ source('new_source', 'sales_rejects') }} r
LEFT JOIN {{ source('new_source', 'load_batches') }} b USING (batch_id)
WHERE b.batch_id IS NULL OR r.reject_reason IS NULL
UNION ALL
SELECT b.batch_id FROM {{ source('new_source', 'load_batches') }} b
WHERE b.rejected_count <> (SELECT COUNT(*) FROM {{ source('new_source', 'sales_rejects') }} r WHERE r.batch_id = b.batch_id)

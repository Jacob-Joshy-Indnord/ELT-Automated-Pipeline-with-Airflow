-- Late or unexpectedly empty batch fails. Provisional thresholds (to be agreed
-- with the source owner): newest batch <= 36h old with >= 1 accepted row.
-- Override with --vars '{new_source_max_lateness_hours: N, new_source_min_accepted_rows: M}'.
{% if var('new_source_enabled', true) %}
WITH latest AS (
    SELECT * FROM {{ source('new_source', 'load_batches') }}
    ORDER BY loaded_at DESC LIMIT 1
)
SELECT 'no batch loaded' AS problem WHERE NOT EXISTS (SELECT 1 FROM latest)
UNION ALL
SELECT 'late batch' FROM latest
WHERE loaded_at < NOW() - INTERVAL '{{ var("new_source_max_lateness_hours", 36) }} hours'
UNION ALL
SELECT 'too few accepted rows' FROM latest
WHERE accepted_count < {{ var('new_source_min_accepted_rows', 1) }}
{% else %}
SELECT 1 WHERE FALSE
{% endif %}

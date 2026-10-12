SYSTEM FLUSH LOGS;

SELECT
    event_time,
    tables,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    projections
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query LIKE '%argMax(cat_id, amount)%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 10

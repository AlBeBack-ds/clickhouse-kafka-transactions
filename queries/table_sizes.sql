SELECT
    table,
    sum(rows) AS rows,
    count() AS parts,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE active
  AND database = currentDatabase()
  AND table IN ('transactions', 'transactions_optimized')
GROUP BY table
ORDER BY table

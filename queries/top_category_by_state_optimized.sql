SELECT
    us_state,
    argMax(cat_id, amount) AS category,
    max(amount) AS max_amount
FROM transactions_optimized
GROUP BY us_state
ORDER BY us_state

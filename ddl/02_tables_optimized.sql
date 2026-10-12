CREATE TABLE IF NOT EXISTS transactions_queue_optimized
(
    transaction_time String,
    merch String,
    cat_id String,
    amount Float64,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code UInt32,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_optimized',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS transactions_optimized
(
    transaction_time DateTime CODEC(Delta, ZSTD(3)),
    merch LowCardinality(String),
    cat_id LowCardinality(String),
    amount Float64 CODEC(ZSTD(3)),
    name_1 LowCardinality(String),
    name_2 LowCardinality(String),
    gender LowCardinality(String),
    street LowCardinality(String),
    one_city LowCardinality(String),
    us_state LowCardinality(String),
    post_code UInt32 CODEC(ZSTD(3)),
    lat Float64 CODEC(ZSTD(3)),
    lon Float64 CODEC(ZSTD(3)),
    population_city UInt32 CODEC(ZSTD(3)),
    jobs LowCardinality(String),
    merchant_lat Float64 CODEC(ZSTD(3)),
    merchant_lon Float64 CODEC(ZSTD(3)),
    target UInt8,

    PROJECTION max_amount_by_state
    (
        SELECT
            us_state,
            max(amount),
            argMax(cat_id, amount)
        GROUP BY us_state
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, cat_id, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_optimized_mv TO transactions_optimized AS
SELECT * REPLACE (parseDateTimeBestEffort(transaction_time) AS transaction_time)
FROM transactions_queue_optimized;

CREATE TABLE IF NOT EXISTS transactions_queue
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
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS transactions
(
    transaction_time DateTime,
    merch String,
    cat_id String,
    amount Float64,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code Int64,
    lat Float64,
    lon Float64,
    population_city Int64,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target Int64
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_mv TO transactions AS
SELECT * REPLACE (parseDateTimeBestEffort(transaction_time) AS transaction_time)
FROM transactions_queue;

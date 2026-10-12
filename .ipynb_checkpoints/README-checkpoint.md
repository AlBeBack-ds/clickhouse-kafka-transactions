# Транзакции из Kafka в ClickHouse

Домашнее задание по ClickHouse. CSV с транзакциями загружается в топик Kafka, ClickHouse забирает данные из топика в таблицы. Затем SQL-запросом для каждого штата находится категория самой крупной транзакции. Отдельно сделана оптимизированная схема хранения, и её эффект измерен.

## Что где лежит

```
.
├── docker-compose.yml
├── producer/
│   ├── producer.py                          загрузка CSV в топик Kafka
│   ├── requirements.txt
│   └── Dockerfile
├── ddl/
│   ├── 01_tables.sql                        базовые таблицы (п.2)
│   └── 02_tables_optimized.sql              оптимизированные таблицы (п.4)
├── queries/
│   ├── top_category_by_state.sql            запрос к базовой таблице (п.3)
│   ├── top_category_by_state_optimized.sql  тот же запрос к оптимизированной
│   ├── table_sizes.sql                      размеры таблиц
│   └── query_stats.sql                      статистика запросов из system.query_log
├── results/
│   └── top_category_by_state.csv            результат запроса
└── data/                                    сюда нужно положить train.csv
```

В `docker-compose.yml` описаны следующие сервисы:

- `zookeeper` и `kafka` — брокер. Снаружи Kafka доступна на `localhost:9095`.
- `kafka-setup` создаёт топик `transactions` из трёх партиций и завершается.
- `kafka-ui` — веб-интерфейс Kafka на http://localhost:8080. Через него удобно смотреть сообщения в топике.
- `clickhouse` при первом старте выполняет скрипты из `ddl/`. HTTP-интерфейс доступен на `localhost:8123`.
- `producer` — контейнер со скриптом загрузки. Он не стартует вместе с остальными, а запускается отдельной командой.

## Данные

Используется `train.csv` из соревнования: https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394. В файле 786 431 транзакция, нужные для задания поля — `us_state`, `cat_id` и `amount`.

Скачайте архив со страницы соревнования (вкладка Data, кнопка Download All) или через Kaggle CLI:

```bash
kaggle competitions download -c teta-ml-1-2025
```

Распакуйте `train.csv` в папку `data/`, чтобы получился путь `data/train.csv`. Сам файл в репозиторий не добавлен: он весит 145 МБ.

## Запуск

Нужен Docker с Compose v2. Порты 8080, 8123 и 9095 на хосте должны быть свободны.

```bash
git clone <ссылка на репозиторий>
cd clickhouse-kafka-transactions
docker compose up -d --build
```

Первый запуск занимает пару минут, в основном на скачивание образов. Проверить состояние сервисов:

```bash
docker compose ps -a
```

Должно получиться так: `clickhouse` в статусе `healthy`, `kafka` в статусе `healthy`, `kafka-setup` в статусе `Exited (0)`. Таблицы ClickHouse создаются сами:

```bash
docker compose exec clickhouse clickhouse-client -q "SHOW TABLES"
```

```
transactions
transactions_mv
transactions_optimized
transactions_optimized_mv
transactions_queue
transactions_queue_optimized
```

## 1. Загрузка CSV в Kafka

```bash
docker compose run --rm producer
```

Скрипт читает `data/train.csv` кусками по 50 тысяч строк и отправляет каждую строку отдельным JSON-сообщением в топик `transactions`. Загрузка всего файла занимает около минуты. В конце скрипт пишет строку `готово: 786431 строк ...`.

Можно отправить только часть файла, например `docker compose run --rm producer --limit 10000`.

Скрипт можно запустить и без Docker, с хоста. По умолчанию он подключается к `localhost:9095`:

```bash
pip install -r producer/requirements.txt
python producer/producer.py --file data/train.csv
```

Учтите, что повторная загрузка того же файла продублирует строки в таблицах. Чтобы начать с чистого листа, выполните `docker compose down -v` и снова `docker compose up -d`.

## 2. Таблицы в ClickHouse

DDL лежит в [`ddl/01_tables.sql`](ddl/01_tables.sql). Схема стандартная для Kafka-интеграции:

- `transactions_queue` — таблица на движке Kafka. Она ничего не хранит, а читает топик `transactions` в формате JSONEachRow.
- `transactions` — таблица MergeTree, в которой лежат данные.
- `transactions_mv` — materialized view. Он забирает новые сообщения из `transactions_queue`, разбирает строку с датой в `DateTime` и пишет результат в `transactions`.

ClickHouse подхватывает сообщения из Kafka в течение пары минут после окончания загрузки. Проверить, что всё доехало:

```bash
docker compose exec clickhouse clickhouse-client -q "SELECT count() FROM transactions"
```

Должно получиться 786431.

## 3. Категория самой крупной транзакции по штатам

Запрос в [`queries/top_category_by_state.sql`](queries/top_category_by_state.sql):

```sql
SELECT
    us_state,
    argMax(cat_id, amount) AS category,
    max(amount) AS max_amount
FROM transactions
GROUP BY us_state
ORDER BY us_state
```

`argMax(cat_id, amount)` возвращает `cat_id` строки, в которой `amount` максимален внутри группы. Сумма транзакции выведена рядом, чтобы результат было легко проверить.

Выгрузка результата в CSV:

```bash
docker compose exec clickhouse sh -c "clickhouse-client --queries-file /queries/top_category_by_state.sql --format CSVWithNames > /results/top_category_by_state.csv"
```

Готовый файл лежит в [`results/top_category_by_state.csv`](results/top_category_by_state.csv): 51 штат, включая DC. Я сверил его с расчётом на pandas прямо по `train.csv`, всё совпадает. Чаще всего самая крупная транзакция в штате приходится на `travel` (35 штатов), реже на `shopping_pos` и `shopping_net` (по 8).

## 4. Оптимизация хранения

Оптимизированная схема лежит в [`ddl/02_tables_optimized.sql`](ddl/02_tables_optimized.sql). У неё своя Kafka-таблица `transactions_queue_optimized` с отдельной consumer group, поэтому она читает тот же топик независимо от базовой. В итоге обе таблицы содержат одни и те же данные, и их можно честно сравнить.

Что изменено по сравнению с `transactions`:

- **LowCardinality(String)** для всех строковых колонок. Уникальных значений в них немного: 51 штат, 14 категорий, около 700 мерчантов, около 500 профессий. Такие строки хранятся как словарь и короткие номера, места занимают в разы меньше, а группировка по ним быстрее.
- **Типы чисел подогнаны под данные:** `UInt32` вместо `Int64` для почтового индекса и населения города, `UInt8` для `target`.
- **Кодеки сжатия:** `Delta + ZSTD` для времени, `ZSTD` для чисел.
- **Ключ сортировки `ORDER BY (us_state, cat_id, transaction_time)`** вместо `tuple()`. Данные одного штата лежат рядом, поэтому лучше сжимаются, а фильтры по штату и категории читают меньше гранул.
- **Партиционирование по месяцу** `toYYYYMM(transaction_time)`. Запросы за период читают только нужные партиции, а старые данные удобно удалять целыми партициями.
- **Агрегирующая проекция `max_amount_by_state`.** Для каждого куска данных ClickHouse заранее хранит `max(amount)` и `argMax(cat_id, amount)` по штатам. Запрос из п.3 автоматически берёт данные из проекции и не читает исходные строки. Текст запроса менять не нужно, проекция подхватывается сама.

### Результаты

Размер таблиц после загрузки 786 431 строки ([`queries/table_sizes.sql`](queries/table_sizes.sql)):

| Таблица | Без сжатия | Со сжатием |
|---|---|---|
| `transactions` | 126.8 MiB | 56.0 MiB |
| `transactions_optimized` | 49.2 MiB | 25.6 MiB |

Запрос из п.3, по данным `system.query_log` ([`queries/query_stats.sql`](queries/query_stats.sql)):

| Таблица | Прочитано строк | Прочитано данных |
|---|---|---|
| `transactions` | 786 431 | 28.9 MiB |
| `transactions_optimized` с выключенной проекцией | 786 431 | 7.5 MiB |
| `transactions_optimized` | 751 | 83 KiB |

Итог: таблица на диске стала в 2.2 раза меньше. Запрос читает в 350 раз меньше данных: только 751 строку проекции (51 штат на каждую из 15 месячных партиций) вместо всей таблицы. Даже без проекции объём чтения падает почти в 4 раза за счёт LowCardinality.

По времени разница на таком объёме почти не видна: оба варианта выполняются за 20–30 мс, и это время уходит в основном на накладные расходы самого запроса. Выигрыш от проекции растёт вместе с таблицей, потому что объём чтения из проекции зависит от числа штатов и партиций, а не от числа строк.

Повторить сравнение:

```bash
docker compose exec clickhouse clickhouse-client --queries-file /queries/table_sizes.sql --format PrettyCompact
docker compose exec clickhouse clickhouse-client --queries-file /queries/top_category_by_state.sql
docker compose exec clickhouse clickhouse-client --queries-file /queries/top_category_by_state_optimized.sql
docker compose exec clickhouse clickhouse-client --queries-file /queries/query_stats.sql --format PrettyCompact
```

В колонке `projections` последнего запроса видно, что для `transactions_optimized` использовалась проекция `max_amount_by_state`. Результаты обоих запросов совпадают.

## Остановка

```bash
docker compose down      # остановить, данные ClickHouse сохранятся
docker compose down -v   # остановить и удалить данные
```

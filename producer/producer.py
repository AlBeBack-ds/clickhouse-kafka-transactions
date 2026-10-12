import argparse
import json
import os
import sys
import time

import pandas as pd
from confluent_kafka import Producer


def parse_args():
    parser = argparse.ArgumentParser(description="Отправка транзакций из CSV в топик Kafka")
    parser.add_argument("--file", default=os.getenv("CSV_FILE", "data/train.csv"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9095"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--limit", type=int, default=None, help="отправить только первые N строк")
    return parser.parse_args()


def main():
    args = parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "linger.ms": 50,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 200_000,
    })

    failed = 0

    def on_delivery(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1

    sent = 0
    started = time.time()
    for chunk in pd.read_csv(args.file, chunksize=args.chunk_size):
        if args.limit is not None:
            chunk = chunk.head(args.limit - sent)
        chunk = chunk.astype(object).where(chunk.notna(), None)

        for record in chunk.to_dict(orient="records"):
            value = json.dumps(record, ensure_ascii=False)
            while True:
                try:
                    producer.produce(args.topic, value=value, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)

        sent += len(chunk)
        print(f"отправлено {sent} строк", flush=True)
        if args.limit is not None and sent >= args.limit:
            break

    producer.flush()
    print(f"готово: {sent} строк за {time.time() - started:.1f} с, ошибок доставки: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

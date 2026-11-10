"""Send JSON events to a Kafka topic and report which partition each one landed in.

Examples:
    python3 producer.py                         # 20 events, keys user-1 .. user-4
    python3 producer.py --count 50 --keys 8     # 50 events, keys user-1 .. user-8
    python3 producer.py --no-key                # events without a key
    python3 producer.py --count 0 --rate 2      # run forever, 2 events per second
"""
import argparse
import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default="clicks")
parser.add_argument("--count", type=int, default=20, help="number of events, 0 = run forever")
parser.add_argument("--keys", type=int, default=4, help="number of distinct keys (user-1 .. user-N)")
parser.add_argument("--no-key", action="store_true", help="send events without a key")
parser.add_argument("--rate", type=float, default=10, help="events per second")
parser.add_argument("--seed", type=int, default=42, help="random seed, so everybody gets the same events")
parser.add_argument("--bootstrap", default="localhost:9092")
args = parser.parse_args()
random.seed(args.seed)

producer = Producer({
    "bootstrap.servers": args.bootstrap,
    # hash keys like the Java clients (and kafka-console-producer.sh) do: murmur2(key) mod #partitions
    "partitioner": "murmur2_random",
})


def report(err, msg):
    """Called once per event, when the broker has acknowledged (or rejected) it."""
    if err is not None:
        print(f"FAILED  {err}")
        return
    key = msg.key().decode() if msg.key() else None
    print(f"key={key!s:8}  ->  partition {msg.partition()}  offset {msg.offset()}")


seq = 0
try:
    while args.count == 0 or seq < args.count:
        seq += 1
        key = None if args.no_key else f"user-{random.randint(1, args.keys)}"
        event = {"seq": seq, "user": key, "page": random.choice(["/home", "/cart", "/checkout"]),
                 "ts": datetime.now().isoformat(timespec="seconds")}
        producer.produce(args.topic, key=key, value=json.dumps(event), on_delivery=report)
        producer.poll(0)          # serve delivery reports
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print(f"sent {seq} events to topic '{args.topic}'")

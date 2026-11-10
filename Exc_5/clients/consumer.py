"""Read events from a Kafka topic as a member of a consumer group.

Prints the partitions assigned to this consumer and every event it reads.
Offsets are committed manually, every --commit-every events.

Examples:
    python3 consumer.py --group dashboard --name A
    python3 consumer.py --group dashboard --name B          # second member, same group
    python3 consumer.py --group billing   --name X          # another group
"""
import argparse
import json
import os

from confluent_kafka import Consumer, KafkaException

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default="clicks")
parser.add_argument("--group", required=True, help="consumer group id")
parser.add_argument("--name", default=str(os.getpid()), help="label printed in the output")
parser.add_argument("--commit-every", type=int, default=5, help="commit offsets every N events")
parser.add_argument("--bootstrap", default="localhost:9092")
args = parser.parse_args()

consumer = Consumer({
    "bootstrap.servers": args.bootstrap,
    "group.id": args.group,
    "client.id": f"consumer-{args.name}",
    "auto.offset.reset": "earliest",   # where to start if the group has no committed offset yet
    "enable.auto.commit": False,       # we commit ourselves, see below
    "session.timeout.ms": 10000,       # a member that stops sending heartbeats is removed after 10 s
})


def on_assign(c, partitions):
    print(f"[{args.name}] assigned partitions: {sorted(p.partition for p in partitions) or 'none (idle)'}")


def on_revoke(c, partitions):
    print(f"[{args.name}] revoked partitions:  {sorted(p.partition for p in partitions)}")


consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
print(f"[{args.name}] joined group '{args.group}' on topic '{args.topic}' (Ctrl+C to stop)")

processed = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[{args.name}] error: {msg.error()}")
            continue
        event = json.loads(msg.value())
        processed += 1
        print(f"[{args.name}] partition {msg.partition()} offset {msg.offset():>4}  "
              f"key={event.get('user')!s:8} seq={event.get('seq')}")

        if processed % args.commit_every == 0:
            consumer.commit(asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    try:
        consumer.commit(asynchronous=False)   # commit what we processed since the last commit
    except KafkaException:
        pass                                  # nothing new to commit
    consumer.close()           # leave the group cleanly -> fast rebalance for the others
    print(f"[{args.name}] stopped after {processed} events")

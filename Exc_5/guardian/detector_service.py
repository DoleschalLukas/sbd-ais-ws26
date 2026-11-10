"""The anomaly detector as a Kafka service. YOUR CODE (Part 2, Stage C2).

The plumbing is here, the decisions (marked TODO) are yours. Contract with
the maintenance team: every reading goes through model.score(); every
reading with score >= model.threshold becomes ONE anomaly event,
model.anomaly_event(reading, score), as JSON, published to the topic
`anomalies`.

    python3 detector_service.py --name det-1
    python3 detector_service.py --name det-2          # one more instance
"""
import argparse
import json
import time

from confluent_kafka import Consumer, Producer

from model import BearingModel

ap = argparse.ArgumentParser()
ap.add_argument("--name", default="det-1", help="instance name (shows up in the scoreboard)")
ap.add_argument("--in-topic", default=None, help="TODO: the topic your gateways write to")
ap.add_argument("--group", default=None, help="TODO: the consumer group of this detector")
ap.add_argument("--out-topic", default="anomalies")
ap.add_argument("--bootstrap", default="localhost:9092")
args = ap.parse_args()
if not args.in_topic or not args.group:
    raise SystemExit("TODO: set --in-topic and --group (or give them defaults above)")

model = BearingModel(instance=args.name)
consumer = Consumer({
    "bootstrap.servers": args.bootstrap,
    "group.id": args.group,
    "client.id": args.name,
    "auto.offset.reset": "latest",     # TODO: why "latest" and not "earliest" for a live detector?
})
producer = Producer({
    "bootstrap.servers": args.bootstrap,
    # TODO: more settings?
})
consumer.subscribe([args.in_topic])
print(f"[{args.name}] {args.in_topic} -> {args.out_topic} (Ctrl+C to stop)", flush=True)

readings = anomalies = 0
report_at = time.time() + 5
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[{args.name}] {msg.error()}")
            continue
        reading = json.loads(msg.value())
        score = model.score(reading)
        readings += 1
        if score is not None and score >= model.threshold:
            anomalies += 1
            event = model.anomaly_event(reading, score)
            producer.produce(args.out_topic, key=None,       # TODO: which key?
                             value=json.dumps(event))
        producer.poll(0)
        if time.time() >= report_at:
            print(f"[{args.name}] {readings:>9,} readings  {anomalies:>6,} anomalies  "
                  f"missing {model.missing}  out-of-order {model.out_of_order}", flush=True)
            report_at += 5
except KeyboardInterrupt:
    pass
finally:
    producer.flush(10)
    consumer.close()
    print(f"[{args.name}] stopped after {readings:,} readings, {anomalies:,} anomalies")

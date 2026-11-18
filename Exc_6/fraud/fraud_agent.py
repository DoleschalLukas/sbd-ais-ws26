"""A fraud agent as a Kafka service (Card Sentinel). Complete: read it, you do
not need to change it.

Each model of the data science team runs as its own service, deployed with
this file. `agents.py` starts several of them for you:

    python3 fraud_agent.py --model anomaly  --name ano-1
    python3 fraud_agent.py --model velocity --name vel-1
    python3 fraud_agent.py --model velocity --name vel-2      # one more instance

Contract with case management: every committed transaction goes through
model.score(); every transaction with score >= model.threshold becomes ONE
alert, model.alert_event(tx, score), as JSON on the topic `fraud-alerts`.

The design decisions are marked DECISION 1-3; the README explains them.
"""
import argparse
import json
import signal
import sys
import time

from confluent_kafka import Consumer, Producer

from models import make_model

ap = argparse.ArgumentParser()
ap.add_argument("--model", required=True, help="anomaly or velocity")
ap.add_argument("--name", required=True, help="instance name (shows up in the scoreboard)")
ap.add_argument("--topic", default="fraud.public.transactions", help="the CDC topic of the transactions")
ap.add_argument("--group", default=None, help="default: <model>-agent")
ap.add_argument("--out-topic", default="fraud-alerts")
ap.add_argument("--bootstrap", default="localhost:9092")
args = ap.parse_args()

# DECISION 1: one consumer group per model. Each group gets EVERY payment
# (R2); the instances of one model share its group and split the partitions
# between them. Both models in one group would each see only part of the
# payments. (Exercise 5, Part 1, step 3.)
group = args.group or f"{args.model}-agent"

model = make_model(args.model, instance=args.name)
consumer = Consumer({
    "bootstrap.servers": args.bootstrap,
    "group.id": group,
    "client.id": args.name,
    # where a new consumer group starts: the anomaly model needs the cardholders'
    # history, the velocity model only the last 60 s (replaying older payments
    # would only build up lag)
    "auto.offset.reset": "earliest" if args.model == "anomaly" else "latest",
    # offsets are committed automatically every 5 s (the default). After a
    # crash, a restarted agent re-reads up to 5 s of payments: some alerts can
    # arrive twice. Fine here; case management can drop duplicates by alert_id.
})
producer = Producer({
    "bootstrap.servers": args.bootstrap,
})


def transaction(msg):
    """The committed transaction (a dict with id, user_id, amount, ...) carried by
    a CDC event, or None if the event does not carry a new transaction (see the
    events of the Guide)."""
    if msg.value() is None:                    # tombstone: only for log compaction
        return None
    payload = json.loads(msg.value())["payload"]
    if payload["op"] not in ("c", "r"):        # c = insert, r = row of the snapshot;
        return None                            # updates and deletes are no new payments
    return payload["after"]


def event_time(tx, msg):
    """When did the transaction happen (epoch seconds)?

    DECISION 2: event time, not processing time. `created_at` is set when the
    payment is committed; Debezium sends it as microseconds since the epoch
    (io.debezium.time.MicroTimestamp). time.time(), the moment this agent
    happens to read the event, would be wrong whenever the agent is behind:
    after an outage, or when the data science team replays last month's
    payments in one hour, the velocity model would see a month of payments
    "within 60 s" and flag almost every cardholder.
    """
    return tx["created_at"] / 1e6


def say(text):
    """Print one line in one piece (several agents may share a terminal)."""
    sys.stdout.write(f"[{args.name}] {text}\n")
    sys.stdout.flush()


consumer.subscribe([args.topic],
                   on_assign=lambda c, ps: say(f"assigned partitions {sorted(p.partition for p in ps)}"))
say(f"model {args.model}, group {group}: {args.topic} -> {args.out_topic}")

alerts = 0
report_at = time.time() + 5
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            say(str(msg.error()))
            continue
        tx = transaction(msg)
        if tx is None:
            continue
        score = model.score(tx, event_time(tx, msg))
        if score >= model.threshold:
            alerts += 1
            # DECISION 3: alerts are keyed by cardholder, so case management
            # gets all alerts of one cardholder in order, in one partition.
            producer.produce(args.out_topic, key=str(tx["user_id"]),
                             value=json.dumps(model.alert_event(tx, score)))
        producer.poll(0)
        if time.time() >= report_at:
            say(f"{model.events:>9,} transactions  {alerts:>5,} alerts  "
                f"{len(model.state):>7,} cardholders  lag {model.lag_s:6.1f} s  "
                f"out-of-order {model.out_of_order}")
            report_at += 5
except KeyboardInterrupt:
    pass
finally:
    signal.signal(signal.SIGINT, signal.SIG_IGN)   # a second Ctrl+C must not interrupt the clean shutdown
    producer.flush(10)
    consumer.close()
    say(f"stopped after {model.events:,} transactions, {alerts:,} alerts")

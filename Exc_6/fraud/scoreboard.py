"""Scores the current run of the card processor (Card Sentinel). Do not change this file.

Plays case management: reads the alerts of the topic `fraud-alerts` (only
committed ones, like a careful consumer would), the ground truth of the run,
the agents' telemetry and the consumer groups, and checks the measurable
requirements.

    python3 scoreboard.py
    python3 scoreboard.py --timeline        # + lag of every model over time

Only alerts for transactions of the current run count (since the last start
of card_processor.py).
"""
import argparse
import json
import statistics
import time
import uuid
from collections import Counter, defaultdict

import psycopg
from confluent_kafka import OFFSET_END, Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from card_processor import CONN, GROUND_TRUTH
from models import VelocityModel

DETECTS = {"card_testing": "velocity", "account_takeover": "anomaly"}
FRESH_S = 5


def ok(cond):
    return "PASS" if cond else "FAIL"


def read_ground_truth():
    if not GROUND_TRUTH.exists():
        raise SystemExit("no ground_truth.jsonl: start card_processor.py first")
    lines = [json.loads(l) for l in GROUND_TRUTH.read_text().splitlines() if l.strip()]
    run = lines[0]
    progress = [l for l in lines if l["kind"] == "progress"]
    last = {}
    for l in lines:                      # a fraud is recorded payment by payment: its last line counts
        if l["kind"] in DETECTS:
            last[l.get("episode", len(last))] = l
    frauds = list(last.values())
    return run, (progress[-1] if progress else {"committed": 0, "ts": run["started"]}), frauds


def read_alerts(topic, bootstrap, since):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"scoreboard-{uuid.uuid4()}",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    md = c.list_topics(topic, timeout=10).topics.get(topic)
    if md is None or md.error is not None or not md.partitions:
        c.close()
        return None
    tps = [TopicPartition(topic, p, int(since * 1000)) for p in md.partitions]
    starts = c.offsets_for_times(tps, timeout=10)
    todo = {}
    for tp in starts:
        high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)[1]
        if tp.offset != OFFSET_END and 0 <= tp.offset < high:
            todo[tp.partition] = high
    alerts = []
    if todo:
        c.assign([tp for tp in starts if tp.partition in todo])
        deadline = time.time() + 60
        while todo and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None:
                # read_committed: the last offsets may be transaction markers, never delivered
                if all(c.position([TopicPartition(topic, p)])[0].offset >= todo[p] - 1 for p in todo):
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())
            if msg.value():
                try:
                    alerts.append(json.loads(msg.value()))
                except ValueError:
                    pass
            if msg.offset() + 1 >= todo.get(msg.partition(), 0):
                todo.pop(msg.partition(), None)
    c.close()
    return alerts


def consumer_groups(bootstrap):
    """Lag of every consumer group that reads a CDC topic of the transactions."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"scoreboard-{uuid.uuid4()}"})
    rows = []
    try:
        groups = [g.group_id for g in admin.list_consumer_groups().result(timeout=10).valid
                  if not g.group_id.startswith(("scoreboard-", "connect-"))]
        if groups:
            from confluent_kafka import ConsumerGroupTopicPartitions
            desc = admin.describe_consumer_groups(groups)
            for g in groups:
                fut = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g)])[g]
                tps = [tp for tp in fut.result(timeout=10).topic_partitions
                       if tp.topic.endswith(".transactions") and tp.offset >= 0]
                if not tps:
                    continue
                members = len(desc[g].result(timeout=10).members)
                by_topic = defaultdict(int)
                parts = defaultdict(int)
                for tp in tps:
                    high = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)[1]
                    by_topic[tp.topic] += max(0, high - tp.offset)
                    parts[tp.topic] += 1
                for t in by_topic:
                    rows.append((g, t, parts[t], members, by_topic[t]))
    except Exception as e:
        print(f"  (consumer groups not available: {e})")
    finally:
        c.close()
    return rows


def health(since):
    try:
        with psycopg.connect(**CONN) as db:
            return db.execute("SELECT extract(epoch FROM ts)::float8, instance, pid, model, events, users, out_of_order, lag_s "
                              "FROM agent_health WHERE ts >= to_timestamp(%s) ORDER BY ts", (since,)).fetchall()
    except psycopg.Error:
        return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="fraud-alerts")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--timeline", action="store_true")
    a = ap.parse_args()

    run, progress, frauds = read_ground_truth()
    t0 = run["started"]
    end = progress["ts"]
    print(f"Run {run['run']}: started {time.strftime('%H:%M:%S', time.localtime(t0))}, "
          f"{progress['committed']:,} transactions in {end - t0:.0f} s "
          f"({progress['committed'] / max(1, end - t0):,.0f}/s), {len(frauds)} frauds so far"
          + ("" if progress.get("final") else " (still running)"))

    print("\nConsumer groups on the CDC topic(s)   [R2]")
    groups = consumer_groups(a.bootstrap)
    if groups:
        print(f"  {'group':<24} {'topic':<34} {'partitions':>10} {'members':>8} {'lag':>9}")
        for g, t, p, m, lag in sorted(groups):
            print(f"  {g:<24} {t:<34} {p:>10} {m:>8} {lag:>9,}")
    else:
        print("  none yet")

    rows = health(t0)
    last = {}
    for ts, inst, pid, model, events, users, ooo, lag in rows:
        last[(inst, pid)] = (ts, model, events, users, ooo, lag)
    print("\nAgent instances (telemetry of this run)   [R3, R4]")
    if last:
        print(f"  {'instance':<12} {'pid':>7} {'model':<9} {'transactions':>13} {'cardholders':>12} "
              f"{'out-of-order':>13} {'lag (s)':>8} {'seen':>9}")
        for (inst, pid), (ts, model, events, users, ooo, lag) in sorted(last.items(), key=lambda x: (x[1][1], x[0])):
            print(f"  {inst:<12} {pid:>7} {model:<9} {events:>13,} {users:>12,} {ooo:>13,} {lag:>8.1f} "
                  f"{time.time() - ts:>7.0f}s ago")
    else:
        print("  no telemetry yet (are the agents running?)")

    alerts = read_alerts(a.topic, a.bootstrap, t0)
    if alerts is None:
        print(f"\nTopic {a.topic!r} not found: no agent has published an alert yet.")
        return
    alerts = [x for x in alerts if isinstance(x, dict) and x.get("tx_id", 0) > run["after_id"]]
    fraud_tx = {m: set() for m in DETECTS.values()}
    for f in frauds:
        fraud_tx[DETECTS[f["kind"]]].update(f["tx_ids"])

    print(f"\nAlerts on {a.topic!r} (committed, this run)")
    for model in ("velocity", "anomaly"):
        mine = [x for x in alerts if x.get("model") == model]
        ids = Counter(x["tx_id"] for x in mine)
        flagged = set(ids)
        episodes = [f for f in frauds if DETECTS[f["kind"]] == model]
        # a card-testing fraud cut short by the end of the run may be too short to be detectable
        episodes = [f for f in episodes if f["kind"] != "card_testing" or len(f["tx_ids"]) >= VelocityModel.threshold]
        caught = [f for f in episodes if flagged & set(f["tx_ids"])]
        windows = [(f["user_id"], f["start"] - 1, f["end"] + VelocityModel.WINDOW_S + 1)
                   for f in frauds if f["kind"] == "card_testing"]

        def under_attack(x):             # the cardholder's own payment while the stolen card is being tested
            t = (x.get("tx_created_at") or 0) / 1e6
            return model == "velocity" and any(u == x["user_id"] and lo <= t <= hi for u, lo, hi in windows)
        false = len({x["tx_id"] for x in mine if x["tx_id"] not in fraud_tx[model] and not under_attack(x)})
        dups = sum(n - 1 for n in ids.values())
        lat = sorted(x["detected_at"] - x["tx_created_at"] / 1e6 for x in mine
                     if isinstance(x.get("tx_created_at"), int))
        p95 = lat[min(len(lat) - 1, int(0.95 * len(lat)))] if lat else None
        kind = [k for k, m in DETECTS.items() if m == model][0]
        print(f"\n  {model} model ({kind.replace('_', ' ')})")
        if not episodes:
            print("    no fraud of this kind yet: let the card processor run longer")
            continue
        recall = len(caught) / len(episodes)
        print(f"    R3  frauds caught          {len(caught)}/{len(episodes)}  ({recall:.0%})   "
              f"{ok(recall >= 0.9)}  (>= 90 %)")
        print(f"    R3  false alerts           {false}   {ok(false <= max(2, 0.1 * len(flagged)))}  "
              f"(at most 10 % of the flagged transactions)")
        if p95 is None:              # nothing to time: judge freshness by the agents' lag instead
            lags = [lag for _, _, _, m, _, _, _, lag in rows if m == model]
            if lags:
                print(f"    R4  no alerts; worst lag of the {model} agents {max(lags):.1f} s   "
                      f"{ok(max(lags) <= FRESH_S)}  (<= {FRESH_S} s)")
            else:
                print(f"    R4  no alerts and no telemetry   {ok(False)}")
        else:
            print(f"    R4  alert latency p50/p95  {statistics.median(lat):.1f} s / {p95:.1f} s   "
                  f"{ok(p95 <= FRESH_S)}  (p95 <= {FRESH_S} s after the commit)")
        print(f"        duplicate alerts       {dups}   (info: alerts case management got twice)")

    if a.timeline:
        print("\nLag over time, worst instance per model (s)   [R4]")
        buckets = defaultdict(dict)
        for ts, inst, pid, model, events, users, ooo, lag in rows:
            b = int((ts - t0) // 15) * 15
            buckets[b][model] = max(buckets[b].get(model, 0), lag)
        print(f"  {'t (s)':>6} {'velocity':>9} {'anomaly':>9}")
        for b in sorted(buckets):
            v = buckets[b]
            print(f"  {b:>6} " + " ".join(f"{v[m]:>9.1f}" if m in v else f"{'-':>9}" for m in ("velocity", "anomaly")))


if __name__ == "__main__":
    main()

"""Scores the current run of the plant (Part 2).

Reads the ground truth of the plant, the anomaly events of a Kafka topic and
the model telemetry of the run, and checks the measurable requirements.

    python3 scoreboard.py                         # topic "anomalies"
    python3 scoreboard.py --reset                 # empty the model telemetry

Only events and rows of the current run count (since the last start of plant.py).
"""
import argparse
import json
import statistics
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime

from confluent_kafka import OFFSET_END, Consumer, KafkaException, TopicPartition

import services
from plant import GROUND_TRUTH

EPISODE_GAP_S = 30          # anomalies of one machine closer than this belong to the same episode


def ok(cond):
    return "PASS" if cond else "FAIL"


def read_topic(topic, bootstrap, since):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"scoreboard-{uuid.uuid4()}",
                  "enable.auto.commit": False})
    md = c.list_topics(topic, timeout=10).topics.get(topic)
    if md is None or md.error is not None or not md.partitions:
        c.close()
        return None
    tps = [TopicPartition(topic, p, int(since * 1000)) for p in md.partitions]
    starts = c.offsets_for_times(tps, timeout=10)                 # first event produced since the run started
    todo = {}
    for tp in starts:
        high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)[1]
        if tp.offset != OFFSET_END and 0 <= tp.offset < high:
            todo[tp.partition] = high
    events = []
    if todo:
        c.assign([tp for tp in starts if tp.partition in todo])
        deadline = time.time() + 60
        while todo and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            if msg.value():
                try:
                    events.append(json.loads(msg.value()))
                except ValueError:
                    pass
            if msg.offset() + 1 >= todo.get(msg.partition(), 0):
                todo.pop(msg.partition(), None)
    c.close()
    return events


def episodes(timestamps):
    eps = []
    for t in sorted(timestamps):
        if eps and t - eps[-1][-1] <= EPISODE_GAP_S:
            eps[-1].append(t)
        else:
            eps.append([t])
    return eps


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="anomalies")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--reset", action="store_true", help="delete the model telemetry, then exit")
    args = ap.parse_args()
    conn = services.connect()
    if args.reset:
        conn.execute("TRUNCATE detector_health")
        print("detector_health emptied")
        return
    if not GROUND_TRUTH.exists():
        raise SystemExit("no ground_truth.jsonl: start plant.py first")
    lines = [json.loads(l) for l in GROUND_TRUTH.read_text().splitlines() if l.strip()]
    run, faults = lines[0], lines[1:]
    now, start = time.time(), run["start"]
    print(f"Run {run['run']}: started {datetime.fromtimestamp(start):%H:%M:%S} ({(now - start) / 60:.1f} min ago), "
          f"{run['machines']} machines x {run['hz']:g} Hz, {len(faults)} faults so far\n")

    # ------------------------------------------------------------ detection
    events = read_topic(args.topic, args.bootstrap, start)
    if events is None:
        print(f"Topic {args.topic!r} does not exist (yet).")
        events = []
    events = [e for e in events if e.get("run") == run["run"]]
    ids = Counter(e.get("anomaly_id") for e in events)
    by_machine = defaultdict(list)
    for e in events:
        by_machine[e["machine_id"]].append(e)

    print(f"Detection  (topic {args.topic}: {len(events):,} anomaly events, "
          f"models {sorted(set(e.get('model') for e in events)) or '-'})")
    print(f"  {'#':>2}  {'machine':8} {'start':>6} {'detected':>9} {'delay':>7}")
    scored, delays = 0, []
    for i, f in enumerate(faults, 1):
        m = f["machine_id"]
        hits = [e["reading_ts"] for e in by_machine[m] if f["start"] <= e["reading_ts"] <= f["end"] + 10]
        if now < f["start"] + 30:
            print(f"  {i:>2}  {m:8} {f['start'] - start:>5.0f}s  (just started, not scored yet)")
            continue
        scored += 1
        delay = min(hits) - f["start"] if hits else None
        if delay is not None:
            delays.append(delay)
        print(f"  {i:>2}  {m:8} {f['start'] - start:>5.0f}s {'yes' if hits else 'NO':>9} "
              f"{f'{delay:.1f} s' if delay is not None else '-':>7}")

    false_eps = 0
    for m, evs in by_machine.items():
        windows = [(f["start"], f["end"] + EPISODE_GAP_S) for f in faults if f["machine_id"] == m]
        outside = [e["reading_ts"] for e in evs if not any(a <= e["reading_ts"] <= b for a, b in windows)]
        false_eps += len(episodes(outside))
    lat = sorted(e["detected_at"] - e["reading_ts"] for e in events if "detected_at" in e)
    detected = len(delays)
    print(f"\n  faults detected: {detected}/{scored}"
          + (f", mean delay {statistics.mean(delays):.1f} s" if delays else "")
          + f"; false alarms (episodes on healthy machines): {false_eps}")
    if lat:
        p50, p95 = lat[len(lat) // 2], lat[int(len(lat) * 0.95) - 1 if len(lat) > 1 else 0]
        note = "  (replayed data? latency only meaningful for live processing)" if p50 > 60 else ""
        print(f"  latency reading -> anomaly event: p50 {p50:.2f} s, p95 {p95:.2f} s, max {lat[-1]:.2f} s{note}")
    dups = sum(n - 1 for n in ids.values() if n > 1)
    print(f"  duplicate anomaly events (same anomaly_id): {dups}")

    # ------------------------------------------------------------ model telemetry
    health = conn.execute("SELECT instance, pid, model, readings, missing, out_of_order, machines, "
                          "extract(epoch FROM now() - updated_at) FROM detector_health "
                          "WHERE updated_at >= to_timestamp(%s) ORDER BY instance, pid", (start,)).fetchall()
    print("\nDetector instances  (model telemetry, this run)")
    if not health:
        print("  none reported")
    for inst, pid, mv, n, miss, ooo, nm, age in health:
        state = "alive" if age < 15 else f"silent for {age:.0f} s"
        print(f"  {inst:14} pid {pid:<7} {mv}  readings {n:>9,}  missing {miss:>7,}  out-of-order {ooo:>6,}  "
              f"machines {nm:>4}  {state}")
    missing = sum(h[4] for h in health)
    ooo = sum(h[5] for h in health)

    # ------------------------------------------------------------ requirements
    print("\nRequirements (measurable part)")
    print(f"  [{ok(health and missing == 0 and ooo == 0)}] R2 complete and ordered: "
          f"{missing:,} readings missing, {ooo:,} out of order")
    print(f"  [{ok(lat and p95 < 5)}] R3 fresh: p95 latency < 5 s")
    print(f"  [{ok(scored and detected == scored and false_eps <= 2)}] detection: all faults found, "
          f"at most 2 false alarms")


if __name__ == "__main__":
    main()

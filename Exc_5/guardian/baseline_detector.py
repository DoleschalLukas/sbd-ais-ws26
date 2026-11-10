"""Today's detector (Part 2, Stage A): polls PostgreSQL for new readings.

It remembers the highest `id` it has read and asks for everything above it,
scores every reading with the model, and prints the anomalies. Every 5 s it
prints how it is doing.

    python3 baseline_detector.py
"""
import argparse
import time

import services
from model import BearingModel

ap = argparse.ArgumentParser()
ap.add_argument("--name", default="baseline")
ap.add_argument("--quiet", action="store_true", help="do not print every anomaly")
args = ap.parse_args()

model = BearingModel(instance=args.name)
conn = services.connect()
last_id = conn.execute("SELECT coalesce(max(id), 0) FROM readings").fetchone()[0]   # start with new readings
cols = ["id", "run", "machine_id", "seq", "ts", "vibration", "temperature", "current"]
print(f"[{args.name}] polling table readings for id > {last_id} (Ctrl+C to stop)")

read = anomalies = 0
report_at = time.time() + 5
try:
    while True:
        rows = conn.execute(f"SELECT {', '.join(cols)} FROM readings WHERE id > %s ORDER BY id LIMIT 2000",
                            (last_id,)).fetchall()
        for row in rows:
            r = dict(zip(cols, row))
            score = model.score(r)
            if score is not None and score >= model.threshold:
                anomalies += 1
                if not args.quiet:
                    print(f"[{args.name}] ANOMALY {r['machine_id']} seq={r['seq']} score={score}")
            last_id = r["id"]
        read += len(rows)
        if time.time() >= report_at:
            newest = conn.execute("SELECT max(id) FROM readings").fetchone()[0] or 0
            delay = time.time() - r["ts"] if rows else 0
            print(f"[{args.name}] read {read:>8,}  backlog {newest - last_id:>7,} rows  "
                  f"delay {delay:5.1f} s  missing {model.missing:>6,}  anomalies {anomalies}")
            report_at += 5
        if not rows:
            time.sleep(0.2)
except KeyboardInterrupt:
    pass
finally:
    print(f"[{args.name}] stopped: read {read:,} readings, last id {last_id:,}, "
          f"{model.missing:,} readings missing, {anomalies} anomalies")

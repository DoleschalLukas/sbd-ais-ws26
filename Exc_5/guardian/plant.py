"""Simulates the machines of the plant and their gateways.

Every machine sends one sensor reading every 1/--hz seconds:

    {"run": 1792065600, "machine_id": "M-017", "seq": 4711, "ts": 1792066071.31,
     "vibration": 2.31, "temperature": 61.4, "current": 12.7}

seq counts the readings of one machine (0, 1, 2, ...); ts is the time the
reading was taken (epoch seconds). Every start of the plant is a new run.

Every --fault-every seconds one machine develops a bearing fault: its vibration
(and a bit later its temperature) rises slowly for --fault-length seconds, then
the machine is repaired. Healthy machines sometimes show a single spike (a
forklift bumps into them): that is not a fault. The faults are written to
ground_truth.jsonl, for the scoreboard only (your code must not read it).

Sinks:
    --sink postgres   today's setup: 4 line gateways INSERT into the table `readings`
    --sink kafka      your setup (Part 2, Stage C): implement KafkaSink below

Examples:
    python3 plant.py --sink postgres
    python3 plant.py --sink kafka --machines 60 --hz 10
"""
import argparse
import json
import queue
import random
import threading
import time
from datetime import date, timedelta
from pathlib import Path

import services

GROUND_TRUTH = Path(__file__).resolve().parent / "ground_truth.jsonl"
LINES = 4


# ---------------------------------------------------------------- sinks
class PostgresSink:
    """Today's setup: each production line has a gateway that writes its machines'
    readings into PostgreSQL, one transaction per batch."""

    def __init__(self, args):
        self.queues = [queue.Queue() for _ in range(LINES)]
        self.errors = []
        self._batch = {}
        for line in range(LINES):
            threading.Thread(target=self._gateway, args=(line,), daemon=True).start()

    def _gateway(self, line):
        rng = random.Random(line)
        with services.connect(autocommit=False) as conn:
            while True:
                batch = self.queues[line].get()
                while not self.queues[line].empty():            # catch up: everything that is waiting
                    batch += self.queues[line].get()
                try:
                    with conn.transaction(), conn.cursor() as cur:
                        cur.executemany(
                            "INSERT INTO readings (run, machine_id, seq, ts, vibration, temperature, current) "
                            "VALUES (%(run)s, %(machine_id)s, %(seq)s, %(ts)s, %(vibration)s, %(temperature)s, %(current)s)",
                            batch)
                        time.sleep(rng.uniform(0, 0.12))            # gateway work + network before COMMIT
                except Exception as e:
                    self.errors.append(e)

    def send(self, reading):
        line = int(reading["machine_id"][2:]) % LINES
        self._batch.setdefault(line, []).append(reading)

    def tick(self):
        """Called once per tick, after all readings of that tick were sent."""
        for line, batch in self._batch.items():
            self.queues[line].put(batch)
        self._batch = {}
        if self.errors:
            raise self.errors[0]

    def close(self):
        time.sleep(0.5)


class KafkaSink:
    """Your setup: the gateways send every reading to Kafka.

    TODO (Part 2, Stage C1). clients/producer.py of Part 1 shows
    how to produce. Decide, and justify in your decision record: which topic
    (created how?), which key, which producer settings?
    """

    def __init__(self, args):
        from confluent_kafka import Producer
        self.producer = Producer({
            "bootstrap.servers": args.bootstrap,
            # TODO: more settings?
        })

    def send(self, reading):
        # TODO: send the reading (as JSON) to Kafka: self.producer.produce(...)
        raise NotImplementedError("KafkaSink.send() is part of your solution (Stage C1)")

    def tick(self):
        """Called once per tick, after all readings of that tick were sent."""
        self.producer.poll(0)

    def close(self):
        self.producer.flush(10)


# ---------------------------------------------------------------- simulation
def register_machines(n, rng):
    types = ["CNC milling", "CNC lathe", "injection moulding", "grinding"]
    rows = []
    for i in range(1, n + 1):
        installed = date(2012, 1, 1) + timedelta(days=rng.randint(0, 4000))
        rows.append((f"M-{i:03d}", f"L{i % LINES + 1}", rng.choice(types), installed,
                     date(2026, 1, 1) + timedelta(days=rng.randint(0, 280)), rng.choice(["A", "B", "B", "C"])))
    with services.connect() as conn, conn.cursor() as cur:
        cur.executemany("INSERT INTO machines VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (machine_id) DO UPDATE SET "
                        "line = EXCLUDED.line, type = EXCLUDED.type, installed = EXCLUDED.installed, "
                        "last_maintenance = EXCLUDED.last_maintenance, criticality = EXCLUDED.criticality", rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sink", choices=["postgres", "kafka"], required=True)
    ap.add_argument("--machines", type=int, default=60)
    ap.add_argument("--hz", type=float, default=10, help="readings per machine and second")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run, 0 = until Ctrl+C")
    ap.add_argument("--fault-every", type=float, default=60, help="seconds between two faults")
    ap.add_argument("--fault-length", type=float, default=45, help="seconds until a faulty machine is repaired")
    ap.add_argument("--first-fault", type=float, default=40, help="seconds until the first fault")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()
    rng = random.Random(args.seed)

    register_machines(args.machines, random.Random(0))
    sink = PostgresSink(args) if args.sink == "postgres" else KafkaSink(args)
    ids = [f"M-{i:03d}" for i in range(1, args.machines + 1)]
    normal = {m: dict(vibration=(rng.uniform(1.5, 3.0), 0.25), temperature=(rng.uniform(55, 70), 0.5),
                      current=(rng.uniform(10, 15), 0.4)) for m in ids}
    seq = dict.fromkeys(ids, 0)
    faults = {}                                  # machine -> start time of its current fault
    faulted = set()                              # machines that had a fault in this run
    run = int(time.time())
    start = time.time()
    next_fault = start + args.first_fault
    spike_p = 0.5 / (args.machines * args.hz)   # about one spike every 2 s in the whole plant
    with GROUND_TRUTH.open("w") as gt:
        gt.write(json.dumps({"run": run, "start": start, "machines": args.machines, "hz": args.hz}) + "\n")
    print(f"run {run}: {args.machines} machines x {args.hz:g} Hz = {args.machines * args.hz:,.0f} readings/s "
          f"-> {args.sink} (Ctrl+C to stop)")

    sent, tick, report_at = 0, 0, start + 5
    try:
        while not args.duration or time.time() - start < args.duration:
            now = time.time()
            if now >= next_fault:
                m = rng.choice([x for x in ids if x not in faulted] or [x for x in ids if x not in faults])
                faults[m] = now
                faulted.add(m)
                with GROUND_TRUTH.open("a") as gt:
                    gt.write(json.dumps({"machine_id": m, "start": now, "end": now + args.fault_length}) + "\n")
                print(f"  t={now - start:5.0f}s  fault starts on {m} (repaired in {args.fault_length:.0f} s)")
                next_fault = now + args.fault_every
            for m, t0 in list(faults.items()):
                if now - t0 > args.fault_length:
                    del faults[m]
            for m in ids:
                r = {"run": run, "machine_id": m, "seq": seq[m], "ts": round(now, 3)}
                for f, (mu, sigma) in normal[m].items():
                    r[f] = rng.gauss(mu, sigma)
                if m in faults:                              # bearing wear: slow drift
                    age = now - faults[m]
                    r["vibration"] += 0.05 * age
                    r["temperature"] += 0.04 * max(0.0, age - 5)
                elif rng.random() < spike_p:                 # a bump, not a fault
                    r["vibration"] += 4.0
                for f in normal[m]:
                    r[f] = round(r[f], 3)
                sink.send(r)
                seq[m] += 1
            sink.tick()
            sent += len(ids)
            tick += 1
            delay = start + tick / args.hz - time.time()
            if now >= report_at:
                print(f"  t={now - start:5.0f}s  {sent:>9,} readings sent ({sent / (now - start):,.0f}/s)"
                      + (f"  faulty: {', '.join(sorted(faults))}" if faults else "")
                      + (f"  WARNING: plant {-delay:.1f} s behind schedule (sink too slow?)" if delay < -1 else ""))
                report_at += 5
            if delay > 0:
                time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()
        print(f"run {run}: sent {sent:,} readings in {time.time() - start:.0f} s")


if __name__ == "__main__":
    main()

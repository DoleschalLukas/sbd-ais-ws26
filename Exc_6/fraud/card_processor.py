"""The card processor: commits card transactions into PostgreSQL. Do not change this file.

PostgreSQL (database `mydb`, table `transactions`) is the card processor's
system of record. Every 100 ms the processor commits one batch of card
payments as one database transaction:

    id | user_id | amount | card_type | merchant_id | created_at

100,000 cardholders pay at 5,000 merchants; every cardholder has their own
usual spending level. Every --fraud-every seconds a fraud starts, alternately:

  - card testing: a stolen card is tried with 12 small payments within ~20 s
    (the velocity model should catch it);
  - account takeover: one payment far above what the cardholder usually
    spends (the anomaly model should catch it).

The frauds are written to ground_truth.jsonl, for the scoreboard only (your
code must not read it; in real life it is only known after the chargeback).

Every start is a new run. Lab simplification: the table only holds the
current run (it is emptied at the start and on Ctrl+C), so that the snapshot
of a new connector stays small. A real system of record keeps its history.

    python3 card_processor.py                 # ~1,000 transactions/s
    python3 card_processor.py --tps 500       # a slower laptop
"""
import argparse
import json
import math
import random
import time
from pathlib import Path

import psycopg

GROUND_TRUTH = Path(__file__).resolve().parent / "ground_truth.jsonl"
CONN = dict(host="127.0.0.1", port=5432, dbname="mydb", user="postgres", password="postgrespw")
USERS = range(100_000, 200_000)
MERCHANTS = 5_000
TICK_S = 0.1
BURST_TX, BURST_GAP_S = 12, (1.2, 2.0)        # card testing: 12 payments, one every 1.2-2 s
INSERT = "INSERT INTO transactions (user_id, amount, card_type, merchant_id) VALUES (%s, %s, %s, %s)"


def cardholder(uid):
    """The usual spending level and card of a cardholder (always the same for one user_id)."""
    r = random.Random(uid)
    return r.lognormvariate(math.log(50), 0.8), r.choice(["VISA", "MASTERCARD", "AMEX"])


def setup(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id INT,
            amount NUMERIC(10,2),
            card_type VARCHAR(20),
            merchant_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    conn.execute("TRUNCATE transactions")
    return conn.execute("SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END "
                        "FROM transactions_id_seq").fetchone()[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tps", type=int, default=1000, help="transactions per second")
    ap.add_argument("--fraud-every", type=float, default=8, help="seconds between two frauds")
    ap.add_argument("--first-fraud", type=float, default=30, help="seconds until the first fraud")
    a = ap.parse_args()
    rng = random.Random()
    per_tick = max(1, round(a.tps * TICK_S))

    with psycopg.connect(**CONN, autocommit=True) as conn:
        after_id = setup(conn)
        run = int(time.time())
        started = time.time()
        gt = GROUND_TRUTH.open("w")
        gt.write(json.dumps({"kind": "run", "run": run, "after_id": after_id, "started": started,
                             "tps": a.tps}) + "\n")
        gt.flush()
        print(f"run {run}: {a.tps:,} transactions/s, a fraud every {a.fraud_every:g} s "
              f"(first after {a.first_fraud:g} s). Ctrl+C to stop.", flush=True)

        seen = {}                  # user_id -> payments in this run (to pick takeover victims with a history)
        robbed = set()             # takeover victims of this run: never the same cardholder twice (their
                                   # history would hold the first takeover and hide the second one)
        bursts = []                # open card-testing frauds: dict(kind, episode, user, due, left, ids, start)
        next_fraud, kind, episodes = started + a.first_fraud, "card_testing", 0
        committed, report_at, tick_at = 0, started + 5, started
        try:
            while True:
                now = time.time()
                rows = []
                for _ in range(per_tick):
                    uid = rng.choice(USERS)
                    base, card = cardholder(uid)
                    rows.append((uid, round(max(1.0, base * rng.lognormvariate(0, 0.35)), 2), card,
                                 rng.randint(1, MERCHANTS)))
                fraud = []         # (episode, row)
                if now >= next_fraud:
                    if kind == "card_testing":
                        episodes += 1
                        bursts.append(dict(kind="card_testing", episode=episodes, user=rng.choice(USERS), due=now,
                                           left=BURST_TX, ids=[], start=now))
                        kind = "account_takeover"
                    else:
                        # >= 3 earlier payments: what the anomaly model needs. Not more: early in a run, only
                        # unusually busy cardholders reach 5, and they trip the velocity model by themselves
                        victims = [u for u, n in seen.items() if n >= 3 and u not in robbed]
                        if victims:
                            uid = rng.choice(victims)
                            robbed.add(uid)
                            base, card = cardholder(uid)
                            amount = round(max(600.0, base * rng.uniform(12, 25)), 2)
                            episodes += 1
                            fraud.append((dict(kind="account_takeover", episode=episodes, user=uid, left=0, ids=[],
                                               start=now),
                                          (uid, amount, card, rng.randint(1, MERCHANTS))))
                            kind = "card_testing"
                    next_fraud += a.fraud_every
                for b in bursts:
                    if b["left"] and now >= b["due"]:
                        fraud.append((b, (b["user"], round(rng.uniform(1, 9.99), 2), cardholder(b["user"])[1],
                                          rng.randint(1, MERCHANTS))))
                        b["left"] -= 1
                        b["due"] = now + rng.uniform(*BURST_GAP_S)

                with conn.transaction(), conn.cursor() as cur:       # one batch = one database transaction
                    cur.executemany(INSERT, rows)
                    for ep, row in fraud:
                        cur.execute(INSERT + " RETURNING id", row)
                        ep["ids"].append(cur.fetchone()[0])
                committed += len(rows) + len(fraud)
                for uid, *_ in rows:
                    seen[uid] = seen.get(uid, 0) + 1

                # every fraud payment is recorded as soon as it is committed (the last line of an episode counts)
                for ep in {id(ep): ep for ep, _ in fraud}.values():
                    gt.write(json.dumps({"kind": ep["kind"], "episode": ep["episode"], "user_id": ep["user"],
                                         "tx_ids": ep["ids"], "start": ep["start"], "end": time.time(),
                                         "complete": not ep["left"]}) + "\n")
                if fraud:
                    gt.flush()
                bursts = [b for b in bursts if b["left"]]
                if now >= report_at:
                    rate = committed / (now - started)
                    print(f"{time.strftime('%H:%M:%S')}  {committed:>9,} transactions  {rate:>6,.0f}/s", flush=True)
                    gt.write(json.dumps({"kind": "progress", "committed": committed, "ts": now}) + "\n")
                    gt.flush()
                    report_at += 5
                tick_at += TICK_S
                time.sleep(max(0.0, tick_at - time.time()))
        except KeyboardInterrupt:
            pass
        finally:
            gt.write(json.dumps({"kind": "progress", "committed": committed, "ts": time.time(), "final": True}) + "\n")
            gt.close()
            conn.execute("TRUNCATE transactions")
            print(f"stopped after {committed:,} transactions (table emptied for the next run)")


if __name__ == "__main__":
    main()

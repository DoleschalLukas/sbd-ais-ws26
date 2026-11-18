"""Fraud models of the data science team. Do not change this file.

Usage:
    model = make_model("velocity", instance="vel-1")    # or "anomaly"
    score = model.score(tx, ts)        # tx = the transaction row as committed in PostgreSQL
                                       # ts = when the transaction happened (epoch seconds)
    if score >= model.threshold:
        alert = model.alert_event(tx, score)

Both models are stateful, PER CARDHOLDER (user_id): they need every
transaction of a cardholder, in the order of `ts`.

    anomaly   keeps the last 50 amounts of every cardholder; scores a payment
              of at least 500 by how many times larger it is than the
              cardholder's average (needs 3 earlier payments). Threshold 5.
              Inference: 0.3 ms per transaction.
    velocity  counts the cardholder's transactions in the last 60 s (by ts),
              including this one. Threshold 8.
              Inference: 2 ms per transaction (a bigger model in production).

Every model instance reports its throughput, the number of cardholders it
keeps state for, transactions that arrive out of order, and its lag (how old
the transaction it just scored is) to the table `agent_health` every 5 s.
"""
import os
import time
from collections import deque

HEALTH_EVERY_S = 5


class _Model:
    name, threshold, cost_ms = None, None, None

    def __init__(self, instance):
        self.instance = instance
        self.state = {}
        self.events = self.out_of_order = 0
        self.lag_s = 0.0
        self._health_at = time.time()
        self._db = None

    def score(self, tx, ts):
        """Score one transaction. Returns a number; >= self.threshold means fraud."""
        time.sleep(self.cost_ms / 1000)                          # inference time
        if not isinstance(tx.get("amount"), (int, float)):
            raise TypeError(f"amount must be a number, got {tx.get('amount')!r} ({type(tx.get('amount')).__name__}). "
                            "Look at the schema of the CDC event.")
        if not isinstance(ts, (int, float)) or not 1e9 < ts < 1e10:
            raise ValueError(f"ts must be epoch seconds (like {time.time():.1f}), got {ts!r}")
        self.events += 1
        if isinstance(tx.get("created_at"), int):               # MicroTimestamp from Debezium
            self.lag_s = time.time() - tx["created_at"] / 1e6
        result = self._score(tx, ts)
        self._health()
        return result

    def alert_event(self, tx, score):
        """The alert in the format case management expects (see README)."""
        return {
            "alert_id": f"{self.name}:{tx['id']}",
            "model": self.name,
            "tx_id": tx["id"],
            "user_id": tx["user_id"],
            "amount": tx["amount"],
            "score": round(score, 2),
            "tx_created_at": tx.get("created_at"),
            "detected_at": time.time(),
            "instance": self.instance,
        }

    def users(self):
        """The cardholders this model instance currently keeps state for."""
        return list(self.state)

    def forget(self, user_id):
        """Drop the state of one cardholder (e.g. when another instance takes it over)."""
        self.state.pop(user_id, None)

    def _health(self):
        now = time.time()
        if self._db is False or now - self._health_at < HEALTH_EVERY_S:
            return
        self._health_at = now
        try:
            if self._db is None:
                import psycopg
                self._db = psycopg.connect(host="127.0.0.1", port=5432, dbname="mydb", user="postgres",
                                           password="postgrespw", autocommit=True)
                try:
                    self._db.execute("CREATE TABLE IF NOT EXISTS agent_health (ts TIMESTAMPTZ DEFAULT now(), "
                                     "instance TEXT, pid INT, model TEXT, events BIGINT, users INT, "
                                     "out_of_order BIGINT, lag_s REAL)")
                except (psycopg.errors.UniqueViolation, psycopg.errors.DuplicateTable):
                    pass                                         # another agent created it at the same moment
            self._db.execute("INSERT INTO agent_health (instance, pid, model, events, users, out_of_order, lag_s) "
                             "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                             (self.instance, os.getpid(), self.name, self.events, len(self.state),
                              self.out_of_order, self.lag_s))
        except Exception as e:                                   # telemetry must never stop the model
            print(f"[model] telemetry disabled: {e}")
            self._db = False


class AnomalyModel(_Model):
    name, threshold, cost_ms = "anomaly", 5.0, 0.3

    def _score(self, tx, ts):
        h = self.state.setdefault(tx["user_id"], deque(maxlen=50))
        score = 0.0
        if len(h) >= 3 and tx["amount"] >= 500:
            score = tx["amount"] / (sum(h) / len(h))
        h.append(tx["amount"])
        return score


class VelocityModel(_Model):
    name, threshold, cost_ms = "velocity", 8, 2.0
    WINDOW_S = 60

    def _score(self, tx, ts):
        w = self.state.setdefault(tx["user_id"], deque())
        if w and ts < w[-1]:
            self.out_of_order += 1
        w.append(ts)
        while w and w[0] < ts - self.WINDOW_S:
            w.popleft()
        return len(w)


def make_model(name, instance):
    models = {"anomaly": AnomalyModel, "velocity": VelocityModel}
    if name not in models:
        raise ValueError(f"unknown model {name!r}, use one of {list(models)}")
    return models[name](instance)

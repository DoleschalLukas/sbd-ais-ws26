"""Bearing-fault model of the data science team. Do not change this file.

Usage:
    model = BearingModel(instance="det-1")
    score = model.score(reading)          # reading = dict as sent by the plant
    if score is not None and score >= model.threshold:
        event = model.anomaly_event(reading, score)

What you need to know:
  - The model is stateful, PER MACHINE: it learns each machine's normal
    vibration from its first 100 readings, then compares the last 50 readings
    with it. So it needs EVERY reading of a machine, in `seq` order.
  - It counts readings that arrive out of order or are missing (the
    scoreboard shows these counters).
  - Inference is not free: every call of score() takes 3 ms.
"""
import math
import os
import time
from collections import deque

WARMUP = 100                 # readings per machine (10 s at 10 Hz)
HEALTH_EVERY_S = 5
VERSIONS = {
    "v1": dict(window=50, threshold=5.0, features=("vibration",), clip=4.0, cost_ms=3.0),
}


class _Machine:
    def __init__(self, run, features, window):
        self.run = run
        self.next_seq = None
        self.n = 0
        self.mean = {f: 0.0 for f in features}     # baseline, learned during warm-up (Welford)
        self.m2 = {f: 0.0 for f in features}
        self.win = {f: deque(maxlen=window) for f in features}


class BearingModel:
    def __init__(self, version="v1", instance="detector"):
        if version not in VERSIONS:
            raise ValueError(f"unknown model version {version!r}, use one of {list(VERSIONS)}")
        p = VERSIONS[version]
        self.version, self.instance = version, instance
        self.window, self.threshold, self.features, self.cost_ms = p["window"], p["threshold"], p["features"], p["cost_ms"]
        self.clip = p["clip"]
        self.state = {}
        self.readings = self.missing = self.out_of_order = 0
        self._health_at = 0.0
        self._db = None

    # ------------------------------------------------------------ public API
    def score(self, reading):
        """Feed one reading. Returns the anomaly score, or None (warm-up, or reading ignored)."""
        time.sleep(self.cost_ms / 1000)                       # inference time
        self.readings += 1
        self._health()
        mid, seq = reading["machine_id"], reading["seq"]
        m = self.state.get(mid)
        if m is None or m.run != reading["run"]:              # first reading of this machine (in this run)
            m = self.state[mid] = _Machine(reading["run"], self.features, self.window)
        elif seq < m.next_seq:
            self.out_of_order += 1                            # older than what we already saw: ignored
            return None
        elif seq > m.next_seq:
            self.missing += seq - m.next_seq                  # readings we never got
        m.next_seq = seq + 1

        if m.n < WARMUP:                                      # learn the machine's normal behaviour
            m.n += 1
            for f in self.features:
                d = reading[f] - m.mean[f]
                m.mean[f] += d / m.n
                m.m2[f] += d * (reading[f] - m.mean[f])
            return None
        z = 0.0
        for f in self.features:
            std = math.sqrt(m.m2[f] / (m.n - 1)) or 1e-9
            x = reading[f]
            if self.clip:                                     # robust against single spikes
                x = min(x, m.mean[f] + self.clip * std)
            m.win[f].append(x)
            if len(m.win[f]) < self.window:
                return None
            se = std * math.sqrt(1 / self.window + 1 / m.n)   # window mean vs. learned baseline
            z = max(z, (sum(m.win[f]) / self.window - m.mean[f]) / se)
        return round(z, 2)

    def anomaly_event(self, reading, score):
        """The anomaly event in the format the maintenance team expects (see README)."""
        return {
            "anomaly_id": f"{reading['machine_id']}:{reading['run']}:{reading['seq']}:{self.version}",
            "machine_id": reading["machine_id"],
            "run": reading["run"],
            "seq": reading["seq"],
            "reading_ts": reading["ts"],
            "detected_at": time.time(),
            "model": self.version,
            "score": score,
            "vibration": reading["vibration"],
            "temperature": reading["temperature"],
        }

    def forget(self, machine_id):
        """Drop the state of one machine (e.g. when another instance takes it over)."""
        self.state.pop(machine_id, None)

    def machines(self):
        """The machines this model instance currently keeps state for."""
        return list(self.state)

    # ------------------------------------------------------------ telemetry
    def _health(self, force=False):
        now = time.time()
        if self._db is False or (not force and now - self._health_at < HEALTH_EVERY_S):
            return
        self._health_at = now
        try:
            if self._db is None:
                import services
                self._db = services.connect()
            self._db.execute(
                "INSERT INTO detector_health (instance, pid, model, readings, missing, out_of_order, machines, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, now()) ON CONFLICT (instance, pid) DO UPDATE SET "
                "model = EXCLUDED.model, readings = EXCLUDED.readings, missing = EXCLUDED.missing, "
                "out_of_order = EXCLUDED.out_of_order, machines = EXCLUDED.machines, updated_at = now()",
                (self.instance, os.getpid(), self.version, self.readings, self.missing, self.out_of_order,
                 len(self.state)))
        except Exception as e:                                # telemetry must never stop the model
            print(f"[model] telemetry disabled: {e}")
            self._db = False

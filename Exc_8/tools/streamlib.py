"""The test bench of Exercise 8: the crash report, live, for two consumers.

The Spark code (read the stream, compute the crash report, write the alerts) is in the notebook
exercise8.ipynb. This file only provides what you need to run and check it:

    import sys; sys.path.append("/home/jovyan/tools")
    from streamlib import check_all, show_alerts, produce, reset, crash_point, load_lake, read_lake

    check_all(start_query, pager, ops_view)
                                   -> runs your query on 2 minutes of live records (5 % of them late, by up to
                                      30 s), stops it once and restarts it, and prints what the two teams get:
                                      the on-call engineer reads crash_alerts through pager(messages), the
                                      operations team through ops_view(messages)
    check_all(start_query, pager, ops_report=ops_report)
                                   -> the same, the operations team reads ops_report() (architecture C)
    show_alerts(n)                 -> the last n messages of the topic `crash_alerts`
    produce(seconds)               -> writes live log records into `logs` (to try something by hand)
    reset()                        -> stops the query and deletes its checkpoint (the topics stay)
    crash_point(batch_id)          -> where check_all simulates a crash: after a micro-batch was written to
                                      Kafka, before Spark notes it in the checkpoint (otherwise it does nothing)

Exercise 7's job, for architecture C:

    load_lake()                    -> copies the new records of `logs` into the lake (Exercise 7, architecture B)
    read_lake()                    -> the lake, as a batch (Spark DataFrame)
"""
import contextlib
import io
import json
import math
import os
import random
import shutil
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lakelib as _lake  # noqa: E402  Exercise 7 (installs confluent-kafka and duckdb on first use)
import confluent_kafka.admin  # noqa: E402,F401

_ck = _lake._ck
TOPIC = "logs"
ALERTS = "crash_alerts"
BOOTSTRAP = _lake.BOOTSTRAP
DATA = _lake.DATA
LAKE = _lake.LAKE
CHECKPOINT = f"{DATA}/stream/checkpoint"   # = the checkpoint of write_alerts in the notebook

RATE = 2_000                # records per second written by produce() and check_all()
CRASH_EVERY = 10            # every 10th record mentions a crash
LATE = 0.05                 # 5 % of the records arrive late ...
MAX_LATE_S = 30             # ... by up to 30 s (event time in the past)
ON_CALL_MAX_S = 20          # the on-call engineer: an alert at most 20 s after the interval ends (p95)
OPS_MIN_CORRECT = 99.0      # the operations team: at least 99 % of the report rows correct

__all__ = ["check_all", "show_alerts", "produce", "reset", "crash_point", "load_lake", "read_lake", "CHECKPOINT"]

_spark = None


def get_spark():
    """The Spark session: the notebook's (getOrCreate returns it), or a new one with the same settings."""
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = (SparkSession.builder.appName("Exercise8")
                  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1")
                  .config("spark.sql.shuffle.partitions", "4")
                  .config("spark.sql.session.timeZone", "UTC")
                  .getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
        _lake._spark = _spark                       # Exercise 7's blocks use the same session
    return _spark


from pyspark.sql import functions as F  # noqa: E402


# ------------------------------------------------------------------ topics and offsets
def _ensure_topics():
    admin = _ck.admin.AdminClient({"bootstrap.servers": BOOTSTRAP})
    have = admin.list_topics(timeout=10).topics
    new = [_ck.admin.NewTopic(t, num_partitions=n, replication_factor=1)
           for t, n in ((TOPIC, 4), (ALERTS, 1)) if t not in have]
    for f in (admin.create_topics(new).values() if new else []):
        f.result()
    deadline = time.time() + 30                     # a new topic needs a moment until its partitions have a leader
    while new and time.time() < deadline:
        md = admin.list_topics(timeout=10).topics
        if all(t.topic in md and md[t.topic].partitions and
               all(p.leader >= 0 for p in md[t.topic].partitions.values()) for t in new):
            break
        time.sleep(0.5)


def _end_offsets(topic):
    c = _ck.Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "exercise8-offsets"})
    try:
        for attempt in range(30):                   # right after Kafka starts, a partition may not be ready yet
            try:
                parts = c.list_topics(topic, timeout=10).topics[topic].partitions
                return {p: c.get_watermark_offsets(_ck.TopicPartition(topic, p), timeout=10)[1]
                        for p in sorted(parts)}
            except _ck.KafkaException:
                if attempt == 29:
                    raise
                time.sleep(1)
    finally:
        c.close()


_ensure_topics()


# ------------------------------------------------------------------ the crash point and Exercise 7's job
class SimulatedStop(Exception):
    """Raised by crash_point when check_all simulates a crash of the query."""


_stop_armed = False
last_check = None                    # the numbers of the last check_all
_alerts_at_arming = 0


def _alerts_written():
    return sum(_end_offsets(ALERTS).values())


def crash_point(batch_id):
    """Call it in foreachBatch right after writing the micro-batch to Kafka. Spark notes the micro-batch in
    the checkpoint only after foreachBatch returns: a crash here means 'written, but not noted as done'.
    check_all uses it once to simulate exactly that crash; at any other time it does nothing."""
    if _stop_armed and _alerts_written() > _alerts_at_arming:     # this micro-batch wrote alerts
        raise SimulatedStop(f"stopped after writing micro-batch {batch_id}")


def load_lake():
    """Exercise 7's job (architecture B): copy the new records of `logs` into the lake, all or nothing."""
    start = _lake.lake_position()
    new = _lake.read_new(start)
    run = _lake.write_files(new, _lake.STAGING)
    _lake.publish(run)


def read_lake():
    """The lake of Exercise 7 as a batch DataFrame (the same columns as read_stream)."""
    return (get_spark().read.option("recursiveFileLookup", "true").parquet(LAKE)
            .withColumn("event_time", F.timestamp_millis("timestamp")))


def reset():
    """Stop the query and delete its checkpoint. The topics (and their records and alerts) stay."""
    for q in get_spark().streams.active:
        q.stop()
    shutil.rmtree(CHECKPOINT, ignore_errors=True)
    print("reset: query stopped, checkpoint deleted")


# ------------------------------------------------------------------ live records
_LEVELS = ["Low", "Medium", "High", "Critical"]
_WORDS = ["kernel", "buffer", "socket", "timeout", "disk", "memory", "login", "request", "cache", "thread"]


class _Feed(threading.Thread):
    """Writes live log records into `logs` at RATE records/s, LATE of them with an event time in the past.
    Remembers every severe crash (event time, user) for the reference report."""

    def __init__(self, seconds, rate=RATE, late=LATE, max_late_s=MAX_LATE_S, seed=None):
        super().__init__(daemon=True)
        self.seconds, self.rate, self.late, self.max_late_ms = seconds, rate, late, int(max_late_s * 1000)
        self.rnd = random.Random(seed)
        self.crashes, self.sent, self.late_sent = [], 0, 0
        self.start_ms = None

    def run(self):
        rnd, producer = self.rnd, _ck.Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
        t0 = time.time()
        self.start_ms = int(t0 * 1000)
        per_tick, tick = max(1, self.rate // 10), 0
        while time.time() - t0 < self.seconds:
            now_ms = int(time.time() * 1000)
            for _ in range(per_tick):
                ts, room = now_ms, min(self.max_late_ms, now_ms - self.start_ms)
                if room > 0 and rnd.random() < self.late:          # buffered on a phone, sent later
                    ts -= rnd.randint(1, room)
                    self.late_sent += 1
                crash = self.sent % CRASH_EVERY == 0
                sev, user = rnd.choice(_LEVELS), f"user_{rnd.randint(1000, 1999)}"
                if crash and sev in ("High", "Critical"):
                    self.crashes.append((ts, user, now_ms - ts))        # event time, user, how late (ms)
                rec = {"timestamp": ts, "status": rnd.choice(["Success", "Failure", "Pending"]), "severity": sev,
                       "source_ip": f"192.168.1.{rnd.randint(1, 254)}", "user_id": user,
                       "content": " ".join(rnd.sample(_WORDS, 4)) + (" crash" if crash else "")}
                while True:
                    try:
                        producer.produce(TOPIC, json.dumps(rec).encode())
                        break
                    except BufferError:
                        producer.poll(0.05)
                self.sent += 1
            producer.poll(0)
            tick += 1
            time.sleep(max(0.0, t0 + tick * 0.1 - time.time()))
        producer.flush()


def produce(seconds=30, rate=RATE):
    """Write live log records into `logs` for `seconds` (rate records/s; 5 % late by up to 30 s)."""
    feed = _Feed(seconds, rate)
    feed.start()
    feed.join()
    print(f"produce: {feed.sent:,} records in {seconds} s, {feed.late_sent:,} of them late")


# ------------------------------------------------------------------ looking at the alerts
def _read_alerts(start=None):
    """All messages of crash_alerts from `start` ({partition: offset}) to the end, oldest first:
    [(time ms, key, interval start ms, interval end ms, user_id, crash_count)]."""
    end = _end_offsets(ALERTS)
    start = start or {p: 0 for p in end}
    todo = {p for p in end if end[p] > start.get(p, 0)}
    out = []
    if not todo:
        return out
    c = _ck.Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "exercise8-check", "enable.auto.commit": False})
    try:
        c.assign([_ck.TopicPartition(ALERTS, p, start.get(p, 0)) for p in todo])
        while todo:
            m = c.poll(5.0)
            if m is None:
                break
            if m.error():
                continue
            v = json.loads(m.value())
            out.append((m.timestamp()[1], (m.key() or b"").decode(), _ms(v["interval_start"]), _ms(v["interval_end"]),
                        v["user_id"], v["crash_count"]))
            if m.offset() + 1 >= end[m.partition()]:
                todo.discard(m.partition())
    finally:
        c.close()
    return out


def _ms(text):
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)


def _hms(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S")


def show_alerts(n=10):
    """The last n messages of the topic crash_alerts (times in UTC)."""
    msgs = _read_alerts()[-n:]
    print(f"{'written at':<11} {'interval (UTC)':<20} {'after its end':>13}   {'user':<10} {'crashes':>7}")
    for t, key, s, e, user, count in msgs:
        print(f"{_hms(t):<11} {_hms(s)}-{_hms(e):<11} {(t - e) / 1000:11.1f} s   {user:<10} {count:>7}")
    if not msgs:
        print("(no alerts yet)")


# ------------------------------------------------------------------ the check
def _report_rows(report):
    """{(interval start ms, user_id): crash_count} from what ops_report() returned:
    a Spark DataFrame like crash_report's, or a pandas DataFrame / list of rows
    (interval_start [datetime in UTC or ms], user_id, crash_count)."""
    if hasattr(report, "rdd"):                                   # Spark
        start = "interval.start" if "interval" in report.columns else "interval_start"
        rows = report.select(F.unix_millis(F.col(start)).alias("s"), "user_id", "crash_count").collect()
        return {(r.s, r.user_id): r.crash_count for r in rows}
    if hasattr(report, "df") and not hasattr(report, "itertuples"):   # DuckDB relation
        report = report.df()
    if hasattr(report, "itertuples"):                            # pandas
        report = report[["interval_start", "user_id", "crash_count"]].itertuples(index=False)
    out = {}
    for s, user, count in report:
        if isinstance(s, datetime):
            s = int((s if s.tzinfo else s.replace(tzinfo=timezone.utc)).timestamp() * 1000)
        elif hasattr(s, "timestamp"):                            # pandas Timestamp
            s = int(s.timestamp() * 1000)
        out[(int(s), user)] = int(count)
    return out


def _reader_rows(reader, messages, name):
    """{(interval start ms, user_id): crash_count} from a reader's {key: crash_count} (None + a message if not)."""
    try:
        got = reader(list(messages))
        out = {}
        for key, count in dict(got).items():
            start, user = key.rsplit(" ", 1)
            out[(_ms(start), user)] = int(count)
        return out
    except Exception as e:  # noqa: BLE001
        print(f"\n{name}(messages) failed or did not return {{key: crash_count}}: {type(e).__name__}: {e}")
        return None


def _pct(values, p):
    v = sorted(values)
    return v[min(len(v) - 1, int(math.ceil(p * len(v))) - 1)] if v else float("nan")


def _wait_until(cond, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        time.sleep(0.5)
    return False


def _consumed_everything(query):
    p = query.lastProgress or {}
    end = _end_offsets(TOPIC)
    try:
        got = p["sources"][0]["endOffset"]
        if isinstance(got, str):                    # Spark 4: a JSON string (Spark 3.5: a dict)
            got = json.loads(got)
        got = got[TOPIC]
        return all(int(got.get(str(k), -1)) >= v for k, v in end.items())
    except (KeyError, IndexError, TypeError, ValueError):
        return False


def check_all(start_query, pager=None, ops_view=None, ops_report=None, seconds=90):
    """Run your query on live log records for `seconds` (plus 40 s, so that the last intervals can close),
    stop it once right after it wrote alerts, restart it from its checkpoint, then compare what the two
    teams get with a reference report computed over the very same records.

    on-call engineer:  pager(messages), the alerts it pages for
    operations team:   ops_report() if given (architecture C), else ops_view(messages)
    messages:          the messages of crash_alerts written during the check, oldest first, as (key, crash_count)

        check_all(start_query, pager, ops_view)                   # A, B
        check_all(start_query, pager, ops_report=ops_report)      # C
    """
    if pager is None or (ops_view is None and ops_report is None):
        print("check_all needs the two readers: check_all(start_query, pager, ops_view)\n"
              "(architecture C: check_all(start_query, pager, ops_report=ops_report))")
        return None
    for name, reader in (("pager", pager), ("ops_view", ops_view)):    # fail now, not after 2 minutes
        if reader is not None and _reader_rows(reader, [("2026-01-01 00:00:00 user_1000", 3)], name) is None:
            return None
    global _stop_armed, _alerts_at_arming
    spark = get_spark()
    with contextlib.redirect_stdout(io.StringIO()):
        reset()
    alerts_from = _end_offsets(ALERTS)
    tail = MAX_LATE_S + 10

    def say(msg):
        print(f"{time.time() - t0:5.0f} s  {msg}", flush=True)

    t0 = time.time()
    with contextlib.redirect_stdout(io.StringIO()):
        query = start_query()
    query = query or next(iter(spark.streams.active), None)
    if query is None:
        print("start_query() started no query: does it return write_alerts(...)?")
        return None
    # a new query starts at the end of `logs` ("latest"): wait until it has fixed that position in its
    # checkpoint, so that it sees every record of this check
    if not _wait_until(lambda: os.path.exists(f"{CHECKPOINT}/sources/0/0") or not query.isActive, 60):
        print("(the query did not note its start position within 60 s; checking anyway)")
    if not query.isActive:
        print("\nYour query failed:\n" + str(query.exception() or "")[:2000])
        return None
    feed = _Feed(seconds + tail, seed=8)
    feed.start()
    say(f"query started; writing {RATE:,} live records/s into `logs`\n"
        f"         watch it in the Spark UI (http://localhost:4040 -> Structured Streaming) and the topic {ALERTS} "
        f"in Kafka UI")

    time.sleep(max(0.0, seconds / 2 - (time.time() - t0)))
    _alerts_at_arming = _alerts_written()
    _stop_armed = True
    restarted = None
    if _wait_until(lambda: not query.isActive, 60):
        err = str(query.exception() or "")
        if "SimulatedStop" not in err:
            _stop_armed = False
            feed.join()
            print("\nYour query failed:\n" + err[:2000])
            return None
        say("the query stopped right after writing alerts (simulated crash)")
        restarted = True
    else:
        query.stop()
        say("the query wrote no alerts in 60 s; stopped it normally")
        restarted = False
    _stop_armed = False
    time.sleep(2)
    with contextlib.redirect_stdout(io.StringIO()):
        query = start_query() or next(iter(spark.streams.active), None)
    say("query restarted from its checkpoint")

    feed.join()
    say(f"{feed.sent:,} records written ({feed.late_sent:,} late); waiting until the query has processed them")
    _wait_until(lambda: _consumed_everything(query), 120)
    time.sleep(4)                                       # a few more micro-batches: the watermark moves on
    if query.exception():
        print("\nYour query failed:\n" + str(query.exception())[:2000])
        return None
    query.stop()

    # ---------------------------------------------------------------- the reference
    first = int(math.ceil(feed.start_ms / 10_000) * 10_000)
    last = int((feed.start_ms + seconds * 1000) // 10_000 * 10_000)       # intervals [first, last)
    counts, lates = {}, {}
    for ts, user, late_ms in feed.crashes:
        s = ts // 10_000 * 10_000
        if first <= s < last:
            counts[(s, user)] = counts.get((s, user), 0) + 1
            if late_ms > 0:
                lates.setdefault((s, user), []).append(late_ms)
    ref = {k: c for k, c in counts.items() if c > 2}

    # ---------------------------------------------------------------- the on-call engineer: pager(messages)
    raw = _read_alerts(alerts_from)                  # (time, key, interval start, interval end, user, count)
    messages = [(key, count) for t, key, s, e, user, count in raw]
    paged = _reader_rows(pager, messages, "pager")
    if paged is None:
        return None
    msgs = [m for m in raw if first <= m[2] < last]
    paged = {k: c for k, c in paged.items() if first <= k[0] < last}
    paged_at, prev, repeats, updates = {}, {}, 0, 0
    for t, key, s, e, user, count in msgs:           # when was each page sent, what did the pager ignore?
        k = (s, user)
        if k in paged and k not in paged_at and count == paged[k]:
            paged_at[k] = (t, e)
        elif k in prev:
            if prev[k] == count:
                repeats += 1
            else:
                updates += 1
        prev[k] = count
    delays = [(paged_at[k][0] - paged_at[k][1]) / 1000 for k in ref if k in paged_at]
    p50, p95 = _pct(delays, 0.50), _pct(delays, 0.95)
    alerted = len(delays)
    pages = len(paged)

    # ---------------------------------------------------------------- the operations team
    batch_s = None
    if ops_report is None:
        ops = _reader_rows(ops_view, messages, "ops_view")
        if ops is None:
            return None
        source = "ops_view(messages)"
    else:
        say("computing the operations report with ops_report()")
        tb = time.time()
        with contextlib.redirect_stdout(io.StringIO()):
            ops = _report_rows(ops_report())
        batch_s = time.time() - tb
        source = f"ops_report() (took {batch_s:.0f} s)"
    ops = {k: c for k, c in ops.items() if first <= k[0] < last}
    correct = sum(ops.get(k) == c for k, c in ref.items())
    missing = sum(k not in ops for k in ref)
    wrong = len(ref) - correct - missing
    extra = sum(k not in ref for k in ops)
    pct = 100 * correct / max(1, len(ref) + extra)
    never_paged = sum(k not in paged for k in ops)                       # C: the two paths disagree
    other_count = sum(k in paged and paged[k] != c for k, c in ops.items())

    on_call_ok = alerted > 0 and p95 <= ON_CALL_MAX_S
    ops_ok = pct >= OPS_MIN_CORRECT
    n = len(ref)
    ok = lambda met: "MET" if met else "NOT MET"                      # noqa: E731

    def row(label, value, status=""):
        print(f"  {label:<54}{str(value):<30}{status}".rstrip())

    def interval(k):
        return f"{_hms(k[0])}-{_hms(k[0] + 10_000)}"

    def why(k):
        late = sorted(lates.get(k, []))
        if not late:
            return "none of its crashes arrived late"
        if len(late) == 1:
            return f"1 of its crashes arrived {late[0] / 1000:.0f} s late"
        return f"{len(late)} of its crashes arrived late: " + ", ".join(f"{x / 1000:.0f} s" for x in late)

    def examples(keys, left, right, how_many=3):
        for k in keys[:how_many]:
            a, b = left(k), right(k)
            print(f"      {k[1]:<10} {interval(k)}   {a:<13} {b:<12} {why(k)}")

    print(f"\nTHE CHECK: {feed.sent:,} log records in {seconds + tail} s, {feed.late_sent:,} of them late (by up to "
          f"{MAX_LATE_S} s)" + (";\n           your query was stopped once and restarted from its checkpoint." if restarted else "."))
    print(f"THE TRUTH: {n:,} problems (a user with more than 2 severe crashes in one 10-second interval) in "
          f"{(last - first) // 10_000} intervals,\n           counted over every record written, late ones included.")

    print(f"\nON-CALL ENGINEER: a page at most {ON_CALL_MAX_S} s after the interval ends, for 95 % of the problems")
    row("delay of the page after the interval ends", f"p50 {p50:.1f} s · p95 {p95:.1f} s", ok(on_call_ok))
    if p50 < 0:
        row("  (negative: paged before the interval was over)", "")
    row("problems paged", f"{alerted:,} of {n:,} ({100 * alerted / max(1, n):.1f} %)")
    row("messages in crash_alerts → pages", f"{len(msgs):,} → {pages:,}")
    row("  ignored: repeats (written again after the restart)", repeats)
    row("  ignored: updates (a new count, same problem)", updates)

    print(f"\nOPERATIONS TEAM: a correct report, at least {OPS_MIN_CORRECT:.0f} % of the rows right, "
          f"late records included")
    row("rows correct (right problem, right count)", f"{correct:,} of {n + extra:,} ({pct:.1f} %)", ok(ops_ok))
    row("  missing: a problem not in the report", missing)
    row("  wrong count", wrong)
    row("  extra: in the report, but not a problem", extra)
    bad = sorted([k for k in ref if k in ops and ops[k] != ref[k]])[:2] + sorted([k for k in ref if k not in ops])[:1]
    bad += sorted([k for k in ops if k not in ref])[:max(0, 3 - len(bad))]
    if bad:
        print("    for example (interval in UTC):")
        examples(bad, lambda k: f"report: {ops.get(k, 'none')}", lambda k: f"truth: {ref.get(k, 'none')}")
    row("read from", source)

    if ops_report is not None:
        print("\nALERTS vs REPORT (architecture C: two paths for the same problems)")
        row("problems in the report, never paged", never_paged)
        row("paged with another count than in the report", other_count)
        diff = sorted([k for k in ops if k not in paged])[:2] + \
            sorted([k for k, c in ops.items() if k in paged and paged[k] != c])[:1]
        if diff:
            print("    for example (interval in UTC):")
            examples(diff, lambda k: f"paged: {paged.get(k, 'none')}", lambda k: f"report: {ops.get(k, 'none')}")

    print(f"\nRESULT: on-call engineer {'served' if on_call_ok else 'NOT served'} (p95 {p95:.1f} s, needs ≤ "
          f"{ON_CALL_MAX_S} s) · operations team {'served' if ops_ok else 'NOT served'} ({pct:.1f} % correct, "
          f"needs ≥ {OPS_MIN_CORRECT:.0f} %)")
    if not restarted:
        print("(no restart was tested: the query wrote no alerts in the 60 s after the half-time)")
    with contextlib.redirect_stdout(io.StringIO()):
        reset()
    global last_check                    # the table's numbers; check_all returns None, so Jupyter shows only the table
    last_check = dict(rows=n, p50=p50, p95=p95, alerted=alerted, messages=len(msgs), pages=pages, updates=updates,
                repeats=repeats, never_paged=never_paged if ops_report else None,
                other_count=other_count if ops_report else None,
                correct=correct, missing=missing, wrong=wrong, extra=extra, pct=pct, on_call_ok=on_call_ok,
                ops_ok=ops_ok, batch_s=batch_s, restarted=restarted)

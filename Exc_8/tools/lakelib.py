"""Building blocks for Exercise 7: copy the topic `logs` into a data lake, run after run.

Use them in a notebook in spark-lab:

    import sys; sys.path.append("/home/jovyan/tools")
    from lakelib import *

Your job is one function, run_once(), built from these blocks:

    load_state()                -> where the last run stopped, from a small state file (None: never ran)
    lake_position()             -> where the last run stopped, read from the lake itself (None: empty lake)
    read_new(start)             -> the records after `start`, up to the current end of the topic
    write_files(new, folder)    -> writes them as Parquet into a new run folder inside `folder`
    publish(run_folder)         -> moves a finished run folder from STAGING into the lake (one rename)
    save_state(position)        -> stores where this run stopped in the state file

    LAKE      the lake folder: everything in it is visible to readers
    STAGING   a folder next to the lake that readers never look at

To check your job:

    check()                     -> compares the lake with the topic (the lake checker)
    check_all(run_once)         -> runs your job through five situations and prints a table
    add_records(n)              -> writes n new log records into the topic (like the generator)
    reset()                     -> deletes the lake, STAGING and the state file (not the topic)
"""
import importlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
import uuid

TOPIC = "logs"
BOOTSTRAP = "kafka:29092"
DATA = "/home/jovyan/data"
LAKE = f"{DATA}/lake/logs"
STAGING = f"{DATA}/staging"
STATE = f"{DATA}/state/position.json"

LAKE_SQL = f"read_parquet('{LAKE}/**/*.parquet')"
DEDUP_SQL = f"(SELECT DISTINCT ON (kafka_partition, kafka_offset) * FROM {LAKE_SQL})"
SCHEMA = "timestamp LONG, status STRING, severity STRING, source_ip STRING, user_id STRING, content STRING"

__all__ = ["LAKE", "STAGING", "STATE", "LAKE_SQL", "DEDUP_SQL", "load_state", "save_state", "lake_position",
           "read_new", "write_files", "publish", "check", "check_all", "add_records", "reset", "get_spark",
           "SimulatedStop"]


def _ensure(module, package):
    try:
        return importlib.import_module(module)
    except ImportError:
        print(f"(installing {package} ...)", flush=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", package], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        importlib.invalidate_caches()
        return importlib.import_module(module)


_ck = _ensure("confluent_kafka", "confluent-kafka==2.11.0")
_duckdb = _ensure("duckdb", "duckdb==1.5.6")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_lake as _check_lake  # noqa: E402

_spark = None


def get_spark():
    """The Spark session (created on first use; downloads the Kafka connector the first time)."""
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = (SparkSession.builder.appName("Exercise7")
                  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1")
                  .config("spark.sql.shuffle.partitions", "4")
                  .getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


# ------------------------------------------------------------------ simulated stops (used by check_all)
class SimulatedStop(Exception):
    """Raised by the blocks when check_all simulates a crash. Do not catch it in run_once()."""


_stop_at = None


def _maybe_stop(point):
    if _stop_at == point:
        raise SimulatedStop(point)


# ------------------------------------------------------------------ where did the last run stop?
def _offsets():
    """{partition: (first offset, end offset)} of the topic right now."""
    c = _ck.Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "exercise7-offsets"})
    try:
        parts = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        return {p: c.get_watermark_offsets(_ck.TopicPartition(TOPIC, p), timeout=10) for p in sorted(parts)}
    finally:
        c.close()


def load_state():
    """Where the last run stopped, from the state file: {partition: next offset}, or None."""
    if not os.path.exists(STATE):
        return None
    return {int(p): o for p, o in json.load(open(STATE)).items()}


def save_state(position):
    """Store {partition: next offset} in the state file (the write of this one file is a single step)."""
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump({str(p): o for p, o in position.items()}, open(STATE + ".tmp", "w"))
    os.replace(STATE + ".tmp", STATE)


def lake_position():
    """Where the last run stopped, read from the lake: {partition: highest kafka_offset + 1}, or None."""
    try:
        rows = _duckdb.sql(f"SELECT kafka_partition, MAX(kafka_offset) + 1 FROM {LAKE_SQL} GROUP BY ALL").fetchall()
    except Exception as e:  # noqa: BLE001
        if "No files found" in str(e):
            return None
        raise
    return {int(p): int(o) for p, o in rows} or None


# ------------------------------------------------------------------ read and write
class NewRecords:
    """The records of one run: offsets [start, end) of every partition. `.df` is a Spark DataFrame."""

    def __init__(self, start, end):
        self.start, self.end = start, end
        self.count = sum(end[p] - start[p] for p in end)

    def partition_df(self, p):
        r = (get_spark().read.format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("assign", json.dumps({TOPIC: [p]}))
             .option("startingOffsets", json.dumps({TOPIC: {str(p): self.start[p]}}))
             .option("endingOffsets", json.dumps({TOPIC: {str(p): self.end[p]}}))
             .load())
        return _parse(r)

    @property
    def df(self):
        r = (get_spark().read.format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("subscribe", TOPIC)
             .option("startingOffsets", json.dumps({TOPIC: {str(p): o for p, o in self.start.items()}}))
             .option("endingOffsets", json.dumps({TOPIC: {str(p): o for p, o in self.end.items()}}))
             .load())
        return _parse(r)

    def __repr__(self):
        return f"NewRecords({self.count:,} records; start={self.start}, end={self.end})"


def _parse(raw):
    from pyspark.sql import functions as F
    return (raw.select(F.col("partition").alias("kafka_partition"),
                       F.col("offset").alias("kafka_offset"),
                       F.from_json(F.col("value").cast("string"), SCHEMA).alias("log"))
            .select("kafka_partition", "kafka_offset", "log.*")
            .withColumn("event_time", F.timestamp_millis("timestamp")))


def read_new(start):
    """The records after `start` ({partition: offset}, None = from the beginning), up to the end of the
    topic NOW. The end is fixed here: records that arrive later are left for the next run."""
    offsets = _offsets()
    start = {p: max((start or {}).get(p, first), first) for p, (first, end) in offsets.items()}
    end = {p: e for p, (_, e) in offsets.items()}
    new = NewRecords(start, end)
    print(f"read_new: {new.count:,} new records")
    return new


def write_files(new, folder):
    """Write the records as Parquet into a NEW run folder inside `folder` (LAKE or STAGING), one Kafka
    partition after the other. Returns the run folder (None if there was nothing to write)."""
    if new is None or new.count == 0:
        print("write_files: nothing to write")
        return None
    if os.path.abspath(folder).startswith(os.path.abspath(STAGING)):
        shutil.rmtree(STAGING, ignore_errors=True)       # leftovers of a run that stopped: never published
    run = f"{folder}/run-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    written = 0
    for p in sorted(new.end):
        if new.end[p] > new.start[p]:
            new.partition_df(p).write.parquet(f"{run}/p{p}")
            written += 1
            if written == 1:
                _maybe_stop("while_writing")             # some files written, others not yet
    print(f"write_files: {new.count:,} records -> {run}")
    _maybe_stop("after_writing")                         # all files written, nothing after this yet
    return run


def publish(run_folder):
    """Move a finished run folder from STAGING into the lake: one rename, a single step."""
    if run_folder is None:
        return None
    os.makedirs(LAKE, exist_ok=True)
    target = f"{LAKE}/{os.path.basename(run_folder)}"
    os.rename(run_folder, target)
    print(f"publish: {os.path.basename(run_folder)} is now in the lake")
    return target


# ------------------------------------------------------------------ checking
def check(lake_sql=LAKE_SQL, quiet=False):
    """Compare the lake with the topic. lake_sql: how readers read the lake (LAKE_SQL or DEDUP_SQL)."""
    return _check_lake.run_check(lake_sql, files=f"{DATA}/lake", quiet=quiet)


def reset():
    """Delete the lake, STAGING and the state file. The topic stays as it is."""
    for path in (f"{DATA}/lake", STAGING, os.path.dirname(STATE)):
        shutil.rmtree(path, ignore_errors=True)
    print("reset: lake, staging and state deleted")


_LEVELS = ["Low", "Medium", "High", "Critical"]
_WORDS = ["kernel", "buffer", "socket", "timeout", "disk", "memory", "login", "request", "cache", "thread"]


def add_records(n=100_000):
    """Write n new log records into the topic, in the generator's format (event time = now)."""
    producer = _ck.Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "batch.num.messages": 10000})
    now = int(time.time() * 1000)
    for i in range(n):
        rec = {"timestamp": now + i // 20, "status": random.choice(["Success", "Failure", "Pending"]),
               "severity": random.choice(_LEVELS), "source_ip": f"192.168.1.{random.randint(1, 254)}",
               "user_id": f"user_{random.randint(1000, 1999)}",
               "content": " ".join(random.sample(_WORDS, 4)) + (" crash" if i % 100 == 0 else "")}
        while True:
            try:
                producer.produce(TOPIC, json.dumps(rec).encode())
                break
            except BufferError:
                producer.poll(0.1)
        if i % 10000 == 0:
            producer.poll(0)
    producer.flush()
    print(f"add_records: {n:,} new records in the topic")


def check_all(run_once, lake_sql=LAKE_SQL):
    """Run your job through five situations and print one table. Starts with reset()."""
    global _stop_at
    import contextlib
    import io
    steps = [("1 first run", 0, None),
             ("2 new records arrive, run again", 100_000, None),
             ("3 nothing new, run again", 0, None),
             ("4 run stops WHILE writing", 100_000, "while_writing"),
             ("5 run stops AFTER writing", 100_000, "after_writing")]
    reset()
    rows = []

    def record(label, secs=None):
        t = check(lake_sql, quiet=True)
        rows.append((label, t, secs))

    for label, n, stop in steps:
        if n:
            with contextlib.redirect_stdout(io.StringIO()):
                add_records(n)
        if stop:
            _stop_at = stop
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    run_once()
                rows.append((label + " (did not stop!)", None, None))
            except SimulatedStop:
                record(label + ": right after")
            finally:
                _stop_at = None
            label = "  ... then the next run"
        t0 = time.time()
        with contextlib.redirect_stdout(io.StringIO()):
            run_once()
        record(label, time.time() - t0)

    print(f"\n{'situation':<38} {'in lake':>10} {'duplicates':>11} {'missing':>8} {'not loaded':>11} {'run':>6}  check")
    print("-" * 95)
    for label, t, secs in rows:
        if t is None:
            print(f"{label:<38}")
            continue
        verdict = "PASS" if t["ok"] else "FAIL"
        run = f"{secs:5.1f}s" if secs is not None else ""
        print(f"{label:<38} {t['lake']:>10,} {t['dup']:>11,} {t['miss']:>8,} {t['lag']:>11,} {run:>6}  {verdict}")
    finals = [t for label, t, secs in rows if t is not None and secs is not None]
    ok = all(t["ok"] for t in finals) and finals and finals[-1]["lag"] == 0
    print("-" * 95)
    print("Exactly once after every run: " + ("YES" if ok else "NO — see the rows marked FAIL"))
    return rows

"""Lake checker: is every record of the topic in the lake exactly once?

Works with any technology: it only needs to read your lake with DuckDB, and
your lake must keep, for every record, where it came from in Kafka:
the columns `kafka_partition` and `kafka_offset`.

Run it inside the spark-lab container (it installs DuckDB and a Kafka client
the first time, ~30 s):

    docker exec spark-lab python /home/jovyan/tools/check_lake.py \
        "read_parquet('/home/jovyan/data/lake/logs/**/*.parquet', hive_partitioning = true)"

The argument is whatever comes after FROM in a DuckDB query over your lake,
for example
    "read_parquet('/home/jovyan/data/lake/logs/**/*.parquet', hive_partitioning = true)"
    "delta_scan('/home/jovyan/data/lake/logs')"                (Delta Lake)
    "iceberg_scan('/home/jovyan/data/lake/logs')"              (Apache Iceberg)
Options:
    --setup "SQL"   run before the check, e.g. "INSTALL delta; LOAD delta;"
    --topic logs    the Kafka topic (default: logs)
    --files DIR     folder whose data files are counted (default: /home/jovyan/data/lake)

What it reports, per topic partition:
    in lake      records of this partition in the lake
    duplicates   records that are in the lake more than once
    missing      offsets the lake skipped: below the highest loaded offset,
                 but not in the lake
    not loaded   records in the topic after the highest loaded offset
                 (= how far the lake is behind the topic right now)
Exactly once = 0 duplicates and 0 missing. "not loaded" is not an error:
it is the work of your next run.
"""
import argparse
import importlib
import os
import subprocess
import sys
import time

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
DEFAULT_LAKE = "/home/jovyan/data/lake"
DATA_EXT = (".parquet", ".orc", ".avro", ".json", ".jsonl", ".csv", ".duckdb", ".db")


def ensure(module, package):
    try:
        return importlib.import_module(module)
    except ImportError:
        print(f"(installing {package} ...)", flush=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", package], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return importlib.import_module(module)


def topic_offsets(topic):
    """{partition: (first offset, end offset)} of the topic, read from the broker."""
    ck = ensure("confluent_kafka", "confluent-kafka==2.11.0")
    consumer = ck.Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "lake-checker",
                            "enable.auto.commit": False})
    try:
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        if meta.error is not None or not meta.partitions:
            sys.exit(f"Topic '{topic}' not found on {BOOTSTRAP}.")
        return {p: consumer.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=10)
                for p in sorted(meta.partitions)}
    finally:
        consumer.close()   # no commit: the checker never joins or moves a consumer group


def file_stats(folder):
    files = [os.path.join(d, f) for d, dirs, fs in os.walk(folder)
             if not any(part.startswith(("_", ".")) for part in d[len(folder):].split(os.sep))
             for f in fs if f.endswith(DATA_EXT) and not f.startswith((".", "_"))]
    sizes = [os.path.getsize(f) for f in files]
    return len(files), sum(sizes)


class CheckError(Exception):
    pass


def run_check(lake, setup="", topic="logs", files=DEFAULT_LAKE, quiet=False):
    """Compare the lake with the topic. Returns a dict with the totals and the verdict;
    prints the table unless quiet. Raises CheckError if the lake cannot be read."""
    out = (lambda *a, **k: None) if quiet else print
    duckdb = ensure("duckdb", "duckdb==1.5.6")
    con = duckdb.connect()
    if setup:
        con.execute(setup)

    # the topic first: records that arrive while we check count as "not loaded", never as "missing"
    offsets = topic_offsets(topic)
    t0 = time.time()
    rows = []
    try:
        cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM {lake}").fetchall()]
    except Exception as e:  # noqa: BLE001 — show DuckDB's message as it is
        if "No files found" not in str(e):
            raise CheckError(f"DuckDB cannot read your lake:\n  {e}") from None
        out("(no files in the lake yet)")
    else:
        for needed in ("kafka_partition", "kafka_offset"):
            if needed not in cols:
                raise CheckError(f"Your lake has no column '{needed}' (columns: {', '.join(cols)}).\n"
                                 "Keep the Kafka partition and offset of every record.")
        rows = con.execute(f"""
            SELECT kafka_partition::INT AS p, COUNT(*) AS n, COUNT(DISTINCT kafka_offset) AS d,
                   MIN(kafka_offset) AS lo, MAX(kafka_offset) AS hi
            FROM {lake} GROUP BY ALL ORDER BY p""").fetchall()
    found = {p: (n, d, lo, hi) for p, n, d, lo, hi in rows}
    elapsed = time.time() - t0

    out(f"\nLake check — topic '{topic}' ({len(offsets)} partitions)   {time.strftime('%Y-%m-%d %H:%M:%S')}")
    out(f"lake: {lake}\n")
    head = f"{'partition':>9}  {'topic offsets':>21}  {'in lake':>10}  {'duplicates':>10}  {'missing':>9}  {'not loaded':>10}"
    out(head)
    out("-" * len(head))
    tot = dict(topic=0, lake=0, dup=0, miss=0, lag=0)
    foreign = 0
    for p, (begin, end) in offsets.items():
        n, d, lo, hi = found.get(p, (0, 0, None, None))
        if n and (lo < begin or hi >= end):
            foreign += n   # offsets the topic does not have (wrong column, or an older topic)
        loaded_to = hi + 1 if n else begin               # first offset not loaded yet
        dup, miss, lag = n - d, max(0, (loaded_to - begin) - d), max(0, end - loaded_to)
        out(f"{p:>9}  {f'[{begin:,}, {end:,})':>21}  {n:>10,}  {dup:>10,}  {miss:>9,}  {lag:>10,}")
        for k, v in zip(tot, (end - begin, n, dup, miss, lag)):
            tot[k] += v
    extra = sum(found[p][0] for p in found if p not in offsets)
    out("-" * len(head))
    topic_total = f"{tot['topic']:,} records"
    out(f"{'total':>9}  {topic_total:>21}  {tot['lake']:>10,}  {tot['dup']:>10,}  "
        f"{tot['miss']:>9,}  {tot['lag']:>10,}")

    if files and os.path.isdir(files):
        nfiles, nbytes = file_stats(files)
        avg = nbytes / nfiles / 1e6 if nfiles else 0
        tot.update(files=nfiles, mb=nbytes / 1e6)
        out(f"\nfiles under {files}: {nfiles:,} data files, {nbytes / 1e6:,.1f} MB, "
            f"{avg:,.2f} MB per file on average")
    out(f"check took {elapsed:.1f} s")

    problems = []
    if extra or foreign:
        problems.append(f"{extra + foreign:,} records have a partition/offset that is not in the topic "
                        "(wrong columns, or the topic was re-created: start a new lake)")
    if tot["dup"]:
        problems.append(f"{tot['dup']:,} duplicates")
    if tot["miss"]:
        problems.append(f"{tot['miss']:,} missing records")
    tot["ok"] = not problems
    tot["problems"] = problems
    if problems:
        out("\nExactly once: FAIL — " + "; ".join(problems))
    else:
        out("\nExactly once: PASS" + (f" ({tot['lag']:,} records not loaded yet)" if tot["lag"] else ""))
    return tot


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("lake", help="DuckDB table expression for your lake (what comes after FROM)")
    ap.add_argument("--setup", default="", help="SQL to run first (INSTALL/LOAD extensions, S3 secrets, ...)")
    ap.add_argument("--topic", default="logs")
    ap.add_argument("--files", default=DEFAULT_LAKE, help="folder whose data files are counted")
    args = ap.parse_args()
    try:
        result = run_check(args.lake, args.setup, args.topic, args.files)
    except CheckError as e:
        sys.exit(str(e))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()

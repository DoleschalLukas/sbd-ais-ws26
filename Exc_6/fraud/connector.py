"""Registers your Debezium connector, or replaces it after you changed its file. Do not change this file.

    python3 connector.py fraud-connector.json            # register, or replace with the changed file
    python3 connector.py fraud-connector.json --status   # is it running? which topic?
    python3 connector.py fraud-connector.json --delete   # remove it (connector, slot, topic)

The partitions, the key and the encoding of events already in a topic cannot
change. So, to apply a changed file, this script starts clean:

  1. it deletes every connector that writes to the same topics (same `topic.prefix`);
  2. it drops their replication slots in PostgreSQL (a slot nobody reads makes the
     database keep its WAL forever);
  3. it deletes the topics `<topic.prefix>.*`;
  4. it registers your file under a fresh name (Kafka Connect remembers how far a
     connector has read by its name: a fresh name is a fresh start).

The new connector first takes a snapshot of the table, then streams the changes.
Stop your agents before you replace the connector, and start them again after.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request

import psycopg
from confluent_kafka.admin import AdminClient

from card_processor import CONN

CONNECT = "http://localhost:8083"
BOOTSTRAP = "localhost:9092"


def rest(method, path, body=None):
    req = urllib.request.Request(CONNECT + path, method=method, headers={"Content-Type": "application/json"},
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")
    except urllib.error.URLError:
        raise SystemExit(f"Kafka Connect is not reachable on {CONNECT} (still starting? wait ~30 s)")


def same_topics(prefix):
    """The connectors that write to the topics <prefix>.*, with their configuration."""
    _, names = rest("GET", "/connectors")
    found = {}
    for name in names or []:
        _, cfg = rest("GET", f"/connectors/{name}/config")
        if cfg and cfg.get("topic.prefix") == prefix:
            found[name] = cfg
    return found


def drop_slot(slot, dbname, seconds=30):
    conn = dict(CONN, dbname=dbname)
    with psycopg.connect(**conn, autocommit=True) as db:
        for _ in range(seconds):
            row = db.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,)).fetchone()
            if row is None:
                return
            if not row[0]:                                # the connector's task has stopped
                db.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                print(f"  dropped replication slot {slot}")
                return
            time.sleep(1)
    raise SystemExit(f"replication slot {slot} is still in use: is another connector using it? "
                     f"See Troubleshooting in the README.")


def delete_topics(prefix):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith(prefix + ".")]
    if not topics:
        return
    for t, fut in admin.delete_topics(topics).items():
        fut.result()
        print(f"  deleted topic {t}")
    for _ in range(30):                                   # deletion is asynchronous
        if not set(topics) & set(admin.list_topics(timeout=10).topics):
            return
        time.sleep(1)


def remove(prefix):
    for name, cfg in same_topics(prefix).items():
        rest("DELETE", f"/connectors/{name}")
        print(f"  deleted connector {name}")
        drop_slot(cfg.get("slot.name", "debezium"), cfg.get("database.dbname", CONN["dbname"]))
    delete_topics(prefix)


def status(prefix):
    found = same_topics(prefix)
    if not found:
        print(f"no connector writes to {prefix}.*")
        return False
    ok = True
    for name, cfg in found.items():
        _, st = rest("GET", f"/connectors/{name}/status")
        tasks = st.get("tasks", [])
        state = st["connector"]["state"] if not tasks else tasks[0]["state"]
        print(f"connector {name}: {state}")
        for t in tasks:
            if t["state"] == "FAILED":
                print("  " + t.get("trace", "").strip().splitlines()[0][:300])
        print(f"  table {cfg.get('table.include.list')} -> topic {prefix}.{cfg.get('table.include.list', '?')}"
              f"   (appears with the first event)")
        ok = ok and state == "RUNNING"
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file", help="your connector file, e.g. fraud-connector.json")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--delete", action="store_true")
    a = ap.parse_args()

    spec = json.load(open(a.file))
    name, cfg = spec["name"], dict(spec["config"])
    todo = [k for k, v in cfg.items() if "TODO" in str(v)]
    if todo:
        raise SystemExit(f"fill in the TODO values first: {', '.join(todo)}")
    prefix = cfg["topic.prefix"]

    if a.status:
        sys.exit(0 if status(prefix) else 1)
    print(f"removing what writes to {prefix}.* ...")
    remove(prefix)
    if a.delete:
        return

    drop_slot(cfg.get("slot.name", "debezium"), cfg.get("database.dbname", CONN["dbname"]))  # left over by hand
    fresh = f"{name}-{time.strftime('%H%M%S')}"
    code, body = rest("POST", "/connectors", {"name": fresh, "config": cfg})
    if code != 201:
        raise SystemExit(f"Kafka Connect refused the configuration ({code}): {body.get('message', body)}")
    print(f"registered {fresh}")
    for _ in range(30):
        _, st = rest("GET", f"/connectors/{fresh}/status")
        if st and st.get("tasks"):
            break
        time.sleep(1)
    sys.exit(0 if status(prefix) else 1)


if __name__ == "__main__":
    main()

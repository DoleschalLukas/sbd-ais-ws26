"""The connection to PostgreSQL, the plant's system of record. Do not change this file.

    conn = services.connect()          # database `factory`, created on first use

Tables: machines (the machine registry), readings (today's sensor store),
detector_health (telemetry of the model, see model.py).
"""
import psycopg

DB = dict(host="localhost", port=5432, user="postgres", password="postgrespw")
DB_NAME = "factory"

SCHEMA = """
CREATE TABLE IF NOT EXISTS machines (
    machine_id TEXT PRIMARY KEY, line TEXT, type TEXT, installed DATE,
    last_maintenance DATE, criticality TEXT);
CREATE TABLE IF NOT EXISTS readings (
    id BIGSERIAL PRIMARY KEY, run BIGINT, machine_id TEXT, seq INT, ts DOUBLE PRECISION,
    vibration DOUBLE PRECISION, temperature DOUBLE PRECISION, current DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS detector_health (
    instance TEXT, pid INT, model TEXT, readings BIGINT, missing BIGINT, out_of_order BIGINT,
    machines INT, updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (instance, pid));
"""

_ready = False


def connect(autocommit=True):
    """A connection to the `factory` database (created on first use)."""
    global _ready
    if not _ready:
        with psycopg.connect(dbname="postgres", autocommit=True, **DB) as conn:
            if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,)).fetchone():
                conn.execute(f"CREATE DATABASE {DB_NAME}")
        with psycopg.connect(dbname=DB_NAME, autocommit=True, **DB) as conn:
            conn.execute(SCHEMA)
        _ready = True
    return psycopg.connect(dbname=DB_NAME, autocommit=autocommit, **DB)

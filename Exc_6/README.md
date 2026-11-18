# Exercise 6 — *Card Sentinel*: real-time fraud detection with CDC

**Goal:** a card processor commits every payment into PostgreSQL, and two
fraud models must see every payment within seconds. You choose how the
payments get from the database to the models, then build the pipeline in
three runs, with Change Data Capture (Debezium) and Kafka. In each run you
predict what will happen, measure it with a scoreboard, and explain it.


| Part | What | When |
|------|------|------|
| Prep | Download images, prepare Python |
| Guide | Change Data Capture with Debezium (follow along) |
| 1 | The decision: how do the payments reach the models? |
| 2 | The pipeline in three runs: predict, run, explain |
| Write-up | One page: your decision and the three runs |

## Setup

PostgreSQL, Kafka and the Kafka UI are the same images as in Exercise 5.
New is **Kafka Connect with Debezium** (~1.4 GB). Download it beforehand:

```bash
docker compose pull
```

Prepare a Python virtual environment in this folder (Windows: `python` or
`py` instead of `python3`, `venv\Scripts\activate` instead of
`source venv/bin/activate`):

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Stop the containers of Exercise 5 (`docker compose down -v` in its folder),
then start this exercise's environment:

```bash
docker compose up -d
```

`docker ps` should show `postgres`, `kafka`, `connect` and `kafka-ui`, and
<http://localhost:8083/connector-plugins> should list the Debezium connectors
(it can take ~30 s after start-up).

## Guide — Change Data Capture with Debezium (follow along)

We do steps 1–3 together on the projector; run the commands along with us
or at home. It is not submitted, but you need what you see here in Part 2.
Step 4 is optional, at home.

Debezium reads PostgreSQL's write-ahead log (WAL) and turns every committed
`INSERT`, `UPDATE` and `DELETE` into a Kafka event, in commit order. Remember
the poller of Exercise 5 (*Line Guardian*, Stage A) that missed rows: CDC
does not query the table at all. Kafka Connect (container `connect`) runs
it; we only have to register a *connector*.

### 1. Create a database and a table

```bash
docker exec -it postgres psql -U postgres
```

```sql
CREATE DATABASE activity;
\c activity
CREATE TABLE activity (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255)
);
```

Keep this `psql` session open.

### 2. Register the Debezium connector

In **another terminal** (Windows: run it in Git Bash or WSL, or use the
PowerShell variant below):

```bash
curl -i -X POST -H "Content-Type: application/json" localhost:8083/connectors/ -d '{
  "name": "activity-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgrespw",
    "database.dbname": "activity",
    "topic.prefix": "dbserver1",
    "plugin.name": "pgoutput",
    "slot.name": "activityslot"
  }
}'
```

<details>
<summary>PowerShell variant</summary>

```powershell
$body = @{
  name = "activity-connector"
  config = @{
    "connector.class"   = "io.debezium.connector.postgresql.PostgresConnector"
    "database.hostname" = "postgres"; "database.port" = "5432"
    "database.user"     = "postgres"; "database.password" = "postgrespw"
    "database.dbname"   = "activity"; "topic.prefix" = "dbserver1"
    "plugin.name"       = "pgoutput"; "slot.name" = "activityslot"
  }
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors/ -ContentType "application/json" -Body $body
```
</details>

| Setting | Meaning |
|---|---|
| `database.*` | how Kafka Connect reaches PostgreSQL (container name `postgres`) |
| `topic.prefix` | events of table `public.activity` go to topic `dbserver1.public.activity` |
| `plugin.name` | PostgreSQL's built-in logical decoding output (`pgoutput`) |
| `slot.name` | the *replication slot*: PostgreSQL remembers how far Debezium has read |

Check that the connector and its task are `RUNNING`:

```bash
curl -s localhost:8083/connectors/activity-connector/status
```

### 3. Change data and watch the events

In `psql`:

```sql
INSERT INTO activity(id, name) VALUES (1, 'Alice');
```

Read the topic:

```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic dbserver1.public.activity --from-beginning --property print.key=true
```

Every event has a **key** (the first JSON object of the line) and a
**value**, each with two parts: a `schema` (the field types) and a
`payload`. The interesting part is the value's payload:

```json
{
  "before": null,
  "after": { "id": 1, "name": "Alice", "email": null },
  "source": { "connector": "postgresql", "db": "activity", "table": "activity", "lsn": 33452944, ... },
  "op": "c",
  "ts_ms": 1791127167212
}
```

The Kafka UI shows the same messages in a more readable form
(*Topics → dbserver1.public.activity → Messages*). Which column did Debezium
choose as the key? Nobody asked you.

### 4. Optional, at home: updates, deletes and tombstones

The lecture (slide *The change event*) showed what an `UPDATE` and a
`DELETE` look like as events. See it for yourself: run in `psql`

```sql
UPDATE activity SET email = 'alice@example.com' WHERE id = 1;
DELETE FROM activity WHERE id = 1;
```

and read the topic again. Find the `op` code, `before` and `after` of each
event, and check them against the slide:

1. `before` of the update is (almost) empty, because of PostgreSQL's
   `REPLICA IDENTITY`. Name a consumer that would need the full old row.
2. After the delete comes one more event, with the value `null` (a
   *tombstone*; lecture: *log compaction*). What would a consumer that keeps
   a copy of the table (a cache, a search index, a vector index) do with it?

## *Card Sentinel* — fraud detection on a system of record

**Scenario.** A card processor commits every card payment into PostgreSQL,
its *system of record* (consistency, durability, transactional integrity).
The payment path belongs to another team and is certified: you may **not**
change it. The data science team has delivered two fraud models, and case
management wants their alerts within seconds, so that a stolen card is
blocked before the thief's next payment.

The lab runs 1,000 payments per second for 100,000 cardholders.

![The pipeline: card_processor.py (the payment path, do not change) commits 1,000 payments/s into the table transactions of PostgreSQL; every commit goes into the write-ahead log (WAL), and a replication slot remembers how far Debezium has read. Kafka Connect runs the Debezium connector (your file fraud-connector.json) and writes into the topic fraud.public.transactions, whose number of partitions and key you set in Part 2. Two consumer groups read the topic: anomaly-agent with ano-1, and velocity-agent with vel-1, vel-2 and vel-3, all started by agents.py. Both write their alerts to the topic fraud-alerts, which scoreboard.py checks against the ground truth (R2–R4).](images/card-sentinel-pipeline.png)

Everything is in the folder `fraud/`:

| File | What | Change it? |
|---|---|---|
| `card_processor.py` | the payment path: commits batches of payments into the table `transactions` of database `mydb`; injects frauds | no |
| `models.py` | the data science team's models: `anomaly` (account takeover) and `velocity` (card testing) | no |
| `fraud_agent.py` | a fraud agent service that runs one model; complete, read it | no |
| `agents.py` | starts 1 anomaly and 3 velocity agents in one terminal | no |
| `fraud-connector.json` | the Debezium connector | **yes**, in Part 2 |
| `connector.py` | registers the connector, or replaces it after a change | no |
| `scoreboard.py` | plays case management: scores a run against the ground truth | no |

**The two models** (details at the top of `models.py`). Both are
**stateful per cardholder**: they remember something about every
cardholder, and only work if they see *all* of that cardholder's payments.

- `anomaly` (account takeover): remembers each cardholder's last 50 amounts
  and flags a payment far above the cardholder's average. 0.3 ms per payment.
- `velocity` (card testing): a thief tries a stolen card with many small
  payments (here: 12 within ~20 s). The model counts each cardholder's
  payments in the last 60 s and flags the 8th. 2 ms per payment.

**Requirements**

| | Requirement |
|---|---|
| R1 | **System of record untouched:** the payment path (`card_processor.py`) stays unchanged, never waits for a fraud check, and gets no extra query load. |
| R2 | **Every payment to every model:** each model gets every committed payment, none is missed. Adding another consumer (e.g. a training-data archive for the data science team) must not change what the others get. |
| R3 | **Correct at any scale:** each model catches ≥ 90 % of its frauds with few false alerts, *however many instances of it run*. |
| R4 | **Fresh:** an alert at most 5 s (p95) after the payment was committed, at full rate. |

R1 you argue in Part 1; the scoreboard measures R2–R4. Do not change the
files marked "no", and do not read `ground_truth.jsonl` in your code (in
real life, fraud is only confirmed after the chargeback).

### Part 1 — The decision (in the session)

There are four common ways to get every committed payment out of a
database and to the models (lecture: *Getting data out of a system of
record: four options* and *Why two writes are not one*):

![Four options. 1. Polling: the agents query PostgreSQL again and again (SELECT … WHERE id > last_id); it misses rows that commit out of id order and adds queries for every agent. 2. Dual write: the card processor commits to PostgreSQL (1) and then sends to Kafka (2); a crash after 1 and before 2 loses the event, and the payment path changes. 3. Outbox: the card processor writes the payment and an outbox row in one transaction, a relay publishes the outbox to Kafka; correct, but it changes the payment path, the best choice if you own the application. 4. Log-based CDC: Debezium reads PostgreSQL's WAL and publishes to Kafka; no code change, no queries, every commit in order; cost: the replication slot keeps WAL while the connector is down.](images/part1-options.png)

The table compares them for this scenario:

| Option | How it works | What goes wrong here | What it costs the database |
|---|---|---|---|
| 1. **Polling** | the agents query the table again and again (`WHERE id > last_id`) | misses payments that commit out of `id` order, as the poller of *Line Guardian* did (R2); every agent adds queries on the system of record (R1) | query load, for every reader |
| 2. **Dual write** | the card processor also sends every payment to Kafka after its commit | changes the certified payment path (R1); a crash between the commit and the send loses the event, a send before the commit invents a payment that was rolled back (R2) | none, but the application changes |
| 3. **Outbox** | the card processor writes the payment and an "outbox" row in the *same* database transaction; a relay publishes the outbox to Kafka | correct and complete, but it also changes the payment path (R1) | one extra row per transaction |
| 4. **Log-based CDC** | Debezium reads PostgreSQL's write-ahead log (WAL), as in the Guide, and publishes every committed change | nothing for R1 or R2: no code change, no queries, every commit, in commit order | a *replication slot*: PostgreSQL keeps all WAL the connector has not read yet. If the connector stops for a weekend, the disk can fill up, and the database (and with it the payment path) stops |

Dual write is also the most common mistake in AI systems: an application
that writes a product to its database *and* to a vector index for RAG gets
the same lost or invented updates. CDC (or an outbox) is the usual fix.

**Your task (for the write-up):** one paragraph, 5–8 sentences. Which
option do you choose for this scenario, and why? What does it cost, and how
would you notice in production before it hurts? Which option would you
choose if your team *owned* the payment path?

### Part 2 — The pipeline in three runs (in the session)

**How many agents.** The velocity model costs 2 ms per payment, so one
instance handles at most 500 payments/s, in practice 400–500 (reading the
JSON events costs time too). For 1,000 payments/s we run **3 velocity
instances**. The anomaly model (0.3 ms) manages with **one**. `agents.py`
starts exactly these. (Lecture: *Keys and partitions are capacity
decisions*.)

**The agent.** `fraud_agent.py` is complete. Read it once: three design
decisions are marked `DECISION` in the code.

| | Decision | Why |
|---|---|---|
| 1 | one consumer group per model (`anomaly-agent`, `velocity-agent`) | every group gets every payment (R2); the instances of one model share the partitions of their group (Exercise 5, Part 1, step 3) |
| 2 | event time: the payment's `created_at`, not the clock of the agent | after an outage, or when last month is replayed in an hour, the clock of the agent would squeeze many minutes of payments into one 60-s window, and the velocity model would flag almost everybody (lecture: *Event time vs. processing time*) |
| 3 | alerts keyed by cardholder | case management gets all alerts of one cardholder in order |

**The routine: how every run works.** A *run* measures one version of
`fraud-connector.json` from a clean start. You do three runs, and change
the file before Run 2 and before Run 3, so each run repeats the same five
steps. Why all five? The connector reads its settings only when it is registered, a topic
keeps the partitions and keys it was created with, and the scoreboard only
counts the payments since `card_processor.py` last started. Editing the
file alone changes nothing that is already running.

Use three terminals in `fraud/`, virtual environment active:

1. **Stop everything from the previous run:** `Ctrl+C` in the terminal of
   the agents and in the one of the card processor. (Before Run 1 there is
   nothing to stop.)
2. **Terminal 1: register the connector** in `fraud-connector.json`. If a
   previous one exists, this removes it, with its replication slot and its
   topic, so the new topic is created with your new settings. It should
   print `RUNNING`:

   ```bash
   cd fraud
   python3 connector.py fraud-connector.json
   ```

3. **Terminal 1: start the payment path,** and keep it running. Every start
   is a new run; the scoreboard only counts the current one.

   ```bash
   python3 card_processor.py
   ```

4. **Terminal 2: start the agents** (1 anomaly, 3 velocity) on the new
   topic.

   ```bash
   python3 agents.py
   ```

5. **Wait about 2 minutes** (the first fraud comes after 30 s, then one
   every 8 s), **then terminal 3: the scoreboard.**

   ```bash
   python3 scoreboard.py
   ```

What to look at: in the agents' output, `assigned partitions [...]` and
`lag`; on the scoreboard, the lines `R3` and `R4` of the velocity model; in
the Kafka UI (<http://localhost:8080>), *Topics →
fraud.public.transactions* (the number of partitions; *Messages*: the key
of an event) and *Consumers*.

For every run, note in your write-up: your **prediction**, the
**velocity lines** of the scoreboard, and **why**, in your own words.

#### Run 1 — Debezium's defaults

`fraud-connector.json` only says how to reach the database and which table
to read. Debezium decides everything else.

**Predict:** do all three velocity instances share the work, and does R4
pass? Write down your guess and your reason, then run the routine (steps
2–5; there is nothing to stop yet). Also look in the Kafka UI: how many partitions does the topic have, and what is
the key of an event?

<details>
<summary><b>What happened, and why</b> (open after you ran it)</summary>

![Run 1: Debezium writes 1,000 payments/s into the topic fraud.public.transactions, which has only partition 0, so the backlog grows by about 500 payments every second. In the group velocity-agent, vel-1 reads partition 0 at about 500 payments/s at most; vel-2 and vel-3 are assigned no partition and stay idle: one partition is read by one instance of a group.](images/run1-one-partition.png)

Only one velocity instance got a partition (`assigned partitions [0]`); the
other two print `assigned partitions []` and do nothing. The busy one
handles ~400–550 payments/s of 1,000, so its lag grows every second: **R4
fails**, and frauds of the last minute are not even caught yet, their
payments still wait in the backlog.

The scoreboard shows it too: `velocity-agent` has `members 3` but
`partitions 1`, and only `vel-1` appears under *Agent instances*. The other
two did not crash: an instance that never gets a payment never reports.

Nobody told Debezium how many partitions to use, so the topic was created
with **1 partition**. Within a consumer group, each partition is read by
exactly one instance: **partitions cap parallelism**, and instances beyond
the number of partitions stay idle.

The anomaly model passes: one instance at 0.3 ms per payment handles
~2,500 payments/s.

Next run: let Debezium create the topic with 6 partitions, enough for
3 instances and room to grow to 6.
</details>

<details>
<summary>Learning note: why the connector sets <code>decimal.handling.mode</code></summary>

`amount` is `NUMERIC(10,2)` in PostgreSQL, an exact decimal, as money must
be. JSON has no exact decimal type, so by default Debezium sends the bytes
of the unscaled integer, base64-encoded, and puts the scale into the
`schema` part of the event: `32.74` arrives as `"DMo="`. The connector
therefore sets `"decimal.handling.mode": "double"`, and the models get a
plain number, at the price of exactness (a `double` cannot represent `0.10`
exactly). That is fine for a fraud score, but wrong for an accounting
ledger built from the same events: there, `string` or the default `precise`
keeps every cent. The encoding of every column is part of the *data
contract* between the database and all its consumers, and one more
decision Debezium makes for you if you do not.
</details>

#### Run 2 — more partitions

Add these two lines inside `"config"` of `fraud-connector.json` (mind the
commas; replication factor 1, because we have a single broker):

```json
    "topic.creation.default.partitions": "6",
    "topic.creation.default.replication.factor": "1",
```

**Predict:** now all three instances work. Will the velocity model catch
≥ 90 % of the card-testing frauds (R3)? Think of the key you saw in Run 1.
Then save the file and repeat the **whole routine, from step 1**: stop the
agents and the card processor, run `connector.py` again, restart both, wait,
scoreboard. You know it worked when the agents print `[vel-1] assigned
partitions [0, 1]`, `[vel-2] … [2, 3]`, `[vel-3] … [4, 5]`.

<details>
<summary><b>What happened, and why</b> (open after you ran it)</summary>

Every instance gets two partitions and keeps up: **R4 passes**. But the
velocity model catches **almost no frauds (0 or 1 of 4): R3 fails**. The
anomaly model still passes.

![Left, Run 2, key = payment id (Debezium's default): the 12 test payments of one stolen card are spread over all six partitions, two in each; vel-1 (P0, P1), vel-2 (P2, P3) and vel-3 (P4, P5) each count only 4 of them, below the threshold of 8: no alert, no instance sees enough of the attack. Right, Run 3, key = cardholder (message.key.columns user_id): all 12 payments land in P3, vel-2 counts 12 and alerts at the 8th; all payments of a cardholder meet in one instance, in commit order.](images/runs-key.png)

The key of every event is the table's primary key, `{"id": ...}`, Debezium's
default. The partition is `hash(key) mod 6`, so the 12 test payments of one
stolen card land on random partitions, spread over the three instances.
Each instance sees only about 4 of them in its 60-s window, below the
threshold of 8: **no instance sees the whole attack**.

Why does the anomaly model still pass? It runs a single instance, which
reads all six partitions and sees every payment. Scaled to two instances,
it would fail the same way.

The rule: **the key is the entity whose state the model keeps**, here the
cardholder (`user_id`), not the primary key of the table (lecture: *Keys
and partitions are capacity decisions*).
</details>

#### Run 3 — the cardholder as the key

Add this line inside `"config"`:

```json
    "message.key.columns": "public.transactions:user_id",
```

**Predict:** what will the key of an event look like in the Kafka UI, and
what changes on the scoreboard? Then save the file and repeat the **whole
routine, from step 1**, as in Run 2.

<details>
<summary><b>What happened, and why</b> (open after you ran it)</summary>

The key is now `{"user_id": ...}` (right half of the figure in Run 2).
All payments of a cardholder land in the same partition, are read by the
same instance, in commit order. The velocity model catches its frauds (R3),
all instances keep up (R4), and every model gets every payment (R2): this is
the final design.

Now and then the scoreboard shows one false velocity alert (still `PASS`):
an honest cardholder who happened to pay 8 times within a minute. The rule
cannot tell them from a thief; that is the price of any threshold, and R3
allows a few.

You changed two defaults that Debezium chose for you: the number of
partitions (1) and the key (the primary key). Both are fine for what
Debezium does by default, copying a table. Both are wrong for a stateful
model that has to scale. And note *how* they failed: nothing crashed, no
error message. Without a scoreboard, the fraud model would simply have
caught fewer frauds. The lecture slide *Operating CDC: defaults are
decisions* lists more of these defaults.
</details>

If R4 fails in Run 3 on your laptop, start one more velocity instance
(`python3 agents.py --velocity 4`), or run the card processor with
`--tps 500`.

### Beyond this lab

Things a production pipeline must also handle, for reading (not part of
the write-up):

- **Crashes.** If a velocity instance crashes, its partitions move to the
  other instances after the session timeout (~45 s), and the per-cardholder
  counts it kept in memory are lost: a fraud in progress can be missed.
  Production systems keep such state where a crash cannot lose it: in a
  stream processor that saves it with checkpoints (lecture *Stream
  Processing*, later in the course), or in an online feature store
  (lecture: *Real-time features: one definition for training and
  serving*).
- **Duplicates.** A restarted agent re-reads the payments after its last
  committed offset, so some alerts can arrive twice. Case management can
  drop duplicates by `alert_id`, or the agent can use Kafka transactions
  (lecture: *Exactly-once stops at Kafka's edge*).
- **Catch-up.** After an outage, the backlog has to be worked off on top of
  the live traffic: size instances and partitions with headroom, not just
  for the normal rate (lecture: the worked example on *Keys and partitions
  are capacity decisions*).

## Submission

Submit in **two steps**, both by the deadline:

1. **Push** your work to your GitHub repository (forked from ours), in the
   folder `Exc_6/solution/`. Do not change the files outside it: they are
   the exercise, and you will sync them from our repository.
2. **Add the link** to that folder on GitHub to the **Moodle submission**.

What to submit, one page in total, as `Exc_6/solution/README.md`:

1. your **decision** paragraph (Part 1);
2. for each of the **three runs**: your prediction, the velocity lines of
   the scoreboard (`R3`, `R4`), and why, in 2–3 sentences of your own;
3. one closing answer: Debezium chose two defaults that you had to change.
   What would have happened in production if nobody had checked them, and
   how could the team have noticed?
4. your final connector, as `Exc_6/solution/fraud-connector.json`.

A wrong prediction is fine, and expected: what counts is that you can
explain what happened.

**Deadline: 18.11.2026, 18:00**, for the push and the Moodle link. An
exercise that is submitted too late, or without the link in Moodle, will be
considered missing.

Be ready to shortly present your solutions (5–8 minutes) in the next exercise
session (19.11.2026).

## Optional self-study — replication and broker failures

Everything above ran on a **single** broker: if it fails, the pipeline
stops. If you want to see what `acks`, the in-sync replicas and
`min.insync.replicas` really guarantee when brokers fail, follow
[`replication/README.md`](replication/README.md) (needs ~2 GB of RAM for
Docker). Not graded.

## Clean up

```bash
docker compose down -v
```

(If you did the replication self-study: `docker compose down` in the `replication` folder too.)

## Troubleshooting

- **`port is already allocated`**: the Exercise 5 containers are still
  running. Run `docker compose down -v` in the `exercise5` folder.
- **`curl: (52) Empty reply` / connection refused on port 8083**: Kafka
  Connect is still starting. Wait ~30 s and retry.
- **Connector `FAILED`**: `connector.py` prints the first line of the
  error (again: `python3 connector.py fraud-connector.json --status`).
  Usually a typo or a missing comma in a line you added. Fix the file and
  run `python3 connector.py fraud-connector.json` again.
- **`replication slot ... is still in use`**: two connectors use the same
  `slot.name`. Give every connector its own slot name.
- **Old slots**: PostgreSQL keeps replication slots (and the WAL they hold
  back) until somebody drops them. List and drop unused ones in
  `psql -d mydb`: `SELECT slot_name, active FROM pg_replication_slots;` and
  `SELECT pg_drop_replication_slot('<name>');`
- **Connector `RUNNING`, but no topic `fraud.public.transactions`**: the
  card processor is not running (no changes, no events).
- **The agents print nothing**: is the connector `RUNNING` and the card
  processor running? Did you start the agents *after* `connector.py`
  (steps 2–4 of the routine)?
- **You changed `fraud-connector.json`, but nothing changed** (still
  `assigned partitions []`, still the same key): the file is only read when
  you register it. Repeat the whole routine from step 1.
- **The scoreboard says "no fraud of this kind yet"**: wait a bit longer;
  the first fraud comes 30 s after `card_processor.py` starts. The
  scoreboard only counts the current run.
- **Your laptop cannot keep up** (`card_processor.py` prints far fewer than
  1,000 payments/s): run it with `--tps 500`.
- **Replication self-study**: see the Troubleshooting section of
  [`replication/README.md`](replication/README.md).

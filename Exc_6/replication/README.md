# Optional self-study — Replication and broker failures

**Goal:** see with your own eyes how Kafka keeps data available when brokers
fail, what `acks` and `min.insync.replicas` really guarantee, and what
durability costs in throughput and latency.

This activity is **optional** and self-paced: do it at home after the
session. It needs **~2 GB of RAM** for Docker (four
Kafka processes). It builds on part 2 of the lecture "Messaging Platforms –
Advanced", *Durability & delivery* (the write path and the high watermark,
acks and in-sync replicas, the idempotent producer), and on the
*Replication* slides of its appendix (leaders and followers, controllers,
what if all in-sync replicas die).

## Background (5-minute read)

| Term | Meaning |
|---|---|
| **Replication factor (RF)** | how many brokers keep a copy of each partition; set per topic |
| **Leader** | the replica that serves all reads and writes of a partition |
| **Followers** | the other replicas; they continuously *fetch* from the leader |
| **ISR** (in-sync replicas) | the replicas that are caught up with the leader; a follower that falls behind or dies is removed from the ISR |
| **High watermark (HW)** | the offset up to which records are *committed* (copied to the ISR); consumers only see records below the HW |
| **`acks`** (producer) | when the producer gets its confirmation: `0` never waits, `1` leader has written it, `all` every ISR member has it |
| **`min.insync.replicas`** (topic) | with `acks=all`: the minimum ISR size for a write to be accepted |
| **Controller** | elects leaders and maintains the ISR (KRaft: controllers store the cluster metadata in a replicated log) |

The cluster of this activity (`docker-compose.yml` in this folder):

```text
                 +------------------+
                 |    controller    |   KRaft controller: elects leaders,
                 +------------------+   tracks the ISR (no data)
                   /       |       \
       +----------+  +----------+  +----------+
       | broker-1 |  | broker-2 |  | broker-3 |   each broker keeps one
       |  P0 P1 P2|  |  P0 P1 P2|  |  P0 P1 P2|   replica of each partition
       +----------+  +----------+  +----------+   (topic "payments", RF 3)
```

The controller runs separately so that stopping brokers never stops it:
this way you observe the **data** side of replication only.

## Setup

Stop the Exercise 6 environment first (it needs the RAM), then start the
cluster **from this folder**:

```bash
docker compose down            # in the exercise6 folder
cd replication
docker compose up -d
```

`docker ps` should show `controller`, `broker-1`, `broker-2` and `broker-3`.
Wait ~20 s until the brokers have registered.

All commands run inside `broker-3`, because we will stop the other two.
The Kafka tools are on the container's `PATH`. The cluster uses no ports on
your machine, so it does not conflict with other containers.

## Step 1 — A replicated topic

Create a topic with 3 partitions, replication factor 3 and
`min.insync.replicas=2`, and describe it:

```bash
docker exec -it broker-3 kafka-topics.sh --bootstrap-server broker-3:9092 \
  --create --topic payments --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

docker exec -it broker-3 kafka-topics.sh --bootstrap-server broker-3:9092 \
  --describe --topic payments
```

**Look for:** which broker leads which partition, the order of the
`Replicas` list (the first one is the *preferred* leader), and the `Isr`.

## Step 2 — Write with `acks=all`

Type a few lines (one record per line), end with `Ctrl+C`, then count the
records per partition (`topic:partition:offset`):

```bash
docker exec -it broker-3 kafka-console-producer.sh --bootstrap-server broker-3:9092 \
  --topic payments --producer-property acks=all

docker exec -it broker-3 kafka-get-offsets.sh --bootstrap-server broker-3:9092 --topic payments
```

`kafka-get-offsets.sh` reports the **high watermark** of each partition, the
offset consumers can read up to.

## Step 3 — One broker fails

```bash
docker stop broker-1
```

Wait ~10 s, then describe the topic again (command of step 1) and list the
partitions that lost a replica:

```bash
docker exec -it broker-3 kafka-topics.sh --bootstrap-server broker-3:9092 \
  --describe --under-replicated-partitions
```

Produce again with `acks=all` (step 2) and count the records.

**Look for:** who leads partition 0 now? How long did the failover take?
Did the producer notice anything?

## Step 4 — Two brokers fail

```bash
docker stop broker-2
```

Describe the topic again, then:

1. produce with `acks=all` and watch the producer's output;
2. produce with `acks=1` (`--producer-property acks=1`); does the producer
   report an error?
3. count the records (step 2) and read the topic (`Ctrl+C` to stop):

   ```bash
   docker exec -it broker-3 kafka-console-consumer.sh --bootstrap-server broker-3:9092 \
     --topic payments --from-beginning
   ```

4. Where are your `acks=1` records? Look **inside** broker-3's log files:

   ```bash
   docker exec -it broker-3 kafka-dump-log.sh --print-data-log \
     --files /tmp/kafka-logs/payments-0/00000000000000000000.log
   ```

   (Repeat with `payments-1` and `payments-2`.) The `payload` field is your
   record. Have a look at the `sequence` field too: which producer feature
   from the lecture is that?

**Look for:** the difference between *written to the leader's log* and
*visible to consumers*. Compare the offsets of step 4.3 with what you find in
the log files.

## Step 5 — Recovery

```bash
docker start broker-2
```

Wait ~15 s, describe the topic, count the records and read the topic again.

**Look for:** what happened to the `acks=1` records of step 4? Which
component decided that they may now be read?

Bring the last broker back and check who leads which partition:

```bash
docker start broker-1
```

The leaders do not move back immediately. Kafka rebalances leadership
automatically after a few minutes, or on request:

```bash
docker exec -it broker-3 kafka-leader-election.sh --bootstrap-server broker-3:9092 \
  --election-type preferred --all-topic-partitions
```

Why does it matter which broker leads which partition?

## Step 6 — The price of durability

Measure the producer with each `acks` setting (`0`, `1`, `all`; change the
last argument):

```bash
docker exec -it broker-3 kafka-topics.sh --bootstrap-server broker-3:9092 \
  --create --topic perf --partitions 3 --replication-factor 3 --config min.insync.replicas=2

docker exec -it broker-3 kafka-producer-perf-test.sh --topic perf \
  --num-records 300000 --record-size 200 --throughput -1 \
  --producer-props bootstrap.servers=broker-3:9092 acks=all
```

The last line of the output summarises the run. Run each setting twice; the
first run warms up the JVM.

## Results sheet

| Step | Brokers running | Leaders (P0, P1, P2) | ISR of P0 | `acks=all` accepted? | `acks=1` accepted? | `acks=1` records readable? |
|---|---|---|---|---|---|---|
| 2 | 1, 2, 3 | | | | – | – |
| 3 | 2, 3 | | | | – | – |
| 4 | 3 | | | | | |
| 5 | 2, 3 | | | | – | |

| `acks` | records/sec | avg latency (ms) | 99th percentile (ms) |
|---|---|---|---|
| 0 | | | |
| 1 | | | |
| all | | | |

## Check yourself

1. With RF 3 and `min.insync.replicas=2`, how many brokers can fail before
   `acks=all` producers get errors? And before data is lost?
2. Why did `acks=1` look successful in step 4, and why is that misleading?
3. What would happen with `min.insync.replicas=1` in step 4?
4. Which `acks` setting would you choose for the fraud transactions of
   *Card Sentinel* (Exercise 6), and which for website click events? Why?
5. Why are the numbers of step 6 optimistic compared with a real cluster
   spread over several machines or availability zones?

If you did this activity, add your results sheet and answers to your
Exercise 6 submission; we discuss them in the next session.

## Clean up

```bash
docker compose down
```

## Troubleshooting

- **Brokers exit or restart in a loop**: Docker has too little memory. Give
  Docker Desktop at least 4 GB (Settings → Resources) and stop other
  containers.
- **`Timeout expired while fetching topic metadata`** right after start-up:
  the brokers are still registering with the controller. Wait 20 s and retry.
- **The producer hangs for a few seconds** after you stop a broker: it is
  waiting for the new leader's metadata. This is the failover you are
  observing.
- **`Error response from daemon: No such container: broker-3`**: run
  `docker compose up -d` in the `replication` folder.

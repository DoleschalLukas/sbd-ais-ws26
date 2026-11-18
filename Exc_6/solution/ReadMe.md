**Your task (for the write-up):** one paragraph, 5–8 sentences. Which
option do you choose for this scenario, and why? What does it cost, and how
would you notice in production before it hurts? Which option would you
choose if your team *owned* the payment path?

I would use log-based CDC with Debezium. It reads commited payments from PostgreSQL's log without changing the certified payment or adding queries. Publishing the payments to kafka lets each model consume payments independently of eachother. However PostgreSQQL must retain unread WAL data for replication, so prolonged connector outages lead to a potential stop in payment processing. Therefore we need to monitor connector health and size of the retained WAL to prevent this from happening.
I would personally choose the outbox if I owned the payment plan. Since by writing the payment to an outbox table in the same transaction, either both commit or neither. So a crash wouldnt lose the event.

#### Run 1 — Debezium's defaults

`fraud-connector.json` only says how to reach the database and which table
to read. Debezium decides everything else.

**Predict:** do all three velocity instances share the work, and does R4
pass? Write down your guess and your reason, then run the routine (steps
2–5; there is nothing to stop yet). Also look in the Kafka UI: how many partitions does the topic have, and what is
the key of an event?

**Prediction:**
No, I don't think all three velocity instances will share the work. With default configuration, the topic has one partition, which can be assigned to only one instance in the consumer group, leaving the other two idle. That instance cannot keep up with 1,000 payments per second, so the backlog grows and R4 fails.

**Results:**

Run 1795037245: started 21:27:25, 195,334 transactions in 195 s (1,001/s), 22 frauds so far (still running)

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   6        1    48,495
  velocity-agent           fraud.public.transactions                   6        3     3,468

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1          54417 anomaly         149,935       77,621             0     46.1       2s ago
  vel-1          54418 velocity         48,868       38,552             1      2.6       2s ago
  vel-2          54419 velocity         48,556       38,491             1      2.1       2s ago
  vel-3          54420 velocity         48,935       38,609             0      2.4       2s ago

key:
{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "int32",
				"optional": false,
				"default": 0,
				"field": "id"
			}
		],
		"optional": false,
		"name": "fraud.public.transactions.Key"
	},
	"payload": {
		"id": 498590
	}
}

#### Run 2 — Debezium's defaults

**Prediction:**
The velocity model will probably not catch at least 90% of card-testing frauds, even though all three instances can now share the work. Events are still keyed by transaction id, so payments from the same cardholder can land in different partitions and be processed by different instances. 
Each instance sees only part of the cardholder’s payment history, so it may never count enough payments to trigger an alert. The extra partitions should help instances to keep up, so R4 is expected to pass.


**Results:**
Run 1795036767: started 21:19:27, 345,355 transactions in 345 s (1,001/s), 40 frauds so far (still running)

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   1        1   784,544
  velocity-agent           fraud.public.transactions                   1        3    26,438

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1          37680 anomaly          45,862       36,849             0   5011.3       1s ago
  vel-1          37681 velocity         14,195       13,214             0     26.2       1s ago

{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "int32",
				"optional": false,
				"default": 0,
				"field": "id"
			}
		],
		"optional": false,
		"name": "fraud.public.transactions.Key"
	},
	"payload": {
		"id": 1225705
	}
}

#### Run 3 — the cardholder as the key
**Prediction:** The event key in the Kafka UI will contain the cardholder ID, for example . This sends all payments for the same cardholder to the same partition, so one velocity instance sees the complete history it needs to detect card testing.

On the scoreboard, I expect R3 to pass because the velocity model can now catch at least 90% of its frauds. R4 should still pass because the six partitions let the three velocity instances share the work.

**Results:**

Run 1795037949: started 21:39:09, 240,371 transactions in 240 s (1,001/s), 27 frauds so far (still running)

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   6        1    87,733
  velocity-agent           fraud.public.transactions                   6        3     4,809

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1         110316 anomaly         157,033       79,272             0     84.3       4s ago
  vel-1         110317 velocity         50,752       26,109             0      1.3       4s ago
  vel-2         110318 velocity         50,856       26,069             0      1.2       4s ago
  vel-3         110319 velocity         50,926       26,077             0      1.0       4s ago


{
	"schema": {
		"type": "struct",
		"fields": [
			{
				"type": "int32",
				"optional": true,
				"field": "user_id"
			}
		],
		"optional": false,
		"name": "fraud.public.transactions.Key"
	},
	"payload": {
		"user_id": 169145
	}
}

#### one closing answer:
Debezium chose two defaults that you had to change. What would have happened in production if nobody had checked them, and howcould the team have noticed?

Debezium’s defaults were one partition and the transaction id as the key. One partition causes a backlog and late alerts; adding partitions without changing the key can cause missed fraud by splitting a cardholder’s payments across instances. The team could monitor consumer lag and alert delay, and compare model results with confirmed fraud cases.
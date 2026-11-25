# Exercise 8: The crash report, live

In Exercise 7, each batch run copied a finished range of records into the
lake. A stream never finishes: now a Spark Structured Streaming query computes
the **crash report** (users with more than 2 severe crashes in a 10-second
interval, from the lecture demo) micro-batch by micro-batch and writes
alerts into the Kafka topic `crash_alerts`.

| | Exercise 7 (batch) | Exercise 8 (stream) |
|---|---|---|
| Input | a finished range of records per run | records that never stop, some late |
| Remembers where it stopped | your choice: state file, lake, ... | Spark's *checkpoint* |
| Question | is every record there exactly once? | **when is an interval complete?** |

Two teams read the alerts:

| Team | Needs |
|---|---|
| **On-call engineer** | an alert at most **20 s** after the interval ends, for 95 % of the problems (*p95*) |
| **Operations team** | a **correct** report (at least 99 % of the rows), late records included |

*p95* (95th percentile): 95 % of the alerts arrive within this time, the
slowest 5 % may take longer; *p50* (the median): half of them.

The catch: 5 % of the records arrive **late**, by up to 30 s (phones keep
their logs while offline). Waiting for them makes the alerts slow; not
waiting makes the report wrong.

**Your task:** choose an architecture that decides who waits, implement it in
a few lines, and check what each team gets.

## Part 0: setup

Stop the containers of Exercise 7 (`docker compose down` in that folder).
Then, in this folder (same images as Exercise 7, nothing new to download):

```bash
docker compose up -d
```

Open <http://localhost:8888/?token=lab> and the notebook
`work/exercise8.ipynb`. Kafka UI is at <http://localhost:8080>, the **Spark
UI** at <http://localhost:4040> (once the notebook has started Spark). The
notebook creates the topics and writes the log records itself.

## Part 1

1. **Read and run the example.** Sections 1-4 of the notebook hold all the
   code: the Spark query (read, crash report, write) and the two teams'
   readers (`pager`, `ops_view`); section 5 runs it (~3 minutes). While it
   runs, watch the query in the **Spark UI** (*Structured Streaming*) and
   the topic `crash_alerts` in Kafka UI. Keep the Spark UI open for the
   whole exercise: it shows what your query does in every micro-batch.
   Which team is not served, and why?
2. **Choose an architecture:**
   - **A — Wait:** one stream that waits long enough for late records
     (watermark 30 s). Every alert is final.
   - **B — Alert early, correct later:** one stream that sends an alert as
     soon as it sees 3 crashes and a new version whenever the count changes
     (output mode `update`). Readers keep the latest version of each alert.
   - **C — Stream for speed, batch for correctness (Lambda):** a fast stream
     for the on-call engineer; the operations team gets a report computed in
     batch from the lake, with Exercise 7's job.
3. **Implement** `start_query()` (section 6 of the notebook). For C, also
   `ops_report()`.
4. **Check** it (section 7): `check_all(start_query, pager, ops_view)` runs
   your query on 2 minutes of live records and restarts it once. For C:
   `check_all(start_query, pager, ops_report=ops_report)`.
5. **Look inside** (section 8): right after your check, take the three
   Spark UI screenshots the notebook asks for.

The notebook uses Spark. You may use another technology if it meets the
same goal: alerts in the topic `crash_alerts` (see `write_alerts` in the
notebook for the format) and, for C, a report that `ops_report()` returns.

In `Exc_8/solution/README.md`, explain:

1. **Your decision:** which architecture, why, and what it costs: which
   team waits, what every reader of `crash_alerts` must do, or whether the
   alerts and the report can disagree.
2. **Your query in Spark:** how it runs (micro-batches, state, watermark,
   checkpoint), with your three screenshots as evidence: why is your alert
   delay what it is, what does the query keep and lose, and what did the
   restart reuse? Which architecture of Exercise 7 does the checkpoint
   behave like?
3. **The pager:** what did it ignore in your run, and why does your
   architecture produce those messages? What would the on-call engineer
   get without its `if`?
4. **The output of `check_all`.**

Add your notebook as `Exc_8/solution/exercise8.ipynb` and the screenshots
as `ui-runs.png`, `ui-delay.png` and `ui-state.png`. Be ready to present
your solution in about 5 minutes in the next exercise session (03.12.2026).

## Submission

Push `Exc_8/solution/` to your fork and add the link to the Moodle
submission. **Deadline: 02.12.2026, 18:00.**

<details>
<summary>Troubleshooting</summary>

- **Container name already in use / port already allocated:** the containers
  of Exercise 7 are still running: `docker compose down` in that folder.
- **`No module named ...` or `Failed to find data source: kafka`:** check
  your internet connection, restart the kernel and run the cells again.
- **`check_all` says "Your query failed":** read the error below it (often a
  typo in `start_query()`), fix it and run the cell again.
- **The times look one hour off:** they are in UTC.
- **The Spark UI is empty or not reachable:** it exists only while the
  notebook's kernel runs. After a kernel restart, run `check_all` again. If
  another notebook holds port 4040, the UI is at <http://localhost:4041>.
- **Clean up:** `docker compose down` removes the containers and the topics.

</details>

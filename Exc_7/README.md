# Exercise 7: From the log to the lake

A batch job copies the Kafka topic `logs` into a data lake (Parquet files),
every few minutes. Each run **reads** the new records, **writes** them into
the lake, and **remembers** where it stopped.

**Your task:** choose how the job remembers where it stopped, implement it in
about five lines, and show that no record is ever missing or copied twice,
even if a run stops halfway.

## Part 0: setup

Stop the containers of Exercise 6 (`docker compose down -v` in that folder).
Then download the images (~5 GB, you need them next week too), start them,
and fill the topic with 2,000,000 log records:

```bash
docker compose --profile generator pull
docker compose up -d

docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic logs --partitions 4 --replication-factor 1

docker compose run --rm generator
```

Open <http://localhost:8888/?token=lab> and the notebook
`work/exercise7.ipynb`.

## Part 1 

1. **Run the example** (section 1 of the notebook). It copies the whole topic
   twice. Why does the checker fail?
2. **Choose an architecture:**
   - **A — State file:** the job saves where it stopped in a small file
     after writing.
   - **B — The lake is the truth:** the job finds where it stopped in the
     lake itself; it writes each run to a staging folder first, then moves
     it into the lake in one step.
   - **C — Remove duplicates when reading:** the lake may contain duplicates;
     readers use a view that keeps each record once.
3. **Implement** `run_once()` (section 3 of the notebook) with the building
   blocks listed there.
4. **Check** it (section 4): `check_all(run_once)` tests five situations,
   including runs that stop halfway. For C: `check_all(run_once, lake_sql=DEDUP_SQL)`.

The building blocks use Spark. You may use another technology if it meets
the same goal: no record missing, none twice, and the lake readable with
DuckDB (the checker in `tools/check_lake.py` works with any technology).

In `Exc_7/solution/README.md`, explain:

1. **Your decision:** which architecture and why
2. **Explain your result**
3. **The output of `check_all`.**

Add your notebook as `Exc_7/solution/exercise7.ipynb`. Be ready to present
your solution in about 5 minutes in the next exercise session (26.11.2026).

## Submission

Push `Exc_7/solution/` to your fork and add the link to the Moodle
submission. **Deadline: 25.11.2026, 18:00.**

<details>
<summary>Troubleshooting</summary>

- **Container name already in use / port already allocated:** the containers
  of Exercise 5 or 6 are still running: `docker compose down -v` in that
  folder.
- **The topic has 1 partition:** the generator ran before the topic was
  created. Delete it
  (`docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic logs`)
  and repeat Part 0.
- **`No module named ...` or `Failed to find data source: kafka`:** check
  your internet connection, restart the kernel and run the cells again.
- **Clean up:** `docker compose down` removes the containers and the topic.

</details>

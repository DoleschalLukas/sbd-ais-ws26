# Exercise 2 — Operation Data Plane with OLTPs

**Goal:** run analytical queries on a single-node OLTP database (PostgreSQL),
find out *where the time actually goes* when a query does not scale, and
discuss which fixes — query, hardware, or architecture — actually help.

## Part 0 — Before the session

The PostgreSQL image is ~450 MB. Downloading it over the classroom Wi-Fi is
slow when everybody does it at once, so please do this beforehand:

```bash
docker compose pull
```

Then check that:

1. `docker ps` shows the running container `pg-bigdata`.
2. `python3 --version` works on your machine (on Windows use `python` or `py`
   instead of `python3` in all commands below).

You need ~2 GB of free disk space. See [Troubleshooting](#troubleshooting) if something fails.

## Part 1 — Environment Setup and Basics (guided walkthrough, not graded)

### 1. Start the environment

If it is not running already:

```bash
docker compose up -d
```

### 2. Access PostgreSQL

```bash
docker exec -it pg-bigdata psql -U postgres
```

### 3. Load and query data in PostgreSQL

#### 3.1 Create a large dataset (in another terminal)

```bash
cd data
python3 expand.py
```

Creates `data/people_1M.csv` with 1 million rows (takes a few seconds).

```bash
wc -l people_1M.csv
```

#### 3.2 Enter PostgreSQL

```bash
docker exec -it pg-bigdata psql -U postgres
```

#### 3.3 Create and load the table

```sql
DROP TABLE IF EXISTS people_big;

CREATE TABLE people_big (
  id SERIAL PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  gender TEXT,
  department TEXT,
  salary INTEGER,
  country TEXT
);

\COPY people_big(first_name,last_name,gender,department,salary,country) FROM '/data/people_1M.csv' DELIMITER ',' CSV HEADER;
```

#### 3.4 Enable timing

```sql
\timing on
```

### 4. Verification

```sql
SELECT COUNT(*) FROM people_big;
SELECT * FROM people_big LIMIT 10;
```

### 5. Examples of analytical queries

#### (a) Simple aggregation

```sql
SELECT department, AVG(salary)
FROM people_big
GROUP BY department
LIMIT 10;
```

#### (b) Nested aggregation

```sql
SELECT country, AVG(avg_salary)
FROM (
  SELECT country, department, AVG(salary) AS avg_salary
  FROM people_big
  GROUP BY country, department
) sub
GROUP BY country
LIMIT 10;
```

#### (c) Top-N sort

```sql
SELECT *
FROM people_big
ORDER BY salary DESC
LIMIT 10;
```

> **Think about it:** does `LIMIT 10` make queries (a) and (b) cheaper?
> Compare with and without it using `EXPLAIN ANALYZE`.

Seems like it does not make the Query any cheaper

## Part 2 — Activities (graded)

### Activity 2.1 — PostgreSQL Analytical Queries (E-commerce)

Generate the dataset (in another terminal, from the `ecommerce` folder):

```bash
cd ecommerce
python3 dataset_generator.py
```

This writes `data/orders_1M.csv`, which is available inside the PostgreSQL
container as `/data/orders_1M.csv`. Its columns are:

```
customer_name, product_category, quantity, price_per_unit, order_date, country
```

Load the generated data into PostgreSQL in a **new table** called `orders`.
Write the `CREATE TABLE` yourself (choose sensible types for each column) and
use the same `\COPY` pattern as in section 3.3.

CREATE TABLE orders (
    customer_name VARCHAR(100),
    product_category VARCHAR(100),
    quantity INTEGER,
    price_per_unit NUMERIC(10,2),
    order_date DATE,
    country VARCHAR(100)
);


Using SQL ([list of supported SQL commands](https://www.postgresql.org/docs/current/sql-commands.html)),
answer the following questions:

**A.** Which order has the highest `price_per_unit`?

| customer_name | product_category | quantity | price_per_unit | order_date | country |
|---|---|---:|---:|---|---|
| Emma Brown | Automotive | 3 | 2000.00 | 2024-10-11 | Italy |


**B.** What are the top 3 product categories with the highest total quantity
sold across all orders?

| product_category | total_quantity |
|---|---:|
| Health & Beauty | 300842 |
| Electronics | 300804 |
| Toys | 300598 |

**C.** What is the total revenue per product category?
(Revenue = `price_per_unit × quantity`)

| product_category | total_revenue |
|---|---:|
| Automotive | 306589798.86 |
| Electronics | 241525009.45 |
| Home & Garden | 78023780.09 |
| Sports | 61848990.83 |
| Health & Beauty | 46599817.89 |
| Office Supplies | 38276061.64 |
| Fashion | 31566368.22 |
| Toys | 23271039.02 |

**D.** Who are the top 5 customers by total spending?

| customer_name | total_spending |
|---|---:|
| Carol Taylor | 991179.18 |
| Nina Lopez | 975444.95 |
| Daniel Jackson | 959344.48 |
| Carol Lewis | 947708.57 |
| Daniel Young | 946030.14 |

**E.** Look at the spending totals in D — and at how many orders each of those
customers has. What do you notice? Open `ecommerce/dataset_generator.py` and
explain *why* the data looks like this.

Names get randomly generated there are 32 first and 33 lastnames. This lets us create 32*33 = 1056 unique people. However our data creates a million people. This means that every person also exists nearly 1000 times in our dataset. Since every order has between 1 and 5 items with price depending on the category we can expect aggregation by name to all yield similar sums.

### Activity 2.2 — Why Is This Self-Join So Slow?

Users of the system sometimes run naive queries such as:

```sql
SELECT COUNT(*)
FROM people_big p1
JOIN people_big p2
  ON p1.country = p2.country;
```

On the full 1M-row table this takes **more than 10 minutes** and slows down
the whole database for everyone else. Your job is to find out *why* before
proposing a fix.

> **Do not run it on the full table in class** — use the smaller tables below.
> (If you are curious, run it at home and let it finish.)

**Step 1 — Measure how it grows.** Create three smaller copies of the table:

```sql
CREATE TABLE people_50k  AS SELECT * FROM people_big WHERE id <= 50000;
CREATE TABLE people_100k AS SELECT * FROM people_big WHERE id <= 100000;
CREATE TABLE people_200k AS SELECT * FROM people_big WHERE id <= 200000;
```

Run the self-join (with `\timing on`) on each of the three tables and fill in:

| rows in table | join result (`COUNT(*)`) | time |
|---|---|---|
| 50 000 | 27501822 | 1571.425 ms|
| 100 000 | 109946508 | 6280.004 ms |
| 200 000 | 439395606 | 21336.244 ms |

When the input **doubles**, by what factor do the result and the time grow?
Use this to **predict** the result size and the runtime on `people_big` (1M rows).

when the input doubles the runtime and result grows by 4x we have quadratic growth.

Going from 200 000 to 1M is 5x increase therefore factor 5² = 25
Time 1M = 533,406.1 ms
Result 1M = 10,984,890,150


**Step 2 — Does an index help?** Create an index on `country` of `people_100k`,
run `ANALYZE people_100k;`, and repeat the query. Did the time change? Use
`EXPLAIN ANALYZE` to support your answer. Why does (or doesn't) the index help?

Previous:  6280 ms
New:       6791 ms

Since we are effectively just mapping 110M entries to eachother an index does not really change much. Indexes are useful when finding few points of data quickly.

**Step 3 — Rewrite it.** The query only wants the *number* of matching pairs,
not the pairs themselves. If a country has *k* people, how many pairs does it
contribute to the join? Write a query that computes the **same number
without a join**. Check that it returns exactly the same result as the join on
`people_100k`, then run it on `people_big` and compare its runtime with your
prediction from Step 1.

We can group our data by country and sum all combinations for each country.

SELECT SUM(person_count * person_count) AS total_pairs
FROM (
    SELECT country, COUNT(*) AS person_count
    FROM people_100k
    WHERE country IS NOT NULL
    GROUP BY country
) sub;

Time: 69.477ms
Previous: 6280 ms

**Step 4 — Discussion (submit in writing).** Considering **scalability** and
**efficiency**, which approaches and/or optimizations can be applied to improve
this kind of query in a real system? Discuss at least:

- what the rewrite in Step 3 tells you about adding more hardware or an index;

The rewrite in Step 3 is a clear sign that not every problem can be solved by indexing or spending more on bigger better hardware. Some problems need to be optimized otherwise the whole system may crumble.

- what you would do if the business actually needed **the pairs themselves**
  (not just their count) — would a bigger machine or a cluster help, and how
  much?

I would assume that it would result in faster processing time. I calculated that 1M parameters for example would need about 10 minutes. Using a second cluster to split it in half we could optimally cut the time required in half. However this will not resolve the underlying problem that our data will scale astronomically if we want 2 million or even more data points paired. It might make sense to think of a solution to decrease what is being mapped or how.

- the limits of an **OLTP database** for this workload, especially in a
  **large-scale cloud environment**.

Usually OLTP systems process small transactions. Small inserts and reads without much overhead. When we now try to compute our mega join we will heavily overload memory, cpu and all processes. This could negatively impact customer experience during execution. Also heavy Cloud Computing becomes very expensive which should be avoided.


> **Optional:** support your answer with a diagram, SQL or code.

## Submission

Commit to the Github repository and add the link to the Github solution in the Moodle submission:

- **2.1:** your `CREATE TABLE` statement and the SQL for A–D with their
  results, and your answer to E;
- **2.2:** the table from Step 1 with your prediction, your observations from
  Steps 2–3 (including your rewrite query), and the written discussion from
  Step 4.

Be ready to shortly present your solutions (5–8 minutes) in the next exercise
session (05.11.2026).

## Clean up

```bash
docker compose down
```

This removes the container, **including the PostgreSQL tables** — the CSV
files in `data/` are kept.

Optionally, delete the generated datasets as well (~95 MB). They are
not needed any more, and running `expand.py` and `dataset_generator.py`
again recreates exactly the same files:

```bash
rm data/people_1M.csv data/orders_1M.csv
```

(Windows PowerShell: `Remove-Item data/people_1M.csv, data/orders_1M.csv`)

## Troubleshooting

- **`port is already allocated` / `address already in use`** — another
  PostgreSQL (port 5432) is running on your machine.
  Stop it, or change the left-hand port in `docker-compose.yml`
  (e.g. `"5433:5432"`).
- **`python3: command not found` (Windows)** — use `python` or `py`.
- **`\COPY ... No such file or directory`** — the CSV was not generated into
  the `data/` folder. Check that `data/people_1M.csv` / `data/orders_1M.csv`
  exist on your machine.

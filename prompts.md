# PROMPTS.md

This file documents the AI/LLM prompts used while building the **IMDb Lakehouse → OLAP
Pipeline**, per the assignment's disclosure requirement.

- **LLM used:** GitHub Copilot (Claude — Opus model)
- **Usage scope:** scaffolding boilerplate, debugging connector/runtime errors, and
  drafting documentation. All generated code was reviewed, adapted, and is fully
  explainable without AI assistance.


---

## 1. Environment & Infrastructure

- "Set up a Docker Compose file to run a Spark standalone cluster (1 master, 2 workers)
  plus a ClickHouse server on a shared network with persistent volumes."
- "How do I run the Spark master and worker processes directly via `spark-class` in the
  foreground inside the official `apache/spark:3.5.1` image?"
- "Auto-apply a ClickHouse schema `.sql` file on container startup."

## 2. Ingestion & ETL (PySpark)

- "Read the IMDb `title.basics.tsv` and `title.ratings.tsv` (tab-separated, header row)
  into PySpark and clean the `\N` null sentinels."
- "Split the pipe-delimited `genres` column into an array and cast numeric columns to
  proper types."

## 3. OLAP Schema Design (ClickHouse)

- "The assignment asks for indexes or primary keys — can I add a `PRIMARY KEY` and
  data-skipping (secondary) indexes to this schema, and how do they map to my queries?"
- "Explain the difference between `ORDER BY`, `PRIMARY KEY` in
  ClickHouse and when each helps."

## 4. Loading Parquet → ClickHouse

- "Draft PySpark code to read a Parquet file and load it into a ClickHouse table, plus
  the `spark-submit` command to run it via Docker."
- "Show both the native ClickHouse Spark connector approach and a JDBC alternative."

## 5. Troubleshooting (Connector & Runtime Errors)

Real errors encountered during integration and the debugging prompts used:

- "`spark-submit` fails with *'Provided Maven Coordinates must be in the form
  groupId:artifactId:version'* — why?" *(cause: a `:all` classifier in `--packages`)*
- "Writing to ClickHouse throws *'Magic is not correct - expect [-126]'* — what does this
  mean and how do I fix it?" *(root cause traced to authentication failure, not
  compression)*
- "ClickHouse returns *'Authentication failed: password is incorrect (REQUIRED_PASSWORD)'*
  on a curl test — how do I find and pass the correct password?"
- "Now I get *'ClassNotFoundException: org.apache.hc.core5.http2.HttpVersionPolicy'* with
  `--jars` — which dependency is missing?" *(missing `httpcore5-h2`; resolved by using
  `--packages` for transitive resolution)*
- "How do I create the jars folder and download aligned ClickHouse client jars inside the
  Spark container?"

## 6. Benchmarking

- "Write a PySpark benchmark script that runs three analytics queries with warm-up +
  multiple measured runs and reports median/mean/min/max to CSV."
- "Write an equivalent ClickHouse benchmark using `clickhouse-connect`."
- "Produce a comparison CSV showing the Spark-vs-ClickHouse speed-up per query."

## 7. Documentation

- "Generate a professional `README.md` covering the architecture, setup, schema,
  partitioning rationale, benchmark results, and a performance note justifying the OLAP
  engine choice — aligned to the assignment deliverables."

---

## Notes on AI Usage

- AI accelerated **boilerplate** (Docker Compose, spark-submit invocations, DDL scaffolds)
  and **error triage** (connector version conflicts, auth/compression issues).
- **Design decisions** — choice of ClickHouse, the `title_type` / `start_year` partitioning
  strategy, index selection, and benchmark methodology — were directed by me; the AI
  provided options which I evaluated and selected.
- All final code was **read, understood, and tested**; I can explain every line without AI.

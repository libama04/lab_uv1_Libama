# Lab answers

## Lab 2: Object storage with S3

**Why are the credentials stored in a Secret and not in the ConfigMap?**
Secrets are meant for sensitive data: separate RBAC, values hidden from `kubectl describe`, can be encrypted at rest. ConfigMaps are plain configuration.

**What happens if the Job runs again tomorrow? How would production provide credentials?**
The STS session token has expired: the upload fails with `ExpiredToken`. In production, the Job's ServiceAccount is mapped to a cloud role (workload identity / IRSA) or gets dynamic credentials from Vault, rotated automatically.

**How would you turn this Job into a daily ingestion?**
A `CronJob` with a daily `schedule` (e.g. `0 2 * * *`), or an orchestrator such as Airflow.

## Lab: SQL analytics with DuckDB

**Persistent secret in the home directory: risks, and why limited on Onyxia?**
Credentials stored in clear text, readable by any process of the user, may leak through backups or commits. On Onyxia they are temporary STS credentials, scoped to the user's bucket, in a personal pod.

**How would you restrict S3 access for a Kubernetes Job?**
A dedicated ServiceAccount with its own credentials, limited by policy to the needed bucket/prefix (read-only if possible), injected only into that Job.

**Why does `approx_unique` return 40 and 3167?**
It is a HyperLogLog estimate, cheap but approximate, not an exact distinct count.

**Types inferred from a sample: which issue with a non-representative file?**
A wrong type is inferred (e.g. integer, then text later in the file): the read fails or values become NULL. Fix with explicit types or a full scan.

**Why is the `uuid` column barely compressed?**
Random, unique, high-entropy values: no repetition for dictionary encoding or compression.

**Why is the compressed size of some columns larger than the uncompressed size?**
The data is already compact (binary, dictionary encoded); compression only adds its own overhead.

**Why is partitioning by `uuid` a bad idea?**
One tiny file per order: millions of objects, one request each, high latency and costly listings on object storage.

**Which partition column for orders growing every day?**
The order date (day or month).

**How many row groups, and how many are skipped?**
3 row groups. With our dataset (dates up to 2048) the filter `date >= '2040-01-01'` skips 1 of them (2020–2034), using the min/max statistics.

**What if the orders were shuffled?**
Every row group would span the whole date range: no row group could be skipped.

**CSV vs Parquet time: network vs parsing?**
CSV: 28.2 MiB transferred in 1.23 s; Parquet: 203.5 KiB in 0.32 s. Network: Parquet reads only 2 compressed columns. Parsing: CSV text must be fully parsed, Parquet is binary. Running the same queries on local files isolates the parsing part.

## Lab: Bronze to Silver with dbt and DuckDB

**Bronze files left untouched: benefit when a bug is found six months later?**
Fix the model and rebuild silver from the original raw data.

**Which format and layout for hourly orders over years?**
Parquet, partitioned by date (or a table format like Iceberg), with incremental models.

**What happens when a view is queried, and when is it preferable to a table?**
Its SQL runs on every query and re-reads the source. Preferable for small, cheap or always-fresh data.

**How many bytes does the `VARCHAR` representation of a UUID use?**
36 characters, so 36 bytes plus overhead, versus 16 bytes.

**Strategy for an age dashboard? For sales per region?**
Age: replace invalid birthdates with NULL. Sales per region: keep the records, the birthdate is irrelevant; monitor with a warning.

**Which test would detect random state/zip combinations, and which reference data?**
A test joining each address with the valid ZIP ranges of its state (official USPS data). Implemented with the `zip_ranges` seed and `assert_zip_code_matches_state`.

**Exercise 4: reference data considerations**
Source: an authoritative provider (USPS/Census). A seed is fine because it is small and versioned in Git. The validation belongs to silver. When the reference changes, update the seed and rebuild: results may change.

**Exercise 5: view vs table**
Tables are stored in `lab_medallion.duckdb` and computed at build time; views are computed at query time. When the CSV changes, a table is stale until the next build, a view is up to date. Tables use more storage but avoid re-reading the source files on every query.

## Lab: Spark RDD API

**What decides the default number of partitions of `textFile()`?**
`max(number of splits, defaultMinPartitions)`. Small file, 1 split, minimum 2: 2 partitions.

**`repartition()` vs `coalesce()`: which one shuffles?**
`repartition()` always shuffles. `coalesce()` merges partitions without shuffle (reduction only).

**Second `count()`: new stage? Why?**
Yes, nothing is stored: each action recomputes the whole lineage.

**When would `.cache()` change that?**
Called on `orders_data` before the first `count()`: the second count reads from memory.

**Why does `read.csv(multiLine=True)` not have the multiline issue?**
It understands quotes, a newline inside `"..."` stays in the value. `textFile()` splits on every newline: 101 lines for 50 users.

**What must be true of a format for a line-oriented reader?**
One record per line, no unescaped newline inside a value.

**What if `product` contained an extra comma?**
`split` returns 6 fields: `ValueError` at the first action touching the line, the job fails.

**Where does the DataFrame API check it, and when?**
In the CSV reader, while parsing with the schema, at execution. `mode` decides: PERMISSIVE (null), DROPMALFORMED, FAILFAST.

**What does `groupByKey` shuffle?**
Every (key, value) pair, with no map-side combine. Measured: 6,230 bytes vs 2,513 for `reduceByKey`.

**When is `groupByKey` unavoidable?**
When all values are needed together: median, percentiles, sorted list of values.

**What plays the role of `PARTITION BY` in the deduplication?**
The `reduceByKey` shuffle, hash partitioned on `order_id`.

**Which version states the intent more directly?**
The window function.

**How to skip the join shuffle?**
Co-partition both RDDs: same partitioner, same key, same number of partitions (`partitionBy` + cache). Or broadcast the small side.

**`join()` vs `leftOuterJoin()`?**
Orders without a user are kept, with `None` instead of being dropped.

**Risk of `.collect()` on `enriched`?**
The whole result goes to the driver: out of memory on a large dataset.

## Lab: Bronze to silver with Spark DataFrames

**What happens to a running job if the service restarts mid-lab?**
The job dies with the driver: `local` mode runs inside the service's pod. On restart, new STS credentials are issued and the session must be recreated.

**Why does `SimpleAWSCredentialsProvider` fail here?**
It only sends the access and secret keys. Temporary STS credentials also need the session token, which only `TemporaryAWSCredentialsProvider` sends.

**Explain the output of the state query.**
Users per state, with the number of NULL cities. Military regions (AE, AP, AA) have no city: their `missing_city` equals their user count.

**DataFrame or SQL?**
SQL for a one-off exploration, quick to write. DataFrame API for a shared model: composable functions, checked at build time, testable and reusable.

**Where else could an empty string silently produce a wrong result?**
`state` and `zip_code`: `regexp_extract` also returns `""` on no match, counted as a group instead of NULL.

**How many part-files under `users.parquet/`, and why?**
1 per output partition. The dataset is small and read from a single CSV split: 1 partition, 1 file (plus `_SUCCESS`).

**What do you lose with `coalesce(1)` on a larger dataset?**
Parallelism: a single task writes everything, slowly, and readers cannot split the work.

**Which files hold the data, which describe the table?**
Parquet files in `data/` hold the rows. `metadata/` (metadata JSON, manifest lists, manifests, `version-hint.text`) describes schema, snapshots and files.

**Overwrite while reading: Parquet vs Iceberg?**
Parquet: the reader may see missing or mixed files, or fail. Iceberg: the reader keeps its snapshot; the new one becomes visible atomically after commit.

**Two concurrent commits with a hadoop catalog?**
Both may write the same next version: one commit is lost or the table is corrupted. A catalog service (REST, Nessie, Polaris) provides atomic compare-and-swap of the current version.

**Schema differences? What about the missing UUID type?**
dbt: `uuid` and `timestamp with time zone`. Spark: `varchar` and `timestamp`. UUIDs are stored as 36-char strings: larger, no type validation, consumers must cast.

**One object (dbt) vs a directory of part-files (Spark)?**
A single object is simpler to read. Part-files scale: written and read in parallel.

**What replaced the dbt tests?**
`assert` on counts and columns. Same guarantees need explicit checks: unique and not-null keys, accepted values, joins for relationships (or a library such as Great Expectations).

**What plays the role of `ref()` and `source()`?**
Python variables and the order of the cells: dependencies are implicit, no graph.

**Why are build times not comparable?**
Spark pays JVM startup, package download and S3A setup, and is built for distributed data. DuckDB is embedded and in-process. On such tiny data, overhead dominates.

**10 GB, 10 TB, SQL-only analysts?**
10 GB: dbt + DuckDB. 10 TB: Spark. SQL-only analysts: dbt (on DuckDB, or Spark/Trino when large).

**Exercise 4: time travel, Iceberg vs Parquet**
After the append, Iceberg shows 2,830 rows and `VERSION AS OF` the previous snapshot returns 2,829. The Parquet directory also shows 2,830 but keeps no history: the previous state cannot be read back.

## Lab: Explore Iceberg tables with Spark

**Which files contain the rows? Which describe the table? Why more than Parquet?**
Parquet files in `data/` hold the rows. `metadata/` describes the table: `vN.metadata.json` (schema, partitioning, snapshots), manifest lists and manifests (Avro, one entry per data file with stats), `version-hint.text`. Without them there is no schema history, no snapshot, no atomic commit, no file-level statistics.

**How many snapshots and data files? What do snapshots add?**
Ours started with several snapshots (one per write of the previous lab) and 1 data file. A snapshot records its id, parent, operation, commit time and summary (files and rows added or removed): history that plain Parquet files do not have.

**After the append: snapshots, old files, why a new snapshot?**
One more `append` snapshot. The original Parquet file is still there, untouched: a new file holds the 3 new rows. A new snapshot makes the change atomic and keeps the previous state readable.

**What does time travel read? Why no copy? What if old files are deleted?**
It reads the metadata of an older snapshot and only the data files it references. Snapshots share unchanged files, so nothing is copied. If those files were physically deleted (e.g. by `expire_snapshots`), that version could no longer be read.

**Schema evolution: rewrite? where recorded? why table-level?**
No rewrite: old files simply have no `source` value (read as NULL). The new schema is recorded in a new `metadata.json`. Columns are tracked by id at table level, so all files, old and new, are read consistently.

**Compare Iceberg with Parquet**
`silver/spark/orders.parquet/` is only a collection of Parquet files; `warehouse/silver/orders/` has metadata and snapshots. To replace a Parquet directory safely, write to a new path and switch readers to it. Iceberg makes a new state visible atomically by swapping the pointer to the new metadata file. A catalog is needed when several engines share a table: one source of truth for the current metadata location, with atomic commits and access control.

**Exercise 6: Parquet vs Iceberg vs Iceberg catalog**
Parquet: a columnar file format. Iceberg: a table format, metadata on top of Parquet files (snapshots, schema evolution, atomic commits). Iceberg catalog: a service mapping table names to their current metadata, coordinating commits between engines.

**Note**
The cleanup step (deleting `warehouse/`) was not run: the next lab (Polaris) registers `lab.silver.orders` from this warehouse.

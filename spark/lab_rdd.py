"""Lab: Introduction to Spark's RDD API.

Run from the project root, after downloading the bronze datasets:
    spark-submit --master "local[*]" spark/lab_rdd.py
"""
import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession


def section(title):
    print(f"\n===== {title} =====")


def stage_shuffle_stats(sc, job_group):
    """Shuffle read/write bytes of the stages of a job group, read from the Spark UI REST API."""
    app_id = sc.applicationId
    base = sc.uiWebUrl
    jobs = json.load(urllib.request.urlopen(f"{base}/api/v1/applications/{app_id}/jobs"))
    stage_ids = [s for j in jobs if j.get("jobGroup") == job_group for s in j["stageIds"]]
    stages = json.load(urllib.request.urlopen(f"{base}/api/v1/applications/{app_id}/stages"))
    totals = {"shuffleWriteBytes": 0, "shuffleReadBytes": 0, "shuffleWriteRecords": 0}
    for s in stages:
        if s["stageId"] in stage_ids:
            for k in totals:
                totals[k] += s.get(k, 0)
    return totals


# SparkContext
spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print("Spark", spark.version, "| UI:", sc.uiWebUrl)

section("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users_lines partitions:", users_lines.getNumPartitions())
print("orders_lines partitions:", orders_lines.getNumPartitions())
print("defaultMinPartitions:", sc.defaultMinPartitions, "| defaultParallelism:", sc.defaultParallelism)
orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8):", orders_repartitioned.getNumPartitions())
print("after coalesce(1):", orders_repartitioned.coalesce(1).getNumPartitions())

section("Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print("orders_data.count():", orders_data.count())
print("orders_data.count() again:", orders_data.count())

section("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(repr(line))
print("users_lines.count():", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_records.count():", users_records.count())


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


section("Manual parsing of orders")
orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)

section("Key-value RDDs: total quantity per user")
sc.setJobGroup("reduceByKey", "reduceByKey")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("reduceByKey:", user_quantity.take(5))
sc.setJobGroup("groupByKey", "groupByKey")
user_quantity_grouped = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)
print("groupByKey: ", user_quantity_grouped.take(5))
sc.setJobGroup("other", "other")
print("same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))
print("shuffle reduceByKey:", stage_shuffle_stats(sc, "reduceByKey"))
print("shuffle groupByKey: ", stage_shuffle_stats(sc, "groupByKey"))

section("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("orders_rdd.count(), orders_dedup.count():", orders_rdd.count(), orders_dedup.count())

section("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
for e in enriched.take(3):
    print(e)
print("enriched.count():", enriched.count())
left = orders_kv.leftOuterJoin(users_kv)
print("leftOuterJoin count:", left.count(), "| without user:", left.filter(lambda kv: kv[1][1] is None).count())

section("Exercise 1: aggregateByKey (order_count, total_quantity)")
count_and_total = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
avg_quantity = count_and_total.mapValues(lambda ct: round(ct[1] / ct[0], 2))
for user_id, ct in count_and_total.take(3):
    print(user_id, ct)
print("average quantity per order, first users:", avg_quantity.take(3))
print("same totals as reduceByKey:",
      sorted(count_and_total.mapValues(lambda ct: ct[1]).collect()) == sorted(user_quantity.collect()))

section("Exercise 2: fake user_id and leftOuterJoin")
fake = {"order_id": "00000000-0000-0000-0000-000000000000", "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "date": "2020-06-01 00:00:00+00:00", "quantity": 1, "product": "bread"}
orders_with_fake = orders_rdd.union(sc.parallelize([fake]))
orphans = orders_with_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv).filter(lambda kv: kv[1][1] is None)
print("orders without matching user:", orphans.count(), "->", orphans.map(lambda kv: kv[0]).collect())

section("Exercise 3: the same aggregation with the DataFrame API")
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")  # the CSV column is user_uuid, not user_id
user_quantity_df.show(5, truncate=False)
print("same totals as reduceByKey:",
      sorted((r["user_uuid"], r["sum(quantity)"]) for r in user_quantity_df.collect()) == sorted(user_quantity.collect()))
user_quantity_df.explain()
print(user_quantity.toDebugString().decode())

section("Exercise 4: timing with and without cache")


def timed(label, fn):
    t0 = time.perf_counter()
    result = fn()
    print(f"{label}: {result} in {time.perf_counter() - t0:.3f}s")


for cached in (True, False):
    rdd = orders_data.map(parse_order)
    if cached:
        rdd = rdd.cache()
    uq = rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    label = "cached" if cached else "not cached"
    timed(f"{label} orders count #1", rdd.count)
    timed(f"{label} orders count #2", rdd.count)
    timed(f"{label} user_quantity count", uq.count)
    storage = json.load(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/storage/rdd"))
    print(f"{label} Storage tab: {len(storage)} cached RDD(s)",
          [(s["name"], s["numCachedPartitions"], s["memoryUsed"]) for s in storage])
    rdd.unpersist()

spark.stop()

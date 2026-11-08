"""Lab: Bronze to silver with Spark DataFrames.

Run from the project root:
    spark-submit --master "local[2]" --driver-memory 1g \
      --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 spark/lab_silver.py
"""
import json
import os
import re
import timeit
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def section(title):
    print(f"\n===== {title} =====", flush=True)


os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[2]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(spark, "| Spark", spark.version)

section("Bronze layer")
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True,
                              samplingRatio=0.01, multiLine=True)
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True,
                               samplingRatio=0.01, multiLine=True)
users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)

section("Silver layer: users, DataFrame API")
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

section("Silver layer: orders, window deduplication")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

section("Silver layer: Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame and SQL versions agree")
stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state, count(*) AS users, sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users GROUP BY state ORDER BY users DESC
""").show(10)

section("Write the silver layer as Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("Parquet read back:", users_silver.count(), "users,", orders_silver.count(), "orders")
print("users part-files:", len(users_silver.inputFiles()), "| orders part-files:", len(orders_silver.inputFiles()))

section("Write the silver layer as Iceberg tables")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)
users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("Iceberg read back:", users_iceberg.count(), "users,", orders_iceberg.count(), "orders")

section("Exercise 1: birthdate_is_valid, DataFrame API and Spark SQL")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn("birthdate_is_valid",
                F.col("first_ordered_at").isNull() | (F.col("birthdate").cast("timestamp") <= F.col("first_ordered_at")))
    .drop("first_ordered_at")
)
stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql("""
    SELECT u.*, (f.first_ordered_at IS NULL OR CAST(u.birthdate AS TIMESTAMP) <= f.first_ordered_at) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, min(ordered_at) AS first_ordered_at FROM stg_orders GROUP BY user_id) f
      ON u.user_id = f.user_id
""")
users_valid_df.groupBy("birthdate_is_valid").count().orderBy("birthdate_is_valid").show()
diff = users_valid_df.select("user_id", "birthdate_is_valid").exceptAll(users_valid_sql.select("user_id", "birthdate_is_valid")).count()
print("rows differing between DataFrame and SQL:", diff)

section("Exercise 2: zip_code with regexp_extract, Python UDF and pandas_udf")
ZIP = re.compile(r"([A-Z]{2}) ([0-9]{5})$")


@F.udf("string")
def zip_udf(address):
    if address is None:
        return None
    m = ZIP.search(address.split("\n")[-1])
    return m.group(2) if m else ""


def run(col):
    return users_bronze.select(col.alias("zip_code")).agg(F.count("zip_code")).collect()


variants = {
    "regexp_extract": F.regexp_extract(F.element_at(F.split("address", "\n"), 2), "([A-Z]{2}) ([0-9]{5})$", 2),
    "python udf": zip_udf("address"),
}
try:
    import pandas as pd

    @F.pandas_udf("string")
    def zip_pandas_udf(addresses: pd.Series) -> pd.Series:
        return addresses.str.split("\n").str[-1].str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")

    variants["pandas_udf"] = zip_pandas_udf("address")
except Exception as e:  # pandas or pyarrow missing
    print("pandas_udf unavailable:", e)
results = {}
for name, col in variants.items():
    run(col)  # warm-up
    t = min(timeit.repeat(lambda: run(col), number=1, repeat=5))
    results[name] = users_bronze.select(col.alias("z")).orderBy("z").collect()
    print(f"{name}: best of 5 = {t:.3f}s")
print("same zip codes for all variants:", all(r == results["regexp_extract"] for r in results.values()))

section("Exercise 3: cache the bronze DataFrames")
app, ui = spark.sparkContext.applicationId, spark.sparkContext.uiWebUrl
users_bronze.cache().count()
orders_bronze.cache().count()
storage = json.load(urllib.request.urlopen(f"{ui}/api/v1/applications/{app}/storage/rdd"))
print("Storage tab:", [(s["name"][:60], s["numCachedPartitions"], s["memoryUsed"]) for s in storage])
t0 = timeit.default_timer(); orders_bronze.groupBy("product").count().collect(); print(f"aggregation on cached orders: {timeit.default_timer() - t0:.3f}s")
users_bronze.unpersist(); orders_bronze.unpersist()

section("Exercise 4: append and time travel, Iceberg vs Parquet")
snap_before = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
new_order = spark.createDataFrame(
    [("00000000-0000-0000-0000-000000000001", stg_orders.first()["user_id"], None, 1, "bread")],
    "order_id string, user_id string, ordered_at timestamp, quantity int, product string",
).withColumn("ordered_at", F.current_timestamp())
new_order.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
print("current count:", spark.table("lab.silver.orders").count())
print("count VERSION AS OF", snap_before, ":", spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {snap_before}").first()[0])
# Parquet: work on a scratch copy, the silver directory is kept intact
scratch = f"s3a://{BUCKET}/scratch/orders.parquet"
stg_orders.write.mode("overwrite").parquet(scratch)
print("Parquet directory count before append:", spark.read.parquet(scratch).count())
new_order.write.mode("append").parquet(scratch)
print("Parquet directory count after append:", spark.read.parquet(scratch).count(),
      "(no previous version can be read back)")
# Keep the silver table clean: roll the Iceberg table back to the snapshot before the append
spark.sql(f"CALL lab.system.rollback_to_snapshot('silver.orders', {snap_before})")
print("after rollback_to_snapshot:", spark.table("lab.silver.orders").count())

spark.stop()

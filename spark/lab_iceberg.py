"""Lab: Explore Iceberg tables with Spark.

Run from the project root:
    spark-submit --master "local[4]" --driver-memory 2g \
      --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 spark/lab_iceberg.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def section(title):
    print(f"\n===== {title} =====", flush=True)


def snapshots(table):
    spark.sql(f"SELECT snapshot_id, parent_id, operation, committed_at FROM {table}.snapshots ORDER BY committed_at").show(truncate=False)


os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[4]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(spark, "| Spark", spark.version)

section("Verify the catalog")
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW TABLES IN lab.silver").show()

section("Query the table through the catalog")
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

section("Explore Iceberg metadata")
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)
print("snapshots:", spark.sql("SELECT count(*) FROM lab.silver.orders.snapshots").first()[0],
      "| data files:", spark.sql("SELECT count(*) FROM lab.silver.orders.files").first()[0])

section("Create a second snapshot")
stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
snapshots("lab.silver.orders")
spark.sql("SELECT count(*) FROM lab.silver.orders").show()
spark.sql("SELECT file_path, record_count FROM lab.silver.orders.files").show(truncate=False)

section("Time travel")
snaps = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
print([s["snapshot_id"] for s in snaps])
first_snapshot = snaps[0]["snapshot_id"]
before_append = snaps[-2]["snapshot_id"]
print("VERSION AS OF first snapshot", first_snapshot)
spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()
print("VERSION AS OF snapshot before the append", before_append)
spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {before_append}").show()
print("current")
spark.sql("SELECT count(*) FROM lab.silver.orders").show()

section("Schema evolution")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
snapshots("lab.silver.orders")
spark.sql("SELECT source, count(*) FROM lab.silver.orders GROUP BY source").show()
spark.sql("SELECT file_path, record_count FROM lab.silver.orders.files").show(truncate=False)
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor FROM lab.silver.orders.history").show(truncate=False)

section("Compare Iceberg with Parquet")
parquet = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("Parquet directory files:", len(parquet.inputFiles()), "| rows:", parquet.count(), "| no snapshot, no history")
print("Iceberg table data files:", spark.sql("SELECT count(*) FROM lab.silver.orders.files").first()[0],
      "| snapshots:", spark.sql("SELECT count(*) FROM lab.silver.orders.snapshots").first()[0])

section("Exercise 1: lab.silver.users as an Iceberg table")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
users = spark.table("lab.silver.users") if spark.catalog.tableExists("lab.silver.users") else \
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
users.writeTo("lab.silver.users").createOrReplace()
print("users:", spark.table("lab.silver.users").count())
snapshots("lab.silver.users")

section("Exercise 2: append a new batch of users")
batch = spark.table("lab.silver.users").limit(5).withColumn("user_id", F.expr("uuid()")) \
    .withColumn("username", F.concat(F.col("username"), F.lit("_new")))
batch.writeTo("lab.silver.users").append()
snapshots("lab.silver.users")

section("Exercise 3: time travel on users")
usnaps = spark.sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()
before = usnaps[-2]["snapshot_id"]
print("before the append:", spark.sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {before}").first()[0],
      "| after the append:", spark.table("lab.silver.users").count())

section("Exercise 4: add a country column")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.sql("""
    INSERT INTO lab.silver.users
    SELECT uuid(), concat(username, '_us'), name, sex, email, birthdate, street, city, state, zip_code, 'US'
    FROM lab.silver.users LIMIT 2
""")
spark.sql("DESCRIBE lab.silver.users").show()
spark.sql("SELECT country, count(*) FROM lab.silver.users GROUP BY country").show()
snapshots("lab.silver.users")

section("Exercise 5: S3 layout of users and orders")
for t in ("users", "orders"):
    files = spark.sql(f"SELECT count(*) FROM lab.silver.{t}.files").first()[0]
    manifests = spark.sql(f"SELECT count(*) FROM lab.silver.{t}.manifests").first()[0]
    snaps_n = spark.sql(f"SELECT count(*) FROM lab.silver.{t}.snapshots").first()[0]
    print(f"{t}: {snaps_n} snapshots, {manifests} manifests in the current snapshot, {files} data files")

spark.stop()

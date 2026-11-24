import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
REGION = os.getenv("AWS_REGION", "ap-southeast-2")
DELTA_PATH = os.getenv("BATCH_DELTA_PATH")
QUARANTINE_PATH = os.getenv("BATCH_QUARANTINE_PATH")
MAX_INVALID_RATE = 0.05

if len(sys.argv) != 2 or not sys.argv[1].strip():
    raise ValueError("Pass exactly one S3 input URI (e.g., s3a://bucket/raw/github_events/...json)")

# Normalize s3:// to s3a:// if needed
INPUT_PATH = sys.argv[1].strip()
if INPUT_PATH.startswith("s3://"):
    INPUT_PATH = "s3a://" + INPUT_PATH[len("s3://"):]

# SPARK CONFIGURATION
builder = (
    SparkSession.builder
    .appName("GitHubEventsToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.hadoop:hadoop-aws:3.4.1"],
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

try:
    # 1. Read only the newly uploaded file
    raw_df = (
        spark.read
        .option("multiLine", "true")
        .json(INPUT_PATH)
    )

    if raw_df.isEmpty():
        print("No records in input file.")
    else:
        # Project & cache so S3 JSON is only read once across counts and writes
        events = raw_df.select(
            col("id").cast("string").alias("event_id"),
            col("type").alias("event_type"),
            col("repo.name").alias("repository"),
            col("created_at").cast("timestamp").alias("created_at"),
        ).cache()

        total = events.count()

        # 2. Data quality validation
        invalid_condition = (
            col("event_id").isNull()
            | col("event_type").isNull()
            | col("repository").isNull()
            | col("created_at").isNull()
        )

        invalid_df = events.filter(invalid_condition)
        valid_df = events.filter(~invalid_condition)

        invalid_count = invalid_df.count()
        valid_count = valid_df.count()

        print(f"Total: {total} | Valid: {valid_count} | Invalid: {invalid_count}")

        # 3. Quarantine invalid records
        if invalid_count > 0:
            invalid_df.write.mode("append").json(QUARANTINE_PATH)

        invalid_rate = invalid_count / total
        if invalid_rate > MAX_INVALID_RATE:
            raise ValueError(
                f"Invalid record rate {invalid_rate:.2%} "
                f"exceeds allowed {MAX_INVALID_RATE:.2%}"
            )

        # 4. Deduplicate & merge into Delta Lake
        valid_df = valid_df.dropDuplicates(["event_id"])

        if not valid_df.isEmpty():
            if DeltaTable.isDeltaTable(spark, DELTA_PATH):
                delta_table = DeltaTable.forPath(spark, DELTA_PATH)
                (
                    delta_table.alias("target")
                    .merge(
                        valid_df.alias("source"),
                        "target.event_id = source.event_id",
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    valid_df.write
                    .format("delta")
                    .mode("overwrite")
                    .save(DELTA_PATH)
                )

        events.unpersist()

        if DeltaTable.isDeltaTable(spark, DELTA_PATH):
            final_count = spark.read.format("delta").load(DELTA_PATH).count()
            print("Final Delta records:", final_count)

finally:
    spark.stop()
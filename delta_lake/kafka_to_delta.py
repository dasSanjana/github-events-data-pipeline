
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
java_17_home = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
if os.path.exists(java_17_home):
    os.environ["JAVA_HOME"] = java_17_home
    os.environ["PATH"] = f"{java_17_home}/bin:" + os.environ.get("PATH", "")




BUCKET = os.getenv("AWS_BUCKET_NAME")
if not BUCKET:
    raise ValueError("AWS_BUCKET_NAME is not set in environment or .env file")

REGION = os.getenv("AWS_REGION", "ap-southeast-2")

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")

DELTA_PATH = os.getenv(
    "STREAMING_DELTA_PATH",
    f"s3a://{BUCKET}/curated/delta/github_events_streaming"
)
QUARANTINE_PATH = os.getenv(
    "STREAMING_QUARANTINE_PATH",
    f"s3a://{BUCKET}/quarantine/github_events_streaming"
)

# helps resolving dynamically relative to the project root if CHECKPOINT_PATH isn't set
DEFAULT_CHECKPOINT = str(Path(__file__).resolve().parent.parent / "checkpoints" / "github_events_streaming")
CHECKPOINT_PATH = os.getenv("STREAMING_CHECKPOINT_PATH", DEFAULT_CHECKPOINT)
builder = (
    SparkSession.builder
    .appName("KafkaToDeltaStreaming")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{REGION}.amazonaws.com"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        "org.apache.hadoop:hadoop-aws:3.4.1",
    ],
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# GitHub event schema
schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField(
        "repo",
        StructType([
            StructField("name", StringType())
        ])
    ),
    StructField("created_at", TimestampType())
])

# 1. Read Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# 2. Parse JSON
events = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("event")
    )
    .select(
        col("event.id").alias("event_id"),
        col("event.type").alias("event_type"),
        col("event.repo.name").alias("repository"),
        col("event.created_at").alias("created_at")
    )
)

# 3. Process each micro-batch
def process_batch(batch_df, batch_id):

    invalid_condition = (
        col("event_id").isNull()
        | col("event_type").isNull()
        | col("repository").isNull()
        | col("created_at").isNull()
    )

    valid_df = (
        batch_df
        .filter(~invalid_condition)
        .dropDuplicates(["event_id"])
    )

    invalid_df = batch_df.filter(invalid_condition)

    if not invalid_df.isEmpty():
        (
            invalid_df.write
            .mode("append")
            .json(
                f"s3a://{BUCKET}/quarantine/"
                "github_events_streaming"
            )
        )

    if valid_df.isEmpty():
        return

    # 4. Idempotent Delta MERGE
    if DeltaTable.isDeltaTable(spark, DELTA_PATH):

        delta_table = DeltaTable.forPath(
            spark, DELTA_PATH
        )

        (
            delta_table.alias("target")
            .merge(
                valid_df.alias("source"),
                "target.event_id = source.event_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        (
            valid_df.write
            .format("delta")
            .mode("append")
            .save(DELTA_PATH)
        )

    print(
        f"Batch {batch_id}: "
        f"{valid_df.count()} valid events processed"
    )

# 5. Start streaming
query = (
    events.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaSparkPipeline")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
    )
    .getOrCreate()
)

# data = [
#     (101, "Rahul", 24, "Bengaluru"),
#     (102, "Priya", 22, "Mumbai"),
#     (103, "Amit", 28, "Delhi"),
#     (104, "Neha", 25, "Bengaluru")
# ]
# columns = ["user_id", "name", "age", "city"]
# df = spark.createDataFrame(data,columns)
# df.show()
# df.printSchema()
# df.select("name", "city").show()
# df.filter(df.age > 23).show()
# df.orderBy("age").show()
# from pyspark.sql.functions import col

# df.withColumn("age_next_year", col("age") + 1).show()
# df.withColumnRenamed("name", "user_name").show()

# df.createOrReplaceTempView("customers")
# result = spark.sql("""
#     SELECT city, COUNT(*) AS total_customers
#     FROM customers
#     GROUP BY city
# """)

# result.show()


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "github-events")
    .option("startingOffsets", "earliest")
    .load()
)

events = df.selectExpr(
    "CAST(key AS STRING) AS event_id",
    "CAST(value AS STRING) AS event"
)

query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/github-checkpoint")
    .start()
)

query.awaitTermination()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "events")
POSTGRES_URL = "jdbc:postgresql://{host}:5432/{db}".format(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    db=os.getenv("POSTGRES_DB", "metrics"),
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "app")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "app")

spark = (
    SparkSession.builder.appName("real-time-streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .load()
)

# adapt schema & parsing to your dataset
from pyspark.sql.functions import from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("ingest_ts", StringType()),
])

parsed = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json("json_str", schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

agg = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds")
    )
    .agg(
        avg("value").alias("avg_value"),
        count("*").alias("count_events"),
    )
)

def write_to_postgres(batch_df, batch_id):
  (
      batch_df
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .drop("window")
      .write
      .format("jdbc")
      .option("url", POSTGRES_URL)
      .option("dbtable", "public.metrics_windowed")
      .option("user", POSTGRES_USER)
      .option("password", POSTGRES_PASSWORD)
      .mode("append")
      .save()
  )

query = (
    agg.writeStream
    .outputMode("update")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/checkpoints")
    .start()
)

query.awaitTermination()

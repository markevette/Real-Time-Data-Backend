import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "tax_events")

POSTGRES_URL = "jdbc:postgresql://{host}:5432/{db}".format(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    db=os.getenv("POSTGRES_DB", "metrics"),
)

spark = (
    SparkSession.builder.appName("taxpayer-streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("steuer_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("taxable_income", DoubleType()),
    StructField("tax_class", StringType()),
    StructField("solidarity_tax", DoubleType()),
    StructField("church_tax", DoubleType()),
    StructField("ingest_ts", StringType()),
])

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .load()
)

from pyspark.sql.functions import from_json

parsed = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json("json_str", schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

agg = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
    .agg(
        avg("taxable_income").alias("avg_income"),
        count("*").alias("filings_count"),
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
        .option("dbtable", "public.taxpayer_metrics")
        .option("user", os.getenv("POSTGRES_USER", "app"))
        .option("password", os.getenv("POSTGRES_PASSWORD", "app"))
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

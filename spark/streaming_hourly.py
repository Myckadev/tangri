from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
)
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "weather.raw.openmeteo")
TOPIC_HOURLY = os.getenv("TOPIC_HOURLY", "weather.hourly.flattened")
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/datalake")
CHECKPOINT = os.getenv("STREAM_CHECKPOINT", f"{HDFS_BASE}/checkpoints/hourly")

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("weather-streaming-hourly")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_kafka(spark: SparkSession) -> DataFrame:
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .load()
    )
    return df.select(F.col("value").cast("string").alias("value"))

def normalize(df_str: DataFrame) -> DataFrame:
    # Extract needed fields with get_json_object, then cast/parse
    base = df_str.select(
        F.get_json_object("value", '$._meta.city_id').alias("city_id"),
        F.get_json_object("value", '$._meta.city_name').alias("city_name"),
        F.get_json_object("value", '$._meta.country').alias("country"),
        F.get_json_object("value", '$._meta.ingested_at').alias("ingested_at"),
        F.get_json_object("value", '$.raw.hourly.time').alias("times"),
        F.get_json_object("value", '$.raw.hourly.temperature_2m').alias("temps"),
        F.get_json_object("value", '$.raw.hourly.relative_humidity_2m').alias("humidity"),
        F.get_json_object("value", '$.raw.hourly.wind_speed_10m').alias("wind"),
    )

    to_arr_str = lambda c: F.from_json(c, ArrayType(StringType()))
    to_arr_dbl = lambda c: F.from_json(c, ArrayType(DoubleType()))

    arr = base.select(
        "city_id", "city_name", "country", "ingested_at",
        to_arr_str("times").alias("times"),
        to_arr_dbl("temps").alias("temps"),
        to_arr_dbl("humidity").alias("humidity"),
        to_arr_dbl("wind").alias("wind"),
    )

    # zip arrays into struct per index
    zipped = arr.select(
        "city_id", "city_name", "country", "ingested_at",
        F.arrays_zip("times", "temps", "humidity", "wind").alias("z")
    ).selectExpr("city_id", "city_name", "country", "ingested_at", "explode(z) as e")

    out = zipped.select(
        "city_id", "city_name", "country",
        F.to_timestamp(F.col("e.times")).alias("hour_ts"),
        F.col("e.temps").cast("double").alias("temperature_2m"),
        F.col("e.humidity").cast("double").alias("relative_humidity_2m"),
        F.col("e.wind").cast("double").alias("wind_speed_10m"),
    ).withColumn("date", F.to_date("hour_ts"))

    return out

def write_hdfs(df: DataFrame):
    return (
        df.writeStream
        .format("parquet")
        .option("path", f"{HDFS_BASE}/bronze/hourly")
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
    )

def to_kafka_sink(df: DataFrame):
    # serialize to JSON for Kafka
    j = F.to_json(F.struct(
        "city_id", "city_name", "country",
        F.date_format("hour_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("hour_ts"),
        "temperature_2m", "relative_humidity_2m", "wind_speed_10m"
    ))
    payload = df.select(
        F.concat_ws("|", "city_id", F.date_format("hour_ts", "yyyyMMddHH")).alias("key"),
        j.alias("value")
    )
    return (
        payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", TOPIC_HOURLY)
        .option("checkpointLocation", f"{CHECKPOINT}_kafka")
        .outputMode("append")
    )

if __name__ == "__main__":
    spark = build_spark()
    src = read_kafka(spark)
    norm = normalize(src)
    # Sinks: HDFS & Kafka
    q1 = write_hdfs(norm).start()
    q2 = to_kafka_sink(norm).start()
    q1.awaitTermination()
    q2.awaitTermination()

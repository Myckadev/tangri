from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "weather.raw.openmeteo")
TOPIC_HOURLY = os.getenv("TOPIC_HOURLY", "weather.hourly.flattened")
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/datalake")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("weather-streaming-hourly")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .load()
    )


def normalize(df: DataFrame) -> DataFrame:
    # value is bytes -> string -> json
    df_str = df.select(F.col("key").cast("string"), F.col("value").cast("string").alias("value"))

    # We expect envelope { _meta: {...}, raw: {...} }
    # Extract only a few useful hourly fields
    meta_schema = StructType([
        StructField("source", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("city_id", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("country", StringType(), True),
    ])

    # Parse JSON
    parsed = df_str.select(F.from_json("value", StructType([
        StructField("_meta", meta_schema, True),
        StructField("raw", F.MapType(StringType(), F.StringType(), True), True)
    ])).alias("data")).select("data.*")

    # Extract hourly arrays using get_json_object for simplicity
    raw_json = df_str.select(F.col("value").alias("raw_json"))
    # Better approach: re-parse with schema, but open-meteo schema can vary. We'll use JSON functions.
    v = F.col("value")
    j = F.from_json(v, StringType())  # noop placeholder to keep API consistent

    # Re-parse with a permissive generic map
    parsed_generic = df_str.select(
        F.from_json("value", "map<string,string>").alias("m")
    )

    # For robustness, we extract needed fields via get_json_object
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

    # Convert JSON arrays to arrays of strings/doubles
    def to_array(col):
        return F.from_json(col, ArrayType(StringType()))
    def to_array_d(col):
        return F.from_json(col, ArrayType(DoubleType()))

    arr = base.select(
        "city_id", "city_name", "country", "ingested_at",
        to_array("times").alias("times"),
        to_array_d("temps").alias("temps"),
        to_array_d("humidity").alias("humidity"),
        to_array_d("wind").alias("wind"),
    )

    # Explode arrays to rows
    exploded = arr.select(
        "city_id", "city_name", "country", "ingested_at",
        F.posexplode("times").alias("idx", "time"),
        "temps", "humidity", "wind"
    ).select(
        "city_id", "city_name", "country", "ingested_at", "time",
        F.col("temps")[F.col("idx")].alias("temperature_2m"),
        F.col("humidity")[F.col("idx")].alias("relative_humidity_2m"),
        F.col("wind")[F.col("idx")].alias("wind_speed_10m"),
    )

    # Derive date partitions
    normalized = exploded.withColumn("date", F.to_date("time")).withColumn("hour", F.date_format("time", "HH"))

    return normalized


def write_hdfs(df: DataFrame):
    out_path = f"{HDFS_BASE}/silver/weather/hourly"
    return (
        df.writeStream
        .format("parquet")
        .option("path", out_path)
        .option("checkpointLocation", f"{HDFS_BASE}/_chk/weather_hourly_ckpt")
        .partitionBy("date", "city_id")
        .outputMode("append")
    )


def to_kafka_sink(df: DataFrame):
    # Serialize as JSON
    as_json = df.select(
        F.to_json(F.struct(*df.columns)).alias("value")
    )
    return (
        as_json.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", TOPIC_HOURLY)
        .option("checkpointLocation", f"{HDFS_BASE}/_chk/weather_hourly_kafka_ckpt")
        .outputMode("append")
    )


if __name__ == "__main__":
    spark = build_spark()
    src = read_kafka(spark)
    norm = normalize(src)

    # Two sinks: HDFS and Kafka
    hdfs_q = write_hdfs(norm).start()
    kafka_q = to_kafka_sink(norm).start()

    hdfs_q.awaitTermination()
    kafka_q.awaitTermination() 
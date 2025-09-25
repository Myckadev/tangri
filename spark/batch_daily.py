from pyspark.sql import SparkSession, functions as F
import os

HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/datalake")

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("batch-daily-agg")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Lire bronze/hourly
    src_path = f"{HDFS_BASE}/bronze/hourly"
    df = spark.read.parquet(src_path)

    # Sanity: colonnes attendues
    required = {"city_id","city_name","country","hour_ts","temperature_2m","relative_humidity_2m","wind_speed_10m","date"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"missing columns in bronze/hourly: {missing}")

    # Agrégats journaliers
    daily = (
        df.groupBy("city_id", "city_name", "country", "date")
          .agg(
              F.avg("temperature_2m").alias("avg_temperature_2m"),
              F.min("temperature_2m").alias("min_temperature_2m"),
              F.max("temperature_2m").alias("max_temperature_2m"),
              F.avg("relative_humidity_2m").alias("avg_relative_humidity_2m"),
              F.avg("wind_speed_10m").alias("avg_wind_speed_10m"),
          )
    )

    out_path = f"{HDFS_BASE}/gold/daily_weather"
    (
      daily
      .repartition(1)
      .write
      .mode("overwrite")
      .partitionBy("date")
      .parquet(out_path)
    )

    print(f"Wrote daily aggregates to {out_path}")
    spark.stop()

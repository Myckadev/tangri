import os
import json
import argparse
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/datalake")


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_hourly(spark: SparkSession) -> DataFrame:
    path = f"{HDFS_BASE}/silver/weather/hourly"
    return spark.read.parquet(path)


def apply_filters(df: DataFrame, city_ids: List[str], date_from: str | None, date_to: str | None) -> DataFrame:
    if city_ids:
        df = df.where(F.col("city_id").isin(city_ids))
    if date_from:
        df = df.where(F.col("date") >= F.lit(date_from))
    if date_to:
        df = df.where(F.col("date") <= F.lit(date_to))
    return df


def aggregate(df: DataFrame, metrics: List[str], agg: str | None) -> DataFrame:
    if not metrics:
        return df
    agg = (agg or "avg").lower()
    group_cols = ["city_id", "city_name", "country", "date"]
    grouped = df.groupBy(*group_cols)
    agg_exprs = []
    for m in metrics:
        col = F.col(m)
        if agg == "avg":
            agg_exprs.append(F.avg(col).alias(f"{m}_{agg}"))
        elif agg == "min":
            agg_exprs.append(F.min(col).alias(f"{m}_{agg}"))
        elif agg == "max":
            agg_exprs.append(F.max(col).alias(f"{m}_{agg}"))
        else:
            agg_exprs.append(F.avg(col).alias(f"{m}_avg"))
    return grouped.agg(*agg_exprs)


def write_result(df: DataFrame, request_id: str) -> str:
    out_dir = f"{HDFS_BASE}/gold/queries/{request_id}"
    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(out_dir)
    )
    return out_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request_id", required=True)
    parser.add_argument("--city_ids", default="")
    parser.add_argument("--date_from", default=None)
    parser.add_argument("--date_to", default=None)
    parser.add_argument("--metrics", default="temperature_2m")
    parser.add_argument("--agg", default="avg")
    args = parser.parse_args()

    city_ids = [c for c in (args.city_ids or "").split(",") if c]
    metrics = [m for m in (args.metrics or "").split(",") if m]

    spark = build_spark(f"weather-query-{args.request_id}")
    try:
        df = load_hourly(spark)
        df = apply_filters(df, city_ids, args.date_from, args.date_to)
        if metrics:
            df = aggregate(df, metrics, args.agg)
        out_path = write_result(df, args.request_id)
        result = {
            "request_id": args.request_id,
            "status": "done",
            "result_path": out_path,
            "rows": df.count(),
            "ts": datetime.utcnow().isoformat(),
        }
        print(json.dumps(result), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 
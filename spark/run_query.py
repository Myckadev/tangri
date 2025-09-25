# spark/run_query.py
import argparse
import os
from pyspark.sql import SparkSession, functions as F

def parse_args():
    p = argparse.ArgumentParser(description="Run parametric weather query and write Parquet to HDFS")
    p.add_argument("--request_id", required=True, help="ID de la requête (dossier de sortie)")
    p.add_argument("--city_ids", default="", help="Liste de city_ids séparés par des virgules")
    p.add_argument("--date_from", default="", help="YYYY-MM-DD (inclusif) ou vide")
    p.add_argument("--date_to", default="", help="YYYY-MM-DD (inclusif) ou vide")
    p.add_argument("--metrics", default="temperature_2m", help="Liste de métriques séparées par des virgules")
    p.add_argument("--agg", default="avg", choices=["avg","min","max"], help="Agrégation")
    return p.parse_args()

def main():
    args = parse_args()

    HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/datalake")
    src_path  = f"{HDFS_BASE}/bronze/hourly"
    out_dir   = f"{HDFS_BASE}/gold/queries/{args.request_id}"

    spark = (SparkSession.builder
             .appName(f"run-query-{args.request_id}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(src_path)

    # Filtre ville(s)
    city_ids = [c.strip() for c in args.city_ids.split(",") if c.strip()]
    if city_ids:
        df = df.where(F.col("city_id").isin(city_ids))

    # Filtre dates
    if args.date_from:
        df = df.where(F.col("date") >= F.lit(args.date_from))
    if args.date_to:
        df = df.where(F.col("date") <= F.lit(args.date_to))

    # Colonnes/métriques
    metrics = [m.strip() for m in args.metrics.split(",") if m.strip()]
    valid_metrics = {
        "temperature_2m": "double",
        "relative_humidity_2m": "double",
        "wind_speed_10m": "double",
    }
    metrics = [m for m in metrics if m in valid_metrics]
    if not metrics:
        raise ValueError("Aucune métrique valide demandée.")

    # Agrégation
    agg_fn = {"avg": F.avg, "min": F.min, "max": F.max}[args.agg]

    # Agrégats par (city_id, date) pour chaque métrique demandée
    exprs = [agg_fn(F.col(m)).alias(f"{args.agg}_{m}") for m in metrics]
    out = (
        df.groupBy("city_id", "city_name", "country", "date")
          .agg(*exprs)
          .orderBy("city_id", "date")
    )

    # Écriture Parquet (un seul dossier par request_id)
    (out
        .repartition(1)
        .write.mode("overwrite")
        .parquet(out_dir)
    )

    print(f"[run_query] wrote {out_dir}")
    spark.stop()

if __name__ == "__main__":
    main()

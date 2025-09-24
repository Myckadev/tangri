from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("hello-batch")
             .getOrCreate())
    print("hello from Spark batch job")
    df = spark.createDataFrame([("batch", 1), ("hello", 2)], ["what", "id"])
    df.show(truncate=False)
    print("hello-batch done (stub).")
    spark.stop()

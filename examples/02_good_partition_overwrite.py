from pyspark.sql import SparkSession

INPUT_PATH = "data/input/orders.csv"
OUTPUT_PATH = "output/partition_overwrite"
TARGET_DATE = "2025-01-15"


def main():
    spark = SparkSession.builder \
        .appName("GoodExample-PartitionOverwrite") \
        .master("local[*]") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read source data
    orders = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(INPUT_PATH)

    # Filter for one day
    daily_orders = orders.filter(orders.order_date == TARGET_DATE)
    print(f"\nRead {daily_orders.count()} orders for {TARGET_DATE}")

    # Overwrite only the affected partition
    daily_orders.write \
        .mode("overwrite") \
        .partitionBy("order_date") \
        .parquet(OUTPUT_PATH)

    # Check total rows
    result = spark.read.parquet(OUTPUT_PATH)
    print(f"Total rows in output: {result.count()}")
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

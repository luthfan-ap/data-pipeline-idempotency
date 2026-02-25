from pyspark.sql import SparkSession

INPUT_PATH = "data/input/orders.csv"
OUTPUT_PATH = "output/bad_append"
TARGET_DATE = "2025-01-15"


def main():
    spark = SparkSession.builder \
        .appName("BadExample-BlindAppend") \
        .master("local[*]") \
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

    # Append to output — this is the problem!
    daily_orders.write \
        .mode("append") \
        .parquet(OUTPUT_PATH)

    # Check total rows
    result = spark.read.parquet(OUTPUT_PATH)
    print(f"Total rows in output: {result.count()}")
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

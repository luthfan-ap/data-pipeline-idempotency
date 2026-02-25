import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

INPUT_PATH = "data/input/orders.csv"
INPUT_PATH_REDELIVERY = "data/input/orders_redelivery.csv"
OUTPUT_PATH = "output/delta_orders"


def main():
    builder = SparkSession.builder \
        .appName("GoodExample-DeltaMerge") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Pick input file based on the flag
    use_redelivery = "--redelivery" in sys.argv
    input_path = INPUT_PATH_REDELIVERY if use_redelivery else INPUT_PATH
    print(f"\nLoading data from: {input_path}")

    # Read source data
    incoming = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)

    print(f"Read {incoming.count()} incoming records")

    # Absolute path needed for LOCATION
    abs_output = os.path.abspath(OUTPUT_PATH)

    # MERGE into the Delta table (or create it on first run)
    if os.path.exists(OUTPUT_PATH):
        incoming.createOrReplaceTempView("source")

        # Register the Delta path as a named table so SQL MERGE works
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS orders
            USING DELTA
            LOCATION '{abs_output}'
        """)

        spark.sql("""
            MERGE INTO orders AS target
            USING source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        print("MERGE complete")
    else:
        incoming.write.format("delta").mode("overwrite").save(OUTPUT_PATH)
        print("Delta table created (first run)")

    # Check total rows
    result = spark.read.format("delta").load(OUTPUT_PATH)
    print(f"Total rows in Delta table: {result.count()}")
    result.orderBy("order_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()


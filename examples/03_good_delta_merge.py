import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

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

    # MERGE into the Delta table (or create it on first run)
    if DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
        delta_table = DeltaTable.forPath(spark, OUTPUT_PATH)

        delta_table.alias("target") \
            .merge(
                incoming.alias("source"),
                "target.order_id = source.order_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

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

from pyspark.sql import functions as F
from delta.tables import DeltaTable  # not used yet but useful later
from spark_utils import get_spark
import config


def ingest_customers_bronze(spark):
    df_customers = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(config.CUSTOMERS_CSV_PATH))
    )

    df_customers = (
        df_customers
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.lit("customers.csv"))
    )

    (
        df_customers
        .write
        .format("delta")
        .mode("overwrite")
        .save(str(config.BRONZE_CUSTOMERS_PATH))
    )
    print("Bronze customers written to:", config.BRONZE_CUSTOMERS_PATH)


def ingest_orders_bronze(spark):
    df_orders = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(config.ORDERS_CSV_PATH))
    )

    df_orders = (
        df_orders
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.lit("orders.csv"))
    )

    (
        df_orders
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")
        .save(str(config.BRONZE_ORDERS_PATH))
    )
    print("Bronze orders written to:", config.BRONZE_ORDERS_PATH)


if __name__ == "__main__":
    spark = get_spark("bronze-ingest")
    ingest_customers_bronze(spark)
    ingest_orders_bronze(spark)
    spark.stop()

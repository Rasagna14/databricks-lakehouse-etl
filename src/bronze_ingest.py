from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from spark_utils import get_spark
import config

# Define explicit schemas instead of inferSchema

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("amount", DoubleType(), True)
])


def ingest_customers_bronze(spark):
    df_customers = (
        spark.read
        .schema(customer_schema)
        .option("header", "true")
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
        .schema(orders_schema)
        .option("header", "true")
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

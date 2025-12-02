from pyspark.sql import functions as F
from spark_utils import get_spark
import config


def build_silver_customers(spark):
    df_bronze_customers = (
        spark.read
        .format("delta")
        .load(str(config.BRONZE_CUSTOMERS_PATH))
    )

    df_silver_customers = (
        df_bronze_customers
        .dropDuplicates(["customer_id"])
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    )

    (
        df_silver_customers
        .write
        .format("delta")
        .mode("overwrite")
        .save(str(config.SILVER_CUSTOMERS_PATH))
    )
    print("Silver customers written to:", config.SILVER_CUSTOMERS_PATH)


def build_silver_orders(spark):
    df_bronze_orders = (
        spark.read
        .format("delta")
        .load(str(config.BRONZE_ORDERS_PATH))
    )

    # simple FX mapping so you can talk about "currency normalization"
    fx_rates = {
        "USD": 1.0,
        "INR": 0.012,
        "EUR": 1.1,
    }

    fx_mapping_expr = F.create_map(
        [F.lit(x) for pair in fx_rates.items() for x in pair]
    )

    df_silver_orders = (
        df_bronze_orders
        .withColumn("amount_usd", F.col("amount") * fx_mapping_expr[F.col("currency")])
        .withColumn("status", F.upper(F.col("status")))
    )

    (
        df_silver_orders
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")
        .save(str(config.SILVER_ORDERS_PATH))
    )
    print("Silver orders written to:", config.SILVER_ORDERS_PATH)


if __name__ == "__main__":
    spark = get_spark("silver-transform")
    build_silver_customers(spark)
    build_silver_orders(spark)
    spark.stop()

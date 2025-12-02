from pyspark.sql import functions as F
from spark_utils import get_spark
import config


def build_daily_order_metrics(spark):
    df_orders = (
        spark.read
        .format("delta")
        .load(str(config.SILVER_ORDERS_PATH))
    )

    df_customers = (
        spark.read
        .format("delta")
        .load(str(config.SILVER_CUSTOMERS_PATH))
    )

    df_joined = (
        df_orders.alias("o")
        .join(
            df_customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
    )

    df_metrics = (
        df_joined
        .groupBy("order_date", "country")
        .agg(
            F.countDistinct("o.customer_id").alias("active_customers"),
            F.count("order_id").alias("total_orders"),
            F.sum("amount_usd").alias("total_revenue_usd"),
            F.avg("amount_usd").alias("avg_order_value_usd"),
        )
        .orderBy("order_date", "country")
    )

    (
        df_metrics
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")
        .save(str(config.GOLD_ORDERS_METRICS_PATH))
    )
    print("Gold metrics written to:", config.GOLD_ORDERS_METRICS_PATH)


if __name__ == "__main__":
    spark = get_spark("gold-aggregates")
    build_daily_order_metrics(spark)
    spark.stop()

from pyspark.sql import functions as F
from spark_utils import get_spark
import config


def check_not_null(df, column_name: str):
    null_count = df.filter(F.col(column_name).isNull()).count()
    if null_count > 0:
        raise ValueError(f"Data quality check failed: {null_count} NULLs in {column_name}")
    print(f"[OK] {column_name} has no NULLs")


def run_data_quality_checks(spark):
    df_customers = (
        spark.read
        .format("delta")
        .load(str(config.SILVER_CUSTOMERS_PATH))
    )
    df_orders = (
        spark.read
        .format("delta")
        .load(str(config.SILVER_ORDERS_PATH))
    )

    check_not_null(df_customers, "customer_id")
    check_not_null(df_orders, "order_id")
    check_not_null(df_orders, "order_ts")

    customer_count = df_customers.count()
    order_count = df_orders.count()
    print(f"[INFO] Customers: {customer_count}, Orders: {order_count}")


if __name__ == "__main__":
    spark = get_spark("dq-checks")
    run_data_quality_checks(spark)
    spark.stop()

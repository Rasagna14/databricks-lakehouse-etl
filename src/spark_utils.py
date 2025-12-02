from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse-etl"):
    """
    Create a SparkSession with Delta support.
    When you run this on Databricks, most of this
    config can be omitted because it's set by default.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

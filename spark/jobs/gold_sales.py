import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    round,
    sum,
    avg,
    max,
    min,
)

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    "s3://iceberg-retail-bucket/warehouse/"
)
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "retail")
ICEBERG_DATABASE = os.getenv("ICEBERG_DATABASE", "retail")

SILVER_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.silver_sales"

GOLD_DAILY_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.gold_daily_sales"
GOLD_PRODUCT_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.gold_product_sales"
GOLD_CATEGORY_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.gold_category_sales"
GOLD_CITY_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.gold_city_sales"


spark = (
    SparkSession.builder
    .appName("Retail-Gold-Sales")
    .config(
        f"spark.sql.catalog.{ICEBERG_CATALOG}",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config(
        f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
        ICEBERG_WAREHOUSE
    )
    .config(
        f"spark.sql.catalog.{ICEBERG_CATALOG}.glue.region",
        AWS_REGION
    )
    .config(
        "spark.sql.defaultCatalog",
        ICEBERG_CATALOG
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("READING SILVER TABLE")
print("=" * 60)

silver_df = spark.table(SILVER_TABLE)

print(f"Silver table: {SILVER_TABLE}")
print(f"Silver record count: {silver_df.count()}")

silver_df.printSchema()

print("=" * 60)
print("CREATING GOLD DAILY SALES")
print("=" * 60)

daily_sales = (
    silver_df
    .groupBy("transaction_date")
    .agg(
        count("*").alias("transaction_count"),
        countDistinct("transaction_id").alias("unique_transactions"),
        sum("quantity").alias("total_quantity"),
        round(sum("total_amount"), 2).alias("total_sales"),
        round(avg("total_amount"), 2).alias("average_transaction_value"),
        round(min("total_amount"), 2).alias("minimum_transaction_value"),
        round(max("total_amount"), 2).alias("maximum_transaction_value"),
    )
    .orderBy("transaction_date")
)

daily_sales.writeTo(GOLD_DAILY_TABLE) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .partitionedBy("transaction_date") \
    .createOrReplace()

print(f"Created: {GOLD_DAILY_TABLE}")

print("=" * 60)
print("CREATING GOLD PRODUCT SALES")
print("=" * 60)

product_sales = (
    silver_df
    .groupBy("product", "category")
    .agg(
        count("*").alias("transaction_count"),
        countDistinct("transaction_id").alias("unique_transactions"),
        sum("quantity").alias("total_quantity"),
        round(sum("total_amount"), 2).alias("total_sales"),
        round(avg("total_amount"), 2).alias("average_transaction_value"),
    )
    .orderBy("category", "product")
)

product_sales.writeTo(GOLD_PRODUCT_TABLE) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print(f"Created: {GOLD_PRODUCT_TABLE}")

print("=" * 60)
print("CREATING GOLD CATEGORY SALES")
print("=" * 60)

category_sales = (
    silver_df
    .groupBy("category")
    .agg(
        count("*").alias("transaction_count"),
        countDistinct("transaction_id").alias("unique_transactions"),
        sum("quantity").alias("total_quantity"),
        round(sum("total_amount"), 2).alias("total_sales"),
        round(avg("total_amount"), 2).alias("average_transaction_value"),
    )
    .orderBy(col("total_sales").desc())
)

category_sales.writeTo(GOLD_CATEGORY_TABLE) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print(f"Created: {GOLD_CATEGORY_TABLE}")

print("=" * 60)
print("CREATING GOLD CITY SALES")
print("=" * 60)

city_sales = (
    silver_df
    .groupBy("customer_city")
    .agg(
        count("*").alias("transaction_count"),
        countDistinct("transaction_id").alias("unique_transactions"),
        sum("quantity").alias("total_quantity"),
        round(sum("total_amount"), 2).alias("total_sales"),
        round(avg("total_amount"), 2).alias("average_transaction_value"),
    )
    .orderBy(col("total_sales").desc())
)

city_sales.writeTo(GOLD_CITY_TABLE) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print(f"Created: {GOLD_CITY_TABLE}")

print("=" * 60)
print("GOLD TABLE COUNTS")
print("=" * 60)

print(f"{GOLD_DAILY_TABLE}: {spark.table(GOLD_DAILY_TABLE).count()}")
print(f"{GOLD_PRODUCT_TABLE}: {spark.table(GOLD_PRODUCT_TABLE).count()}")
print(f"{GOLD_CATEGORY_TABLE}: {spark.table(GOLD_CATEGORY_TABLE).count()}")
print(f"{GOLD_CITY_TABLE}: {spark.table(GOLD_CITY_TABLE).count()}")


print("=" * 60)
print("GOLD DAILY SALES")
print("=" * 60)

spark.table(GOLD_DAILY_TABLE).show(10, truncate=False)


print("=" * 60)
print("GOLD PRODUCT SALES")
print("=" * 60)

spark.table(GOLD_PRODUCT_TABLE).show(10, truncate=False)


print("=" * 60)
print("GOLD CATEGORY SALES")
print("=" * 60)

spark.table(GOLD_CATEGORY_TABLE).show(10, truncate=False)


print("=" * 60)
print("GOLD CITY SALES")
print("=" * 60)

spark.table(GOLD_CITY_TABLE).show(10, truncate=False)


print("=" * 60)
print("GOLD LAYER COMPLETED")
print("=" * 60)

spark.stop()
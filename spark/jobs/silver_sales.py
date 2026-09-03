import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, round


AWS_REGION = os.environ["AWS_REGION"]
ICEBERG_WAREHOUSE = os.environ["ICEBERG_WAREHOUSE"]
ICEBERG_CATALOG = os.environ["ICEBERG_CATALOG"]
ICEBERG_DATABASE = os.environ["ICEBERG_DATABASE"]

BRONZE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.bronze_sales"
SILVER_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.silver_sales"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverSales")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            ICEBERG_WAREHOUSE,
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.glue.region",
            AWS_REGION,
        )
        .config(
            "spark.sql.defaultCatalog",
            ICEBERG_CATALOG,
        )
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    print(f"Reading Bronze table: {BRONZE_TABLE}")

    bronze_df = spark.table(BRONZE_TABLE)

    print(f"Bronze record count: {bronze_df.count()}")

    # Clean and transform the data
    silver_df = (
        bronze_df

        # Remove unnecessary whitespace
        .withColumn("product", trim(col("product")))
        .withColumn("category", trim(col("category")))
        .withColumn("customer_city", trim(col("customer_city")))

        # Standardize payment method
        .withColumn(
            "payment_method",
            lower(trim(col("payment_method")))
        )

        # Calculate total transaction value
        .withColumn(
            "total_amount",
            round(col("quantity") * col("unit_price"), 2)
        )

        # Remove invalid records
        .filter(col("transaction_id").isNotNull())
        .filter(col("transaction_timestamp").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
        .filter(col("product").isNotNull())
        .filter(col("category").isNotNull())
    )

    print(f"Silver record count: {silver_df.count()}")

    print("Silver schema:")
    silver_df.printSchema()

    print(f"Creating Silver table: {SILVER_TABLE}")

    (
        silver_df.writeTo(SILVER_TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("transaction_date")
        .createOrReplace()
    )

    print(f"Silver table created successfully: {SILVER_TABLE}")

    print("Sample Silver data:")
    silver_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
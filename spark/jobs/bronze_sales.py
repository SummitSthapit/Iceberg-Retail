import os
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date


AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET = os.environ["S3_BUCKET"]
ICEBERG_WAREHOUSE = os.environ["ICEBERG_WAREHOUSE"]
ICEBERG_CATALOG = os.environ["ICEBERG_CATALOG"]
ICEBERG_DATABASE = os.environ["ICEBERG_DATABASE"]

S3_RAW_KEY = "raw/sales/sales.csv"
LOCAL_RAW_PATH = "/tmp/sales.csv"
BRONZE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.bronze_sales"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeSales")
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

    s3 = boto3.client("s3", region_name=AWS_REGION)

    print("Downloading raw sales data from S3...")

    s3.download_file(
        S3_BUCKET,
        S3_RAW_KEY,
        LOCAL_RAW_PATH,
    )

    print(f"Downloaded to: {LOCAL_RAW_PATH}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(LOCAL_RAW_PATH)
    )

    print("Raw schema:")
    df.printSchema()

    bronze_df = (
        df
        .withColumn(
            "transaction_date",
            to_date("transaction_timestamp"),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
    )

    print(f"Creating Bronze table: {BRONZE_TABLE}")

    (
        bronze_df.writeTo(BRONZE_TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("transaction_date")
        .createOrReplace()
    )

    print(f"Bronze table created successfully: {BRONZE_TABLE}")

    print(f"Bronze record count: {bronze_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
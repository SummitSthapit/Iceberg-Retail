import os

from pyspark.sql import SparkSession

AWS_REGION = os.environ["AWS_REGION"]
ICEBERG_WAREHOUSE = os.environ["ICEBERG_WAREHOUSE"]
ICEBERG_CATALOG = os.environ["ICEBERG_CATALOG"]
ICEBERG_DATABASE = os.environ["ICEBERG_DATABASE"]

BRONZE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.bronze_sales"


spark = (
    SparkSession.builder
    .appName("VerifyBronze")
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

print("\n=== TABLE ===")
print(BRONZE_TABLE)

print("\n=== COUNT ===")
spark.sql(
    f"SELECT COUNT(*) FROM {BRONZE_TABLE}"
).show()

print("\n=== SAMPLE DATA ===")
spark.sql(
    f"""
    SELECT *
    FROM {BRONZE_TABLE}
    ORDER BY transaction_id
    LIMIT 10
    """
).show(truncate=False)

print("\n=== SCHEMA ===")
spark.sql(
    f"DESCRIBE {BRONZE_TABLE}"
).show(truncate=False)

print("\n=== TABLES ===")
spark.sql(
    f"SHOW TABLES IN {ICEBERG_CATALOG}.{ICEBERG_DATABASE}"
).show()

spark.stop()
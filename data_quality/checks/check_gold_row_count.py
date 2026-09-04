import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

CATALOG = os.getenv("ICEBERG_CATALOG", "retail")
DATABASE = os.getenv("ICEBERG_DATABASE", "retail")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-retail-bucket/warehouse/")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

def create_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG}.glue.region", AWS_REGION)
        .config("spark.sql.defaultCatalog", CATALOG).getOrCreate())

TABLES = {
    "daily": f"{CATALOG}.{DATABASE}.gold_daily_sales",
    "product": f"{CATALOG}.{DATABASE}.gold_product_sales",
    "category": f"{CATALOG}.{DATABASE}.gold_category_sales",
    "city": f"{CATALOG}.{DATABASE}.gold_city_sales",
}

MINIMUM_ROWS={"daily":1,"product":1,"category":1,"city":1}

def main():
    spark=create_spark("DQ_GoldRowCount"); spark.sparkContext.setLogLevel("WARN"); failures=[]
    for name,table in TABLES.items():
        n=spark.table(table).count(); m=MINIMUM_ROWS[name]; print(f"{table} row count: {n} (minimum: {m})")
        if n<m: failures.append(f"{table}: {n} < {m}")
    if failures: raise RuntimeError(f"DATA QUALITY FAILED: Row-count checks failed: {failures}")
    print("DATA QUALITY PASSED: All Gold row counts are within minimum limits"); spark.stop()

if __name__ == "__main__": main()

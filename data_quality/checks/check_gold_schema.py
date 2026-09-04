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

EXPECTED_COLUMNS={
"daily":["transaction_date","transaction_count","unique_transactions","total_quantity","total_sales","average_transaction_value","minimum_transaction_value","maximum_transaction_value"],
"product":["product","category","transaction_count","unique_transactions","total_quantity","total_sales","average_transaction_value"],
"category":["category","transaction_count","unique_transactions","total_quantity","total_sales","average_transaction_value"],
"city":["customer_city","transaction_count","unique_transactions","total_quantity","total_sales","average_transaction_value"]}

def main():
    spark=create_spark("DQ_GoldSchema"); spark.sparkContext.setLogLevel("WARN"); failures=[]
    for name,table in TABLES.items():
        actual=spark.table(table).columns; missing=[c for c in EXPECTED_COLUMNS[name] if c not in actual]
        print(f"\n{table}\nExpected columns: {EXPECTED_COLUMNS[name]}\nActual columns: {actual}")
        if missing: failures.append(f"{table}: missing {missing}")
    if failures: raise RuntimeError(f"DATA QUALITY FAILED: Schema checks failed: {failures}")
    print("\nDATA QUALITY PASSED: All required Gold columns exist"); spark.stop()

if __name__ == "__main__": main()

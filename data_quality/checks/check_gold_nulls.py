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

CRITICAL_COLUMNS={"daily":["transaction_date"],"product":["product","category"],"category":["category"],"city":["customer_city"]}

def main():
    spark=create_spark("DQ_GoldNulls"); spark.sparkContext.setLogLevel("WARN"); failures=[]
    for name,table in TABLES.items():
        df=spark.table(table); row=df.select(*[spark_sum(col(c).isNull().cast("int")).alias(c) for c in CRITICAL_COLUMNS[name]]).collect()[0]
        for c in CRITICAL_COLUMNS[name]:
            n=row[c]; print(f"{table} - {c} NULL count: {n}")
            if n and n>0: failures.append(f"{table}.{c}: {n} NULL values")
    if failures: raise RuntimeError(f"DATA QUALITY FAILED: NULL values found: {failures}")
    print("DATA QUALITY PASSED: No NULL values in critical Gold dimensions"); spark.stop()

if __name__ == "__main__": main()

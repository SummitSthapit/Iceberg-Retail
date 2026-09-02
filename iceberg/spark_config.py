from pyspark.sql import SparkSession


AWS_REGION = "ap-south-1"
S3_BUCKET = "iceberg-retail-bucket"
ICEBERG_WAREHOUSE = f"s3://{S3_BUCKET}/warehouse/"


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session configured for
    Apache Iceberg with AWS Glue Catalog and S3.
    """

    return (
        SparkSession.builder
        .appName(app_name)

        # Iceberg catalog
        .config(
            "spark.sql.catalog.retail",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.retail.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            "spark.sql.catalog.retail.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )

        # S3 warehouse
        .config(
            "spark.sql.catalog.retail.warehouse",
            ICEBERG_WAREHOUSE,
        )

        # AWS region
        .config(
            "spark.sql.catalog.retail.glue.region",
            AWS_REGION,
        )

        .getOrCreate()
    )
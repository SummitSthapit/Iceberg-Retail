import os
import boto3

AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET = os.environ["S3_BUCKET"]

LOCAL_FILE = "/home/iceberg/data/raw/sales.csv"
S3_KEY = "raw/sales/sales.csv"


def upload_sales():
    s3 = boto3.client("s3", region_name=AWS_REGION)

    print(f"Uploading {LOCAL_FILE}")
    print(f"Destination: s3://{S3_BUCKET}/{S3_KEY}")

    s3.upload_file(
        LOCAL_FILE,
        S3_BUCKET,
        S3_KEY,
    )

    print(
        f"Uploaded {LOCAL_FILE} "
        f"to s3://{S3_BUCKET}/{S3_KEY}"
    )


if __name__ == "__main__":
    upload_sales()
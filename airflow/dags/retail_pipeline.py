from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
"owner": "summit",
"depends_on_past": False,
"retries": 1,
}

with DAG(
dag_id="retail_iceberg_pipeline",
default_args=default_args,
description="Retail data pipeline using Spark, Iceberg, and AWS Glue",
start_date=datetime(2026, 9, 1),
schedule=None,
catchup=False,
tags=["retail", "spark", "iceberg", "data-quality"],
) as dag:

    run_gold_pipeline = BashOperator(
        task_id="run_gold_pipeline",
        bash_command=(
            "docker exec spark-iceberg "
            "spark-submit /home/iceberg/spark/jobs/gold_sales.py"
        ),
    )

    run_gold_dq = BashOperator(
        task_id="run_gold_dq",
        bash_command=(
            "docker exec spark-iceberg "
            "python /home/iceberg/data_quality/run_gold_dq.py"
        ),
    )

run_gold_pipeline >> run_gold_dq
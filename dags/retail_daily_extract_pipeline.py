from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_daily_extract_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 10 * * *",  # 4:00 PM IST ≈ 10:30 UTC
    catchup=False,
    tags=["extract", "oracle", "csv"],
) as dag:

    extract_fact_sales = BashOperator(
        task_id="extract_fact_sales",
        bash_command="python /opt/airflow/scripts/extract_sales_daily.py",
    )

    extract_sales_snapshot = BashOperator(
        task_id="extract_sales_snapshot",
        bash_command="python /opt/airflow/scripts/extract_sales_snapshot.py",
    )

    read_extract_snapshot = BashOperator(
        task_id="read_extract_snapshot",
        bash_command="python /opt/airflow/scripts/read_extract_snapshot.py",
    )

    read_current = BashOperator(
        task_id="read_current_file",
        bash_command="python /opt/airflow/scripts/read_current_file.py",
    )

    read_archive = BashOperator(
        task_id="read_archive_files",
        bash_command="python /opt/airflow/scripts/read_archive_files.py",
    )

    # ✅ Proper dependency chain
    extract_fact_sales >> extract_sales_snapshot >> read_extract_snapshot >> read_current >> read_archive
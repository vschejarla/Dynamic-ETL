from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_daily_incremental_pipeline",
    default_args=default_args,
    description="Daily incremental load for Retail Data Warehouse",
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 9 * * *", 
    catchup=False,
    tags=["retail", "oracle", "faker", "incremental"],
) as dag:

    dim_store = BashOperator(
        task_id="dim_store_daily_load",
        bash_command="python /opt/airflow/scripts/dim_store_daily.py",
    )

    dim_product = BashOperator(
        task_id="dim_product_daily_load",
        bash_command="python /opt/airflow/scripts/dim_product_daily.py",
    )

    dim_distributor = BashOperator(
        task_id="dim_distributor_daily_load",
        bash_command="python /opt/airflow/scripts/dim_distributor_daily.py",
    )

    dim_date = BashOperator(
        task_id="dim_date_daily_load",
        bash_command="python /opt/airflow/scripts/dim_date_daily.py",
    )

    fact_sales = BashOperator(
        task_id="fact_sales_daily_load",
        bash_command="python /opt/airflow/scripts/fact_sales_daily.py",
    )

    dim_store >> dim_product >> dim_distributor >> dim_date >> fact_sales

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
    dag_id="retail_target_dw_load_pipeline",
    default_args=default_args,
    description="Load data from Incoming CSV into Target DW",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 11 * * *",  # ✅ 5:00 PM IST
    catchup=False,
    tags=["dw", "target", "etl"],
) as dag:

    # -----------------------------
    # Dimension Loads
    # -----------------------------
    load_dim_store = BashOperator(
        task_id="load_dim_store_dw",
        bash_command="python /opt/airflow/scripts2/load_dim_store_dw.py",
    )

    load_dim_product = BashOperator(
        task_id="load_dim_product_dw",
        bash_command="python /opt/airflow/scripts2/load_dim_product_dw.py",
    )

    load_dim_distributor = BashOperator(
        task_id="load_dim_distributor_dw",
        bash_command="python /opt/airflow/scripts2/load_dim_distributor_dw.py",
    )

    load_dim_date = BashOperator(
        task_id="load_dim_date_dw",
        bash_command="python /opt/airflow/scripts2/load_dim_date_dw.py",
    )

    # -----------------------------
    # Fact Load
    # -----------------------------
    load_fact_sales = BashOperator(
        task_id="load_fact_sales_dw",
        bash_command="python /opt/airflow/scripts2/load_fact_sales_dw.py",
    )

    # -----------------------------
    # Dependency Chain
    # -----------------------------
    (
        load_dim_store
        >> load_dim_product
        >> load_dim_distributor
        >> load_dim_date
        >> load_fact_sales
    )
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
    dag_id="retail_target_dw_validation_pipeline",
    default_args=default_args,
    description="Validates all Target DW tables in Oracle",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 12 * * *",  # 6:00 PM IST
    catchup=False,
    tags=["validation", "dw", "target"],
) as dag:

    # -----------------------------
    # DIM STORE
    # -----------------------------
    validate_dim_store = BashOperator(
        task_id="validate_dim_store_dw",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_STORE_DW
        """
    )

    # STORE CHAIN
    validate_dim_store_chain = BashOperator(
        task_id="validate_dim_store_chain_dw",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_STORE_CHAIN_DW
        """
    )

    # -----------------------------
    # PRODUCT DIMENSIONS
    # -----------------------------
    validate_dim_category = BashOperator(
        task_id="validate_dim_category",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_CATEGORY
        """
    )

    validate_dim_sub_category = BashOperator(
        task_id="validate_dim_sub_category",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_SUB_CATEGORY
        """
    )

    validate_dim_manufacturer = BashOperator(
        task_id="validate_dim_manufacturer",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_MANUFACTURER
        """
    )

    validate_dim_product = BashOperator(
        task_id="validate_dim_product_dw",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_PRODUCT_DW
        """
    )

    # -----------------------------
    # DISTRIBUTOR
    # -----------------------------
    validate_dim_distributor = BashOperator(
        task_id="validate_dim_distributor_dw",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_DISTRIBUTOR_DW
        """
    )

    # -----------------------------
    # DATE DIMENSION
    # -----------------------------
    validate_dim_date = BashOperator(
        task_id="validate_dim_date_dw",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table DIM_DATE_DW
        """
    )

    # -----------------------------
    # FACT SALES
    # -----------------------------
    validate_fact_sales = BashOperator(
        task_id="validate_fact_sales_dw",
        bash_command="""
        python /opt/airflow/scripts2/validate_target_dw.py \
        --table FACT_SALES_DW
        """
    )

    # -------------------------------------------------------
    # DAG EXECUTION ORDER
    # -------------------------------------------------------
    (
        validate_dim_store
        >> validate_dim_store_chain
        >> validate_dim_category
        >> validate_dim_sub_category
        >> validate_dim_manufacturer
        >> validate_dim_product
        >> validate_dim_distributor
        >> validate_dim_date
        >> validate_fact_sales
    )

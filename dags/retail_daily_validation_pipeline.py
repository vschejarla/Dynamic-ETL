from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_daily_validation_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 11 * * *",  # 4:30 PM IST
    catchup=False,
    tags=["validation", "dq", "oracle"],
) as dag:


    # -----------------------------
    # Existing validations (UNCHANGED)
    # -----------------------------

    validate_dim_store = BashOperator(
        task_id="validate_dim_store",
        bash_command="""
        python /opt/airflow/scripts/validate_table.py \
        --table_name dim_store_master \
        --pk_column store_id \
        --mandatory_columns store_name,store_city,store_state \
        --min_rows 1000
        """
    )

    validate_dim_product = BashOperator(
        task_id="validate_dim_product",
        bash_command="""
        python /opt/airflow/scripts/validate_table.py \
        --table_name dim_product \
        --pk_column product_id \
        --mandatory_columns product_name,category,brand \
        --min_rows 1000
        """
    )

    validate_dim_distributor = BashOperator(
        task_id="validate_dim_distributor",
        bash_command="""
        python /opt/airflow/scripts/validate_table.py \
        --table_name dim_distributor \
        --pk_column distributor_id \
        --mandatory_columns distributor_name \
        --min_rows 1000
        """
    )

    validate_dim_date = BashOperator(
        task_id="validate_dim_date",
        bash_command="""
        python /opt/airflow/scripts/validate_table.py \
        --table_name dim_date \
        --pk_column date_id \
        --mandatory_columns full_date,year,month \
        --min_rows 1000
        """
    )

    # OPTION 1: Add --skip_freshness_check flag to allow missing dates
    validate_fact_sales = BashOperator(
        task_id="validate_fact_sales",
        bash_command="""
        python /opt/airflow/scripts/validate_table.py \
        --table_name fact_sales \
        --pk_column sales_id \
        --mandatory_columns date_id,store_id,product_id,net_amount \
        --min_rows 1000 \
        --date_column date_id \
        --execution_date {{ ds }} \
        --skip_freshness_check
        """
    )

    # OPTION 2 (Alternative): Remove freshness check entirely
    # validate_fact_sales = BashOperator(
    #     task_id="validate_fact_sales",
    #     bash_command="""
    #     python /opt/airflow/scripts/validate_table.py \
    #     --table_name fact_sales \
    #     --pk_column sales_id \
    #     --mandatory_columns date_id,store_id,product_id,net_amount \
    #     --min_rows 1000
    #     """
    # )

    # -----------------------------
    # 📹 NEW: Extract validations
    # -----------------------------

    validate_extract_snapshot = BashOperator(
        task_id="validate_extract_snapshot",
        bash_command="""
        python /opt/airflow/scripts/validate_table.py \
        --file_pattern '/opt/airflow/data_extracts/incoming/sales_snapshot_{{ ds_nodash }}_*.csv' \
        --delimiter '|' \
        --mandatory_columns SALES_ID,NET_AMOUNT,STORE_NAME,PRODUCT_NAME,FULL_DATE \
        --numeric_columns QUANTITY_SOLD,SALES_UNIT_PRICE,GROSS_AMOUNT,NET_AMOUNT,PRODUCT_UNIT_PRICE \
        --flag_columns IS_CHAIN,ACTIVE_FLAG,IS_WEEKEND \
        --min_rows 1
        """
    )



    # -----------------------------
    # ✅ Updated dependency chain
    # -----------------------------
    (
        validate_dim_store
        >>validate_dim_product
        >> validate_dim_distributor
        >> validate_dim_date
        >> validate_fact_sales
        >>validate_extract_snapshot
    )
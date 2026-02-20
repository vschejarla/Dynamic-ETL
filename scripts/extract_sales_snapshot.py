import os
import csv
from datetime import datetime
import oracledb

# -----------------------------
# Output directory (INCOMING)
# -----------------------------
BASE_PATH = "/opt/airflow/data_extracts/incoming"
os.makedirs(BASE_PATH, exist_ok=True)

# File name with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
file_name = f"sales_snapshot_{timestamp}.csv"
file_path = os.path.join(BASE_PATH, file_name)

print(f"📄 Writing file: {file_path}")

# -----------------------------
# Oracle Connection
# -----------------------------
connection = oracledb.connect(
    user="system",
<<<<<<< HEAD
    password="905966Sh@r4107",
=======
    password="oracle123",
>>>>>>> etl-update
    dsn="host.docker.internal/orcl"
)
cursor = connection.cursor()

# -----------------------------
# Query (UNCHANGED)
# -----------------------------
query = """
SELECT
    fs.sales_id,
    fs.quantity_sold,
    fs.unit_price AS sales_unit_price,
    fs.gross_amount,
    fs.discount_amount,
    fs.net_amount,
<<<<<<< HEAD
=======
    
    dt.full_date,
    dt.day,
    dt.day_name,
    dt.week_of_year,
    dt.month,
    dt.month_name,
    dt.quarter,
    dt.year,
    dt.is_weekend,
>>>>>>> etl-update

    ds.store_name,
    ds.store_address_lane_1,
    ds.store_address_lane_2,
    ds.store_city,
    ds.store_zip,
    ds.store_state,
    ds.store_class_of_trade,
    ds.is_chain,
    ds.chain_name,

    dp.product_name,
    dp.category,
    dp.sub_category,
    dp.brand,
    dp.flavour,
    dp.product_size,
<<<<<<< HEAD
    dp.sqc,
    dp.uom,
    dp.unit_price AS product_unit_price,
=======
    dp.sku,
    dp.uom,
    dp.unit_price AS product_unit_price,
    dp.business_stage,
>>>>>>> etl-update

    dd.distributor_name,
    dd.city AS distributor_city,
    dd.state AS distributor_state,
    dd.distributor_type,
    dd.onboarding_date,
<<<<<<< HEAD
    dd.active_flag,

    dt.full_date,
    dt.day,
    dt.day_name,
    dt.week_of_year,
    dt.month,
    dt.month_name,
    dt.quarter,
    dt.year,
    dt.is_weekend
=======
    dd.active_flag
>>>>>>> etl-update
FROM fact_sales fs
JOIN dim_store_master ds ON fs.store_id = ds.store_id
JOIN dim_product dp ON fs.product_id = dp.product_id
JOIN dim_distributor dd ON fs.distributor_id = dd.distributor_id
JOIN dim_date dt ON fs.date_id = dt.date_id
<<<<<<< HEAD
=======
ORDER BY fs.sales_id
>>>>>>> etl-update
"""

cursor.execute(query)

# -----------------------------
# Write CSV
# -----------------------------
with open(file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter="|")  # using | is DW best practice
    writer.writerow([col[0] for col in cursor.description])
    writer.writerows(cursor.fetchall())

print("✅ CSV file generated successfully in incoming folder")

cursor.close()
connection.close()
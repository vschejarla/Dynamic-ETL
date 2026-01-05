import os
import shutil
import csv
from datetime import datetime
import oracledb

# Paths inside container
BASE_PATH = "/opt/airflow/data_extracts"
CURRENT_DIR = os.path.join(BASE_PATH, "Current")
ARCHIVE_DIR = os.path.join(BASE_PATH, "Archive")

# Ensure folders exist
os.makedirs(CURRENT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

print("📂 Folders checked")

# Move existing files from Current → Archive
for file in os.listdir(CURRENT_DIR):
    src = os.path.join(CURRENT_DIR, file)
    dest = os.path.join(ARCHIVE_DIR, file)
    shutil.move(src, dest)
    print(f"Moved {file} to Archive")

# File name with date
today = datetime.now().strftime("%Y%m%d")
file_name = f"fact_sales_{today}.csv"
file_path = os.path.join(CURRENT_DIR, file_name)

# Oracle connection
connection = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)

cursor = connection.cursor()

query = """
SELECT
    sales_id,
    date_id,
    store_id,
    product_id,
    distributor_id,
    quantity_sold,
    gross_amount,
    discount_amount,
    net_amount
FROM fact_sales
"""

cursor.execute(query)

# Write CSV
with open(file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([col[0] for col in cursor.description])
    writer.writerows(cursor.fetchall())

print(f"✅ Data extracted to {file_name}")

cursor.close()
connection.close()

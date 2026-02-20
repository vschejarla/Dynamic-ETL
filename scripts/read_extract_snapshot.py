import os
import pandas as pd

INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

# -----------------------------
# 1️⃣ Get all CSV files
# -----------------------------
files = [
    f for f in os.listdir(INCOMING_DIR)
    if f.endswith(".csv")
]

if not files:
    raise FileNotFoundError("❌ No files found in incoming folder")

# -----------------------------
# 2️⃣ Pick latest file (by name timestamp)
# -----------------------------
latest_file = sorted(files)[-1]
file_path = os.path.join(INCOMING_DIR, latest_file)

print(f"📄 Reading latest file: {latest_file}")

# -----------------------------
# 3️⃣ Read CSV using PIPE delimiter
# -----------------------------
df = pd.read_csv(file_path, delimiter="|")

print(f"✅ Rows loaded: {len(df)}")
print(df.head())
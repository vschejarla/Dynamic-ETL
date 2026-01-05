import os
import pandas as pd

ARCHIVE_DIR = "/opt/airflow/data_extracts/Archive"

files = sorted(os.listdir(ARCHIVE_DIR))

if not files:
    print("ℹ️ No archive files found")
    exit(0)

for file in files:
    file_path = os.path.join(ARCHIVE_DIR, file)
    print("📦 Reading ARCHIVE file:", file_path)

    df = pd.read_csv(
        file_path,
        delimiter="|",
        encoding="utf-8"
    )

    print(f"✅ {file} → Rows: {len(df)}")

import os
import pandas as pd

CURRENT_DIR = "/opt/airflow/data_extracts/Current"

files = sorted(os.listdir(CURRENT_DIR), reverse=True)

if not files:
    raise FileNotFoundError(" No file found in Current folder")

file_path = os.path.join(CURRENT_DIR, files[0])

print(" Reading CURRENT file:", file_path)

df = pd.read_csv(
    file_path,
    delimiter="|",
    encoding="utf-8"
)

print(" CURRENT rows:", len(df))
print(df.head())

import argparse
import os
import glob
import pandas as pd
import oracledb
from datetime import datetime

# --------------------------------------------------
# Argument Parsing
# --------------------------------------------------
parser = argparse.ArgumentParser()

# Table validation
parser.add_argument("--table_name")
parser.add_argument("--pk_column")
parser.add_argument("--date_column")
parser.add_argument("--execution_date")
parser.add_argument("--skip_freshness_check", action="store_true", help="Skip the freshness check if no data exists")

# File validation
parser.add_argument("--file_path")
parser.add_argument("--file_pattern", help="Use glob pattern to find files (alternative to --file_path)")
parser.add_argument("--delimiter", default="|")

# Common
parser.add_argument("--mandatory_columns", required=True)
parser.add_argument("--numeric_columns")
parser.add_argument("--flag_columns")
parser.add_argument("--min_rows", required=True)

args = parser.parse_args()

mandatory_columns = args.mandatory_columns.split(",")
numeric_columns = args.numeric_columns.split(",") if args.numeric_columns else []
flag_columns = args.flag_columns.split(",") if args.flag_columns else []
min_rows = int(args.min_rows)

# ==================================================
# 📹 FILE VALIDATION LOGIC
# ==================================================
if args.file_path or args.file_pattern:
    # Determine which parameter to use
    pattern = args.file_pattern if args.file_pattern else args.file_path
    
    # Find matching files
    if '*' in pattern or '?' in pattern:
        files = sorted(glob.glob(pattern), reverse=True)
        if not files:
            raise FileNotFoundError(f"❌ No file found matching pattern: {pattern}")
        file_path = files[0]
        print(f"📄 Found {len(files)} file(s), validating latest: {os.path.basename(file_path)}")
    else:
        # Direct file path
        file_path = pattern
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        print(f"📄 Validating file: {os.path.basename(file_path)}")

    df = pd.read_csv(file_path, delimiter=args.delimiter)

    # ----------------------------
    # 1️⃣ Row count
    # ----------------------------
    if len(df) < min_rows:
        raise ValueError(f"❌ Row count {len(df)} < {min_rows}")
    print(f"✅ Row count OK: {len(df)}")

    # ----------------------------
    # 2️⃣ Mandatory columns
    # ----------------------------
    for col in mandatory_columns:
        if col not in df.columns:
            raise ValueError(f"❌ Missing column: {col}")
        if df[col].isnull().any():
            null_count = df[col].isnull().sum()
            raise ValueError(f"❌ NULL values found in {col}: {null_count} rows")
    print("✅ Mandatory column check passed")

    # ----------------------------
    # 3️⃣ Numeric validation
    # ----------------------------
    if numeric_columns:
        for col in numeric_columns:
            if col not in df.columns:
                raise ValueError(f"❌ Missing numeric column: {col}")

            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace("", None)
            )

            df[col] = pd.to_numeric(df[col], errors="coerce")

            if df[col].isnull().any():
                bad_rows = df[df[col].isnull()]
                raise ValueError(
                    f"❌ Invalid numeric values in {col}\n"
                    f"First 5 bad rows:\n{bad_rows[[col]].head()}"
                )

        print("✅ Numeric columns validated")

    # ----------------------------
    # 4️⃣ Flag validation
    # ----------------------------
    if flag_columns:
        for col in flag_columns:
            if col not in df.columns:
                raise ValueError(f"❌ Missing flag column: {col}")
            invalid = df[~df[col].isin(["Y", "N"])]
            if len(invalid) > 0:
                raise ValueError(f"❌ Invalid flag values in {col}: {invalid[col].unique()}")
        print("✅ Flag columns validated")

    print("🎉 FILE VALIDATION SUCCESS")
    exit(0)

# ==================================================
# 📹 TABLE VALIDATION LOGIC (ORACLE)
# ==================================================
print(f"🔍 Validating table: {args.table_name}")

conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

# ----------------------------
# 1️⃣ Row count
# ----------------------------
cur.execute(f"SELECT COUNT(*) FROM {args.table_name}")
row_count = cur.fetchone()[0]

if row_count < min_rows:
    raise ValueError(f"❌ Row count {row_count} < {min_rows}")

print(f"✅ Row count OK: {row_count}")

# ----------------------------
# 2️⃣ NULL checks
# ----------------------------
for col in mandatory_columns:
    cur.execute(
        f"SELECT COUNT(*) FROM {args.table_name} WHERE {col} IS NULL"
    )
    null_count = cur.fetchone()[0]
    if null_count > 0:
        raise ValueError(f"❌ NULL values in {col}: {null_count} rows")

print("✅ Mandatory column check passed")

# ----------------------------
# 3️⃣ Duplicate PK
# ----------------------------
cur.execute(f"""
    SELECT COUNT(*) FROM (
        SELECT {args.pk_column}
        FROM {args.table_name}
        GROUP BY {args.pk_column}
        HAVING COUNT(*) > 1
    )
""")

duplicate_count = cur.fetchone()[0]
if duplicate_count > 0:
    raise ValueError(f"❌ Duplicate primary keys found: {duplicate_count} duplicates")

print("✅ No duplicate primary keys")

# ----------------------------
# 4️⃣ Freshness check
# ----------------------------
if args.date_column and args.execution_date:
    date_key = args.execution_date.replace("-", "")
    cur.execute(
        f"""
        SELECT COUNT(*) FROM {args.table_name}
        WHERE {args.date_column} = :1
        """,
        [date_key]
    )
    record_count = cur.fetchone()[0]
    
    if record_count == 0:
        if args.skip_freshness_check:
            print(f"⚠️  WARNING: No data for date {args.execution_date} (skipped due to --skip_freshness_check)")
        else:
            raise ValueError(f"❌ No data for date {args.execution_date}")
    else:
        print(f"✅ Freshness check passed: {record_count} records for {args.execution_date}")

print("🎉 TABLE VALIDATION SUCCESS")

cur.close()
conn.close()
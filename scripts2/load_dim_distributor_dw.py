import sys
import os
import pandas as pd
import oracledb
from datetime import datetime

# ========================================
# CONFIG
# ========================================
INCOMING_DIR   = "/opt/airflow/data_extracts/incoming"
# extract_to_csv.py writes distributor_YYYYMMDD.csv
FILE_PREFIX    = "distributor_"
PROCESSED_LOG  = "/opt/airflow/data_extracts/processed_distributors.log"

DB_CONFIG = {
    "user":     "target_dw",
    "password": "target_dw123",
    "dsn":      "host.docker.internal/orcl"
}

# ========================================
# FILE TRACKING
# ========================================
def is_file_processed(filename):
    if not os.path.exists(PROCESSED_LOG):
        return False
    with open(PROCESSED_LOG, 'r') as f:
        return any(line.startswith(filename) for line in f.read().splitlines())

def mark_file_processed(filename):
    os.makedirs(os.path.dirname(PROCESSED_LOG), exist_ok=True)
    with open(PROCESSED_LOG, 'a') as f:
        f.write(f"{filename}|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ========================================
# DATA CLEANING
# ========================================
def clean_distributor_name(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN DISTRIBUTOR"
    return ' '.join(str(val).strip().split()).upper()

def clean_city(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    return str(val).strip().title()

def clean_state(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    return str(val).strip().title()

def clean_distributor_type(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "Local"
    val_upper = str(val).strip().upper()
    type_map = {
        "NATIONAL": "National", "NAT": "National", "PAN INDIA": "National",
        "REGIONAL": "Regional", "REG": "Regional", "MULTI STATE": "Regional",
        "LOCAL": "Local",       "CITY": "Local",    "DISTRICT": "Local",
    }
    for key, value in type_map.items():
        if key in val_upper:
            return value
    return val.strip().title()

def clean_active_flag(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return 'Y'
    return 'Y' if str(val).strip().upper() in ('Y','YES','1','TRUE','ACTIVE','A') else 'N'

def clean_onboarding_date(val):
    if pd.isna(val) or val is None or val == '':
        return None
    try:
        parsed = pd.to_datetime(val, errors='coerce')
        if pd.isna(parsed):
            return None
        if parsed < datetime(1990, 1, 1) or parsed > datetime.now():
            return None
        return parsed
    except Exception:
        return None

# ========================================
# GUARD: incoming directory must exist
# ========================================
if not os.path.exists(INCOMING_DIR):
    raise RuntimeError(f"Incoming directory not found: {INCOMING_DIR}")

# ========================================
# FIND NEXT UNPROCESSED FILE
# ========================================
all_files = sorted(
    [f for f in os.listdir(INCOMING_DIR)
     if f.startswith(FILE_PREFIX) and f.endswith(".csv")],
    reverse=True
)

if not all_files:
    print(f"No '{FILE_PREFIX}*.csv' files found in {INCOMING_DIR}")
    sys.exit(0)

source_file = next((f for f in all_files if not is_file_processed(f)), None)
if source_file is None:
    print(f"All {len(all_files)} file(s) already processed — nothing to do")
    sys.exit(0)

file_path = os.path.join(INCOMING_DIR, source_file)

# ========================================
# READ & VALIDATE FILE
# ========================================
try:
    df = pd.read_csv(
        file_path, delimiter="|", dtype=str,
        na_values=['', 'NULL', 'null', 'NA']
    )
except Exception as e:
    raise RuntimeError(f"Failed to read '{source_file}': {e}") from e

if len(df) == 0:
    mark_file_processed(source_file)
    print(f"Empty file '{source_file}' — marked as processed")
    sys.exit(0)

df.columns = df.columns.str.strip().str.upper()

REQUIRED_COLS = [
    "DISTRIBUTOR_NAME", "DISTRIBUTOR_CITY", "DISTRIBUTOR_STATE",
    "DISTRIBUTOR_TYPE", "ONBOARDING_DATE", "ACTIVE_FLAG",
]
missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
if missing_cols:
    mark_file_processed(source_file)   # prevent retry loop
    raise RuntimeError(f"Missing columns {missing_cols} in '{source_file}'")

# ========================================
# CLEAN & DEDUPLICATE
# ========================================
df["DISTRIBUTOR_NAME_CLEAN"]  = df["DISTRIBUTOR_NAME"].apply(clean_distributor_name)
df["DISTRIBUTOR_CITY_CLEAN"]  = df["DISTRIBUTOR_CITY"].apply(clean_city)
df["DISTRIBUTOR_STATE_CLEAN"] = df["DISTRIBUTOR_STATE"].apply(clean_state)
df["DISTRIBUTOR_TYPE_CLEAN"]  = df["DISTRIBUTOR_TYPE"].apply(clean_distributor_type)
df["ACTIVE_FLAG_CLEAN"]       = df["ACTIVE_FLAG"].apply(clean_active_flag)
df["ONBOARDING_DATE_CLEAN"]   = df["ONBOARDING_DATE"].apply(clean_onboarding_date)

df_valid = df[
    df["DISTRIBUTOR_NAME_CLEAN"].notna() &
    (df["DISTRIBUTOR_NAME_CLEAN"] != "UNKNOWN DISTRIBUTOR") &
    df["DISTRIBUTOR_CITY_CLEAN"].notna() &
    df["DISTRIBUTOR_STATE_CLEAN"].notna()
].copy()

dist_df = df_valid.drop_duplicates(
    subset=["DISTRIBUTOR_NAME_CLEAN","DISTRIBUTOR_CITY_CLEAN","DISTRIBUTOR_STATE_CLEAN"]
)[[
    "DISTRIBUTOR_NAME_CLEAN","DISTRIBUTOR_CITY_CLEAN","DISTRIBUTOR_STATE_CLEAN",
    "DISTRIBUTOR_TYPE_CLEAN","ONBOARDING_DATE_CLEAN","ACTIVE_FLAG_CLEAN",
]]

if len(dist_df) == 0:
    mark_file_processed(source_file)
    print("No valid distributor records after cleaning — file marked processed")
    sys.exit(0)

# ========================================
# DATABASE WORK
# ========================================
conn = oracledb.connect(**DB_CONFIG)
cur  = conn.cursor()

try:
    # ── Staging table ────────────────────────────────────────────────────────
    try:
        cur.execute("DROP TABLE distributor_staging")
    except Exception:
        pass

    cur.execute("""
        CREATE TABLE distributor_staging (
            dist_key             NUMBER,
            dist_name            VARCHAR2(50),
            dist_type            VARCHAR2(30),
            dist_city            VARCHAR2(30),
            dist_state           VARCHAR2(30),
            dist_onboarding_date DATE,
            dist_active_flag     CHAR(1),
            operation            VARCHAR2(10)
        )
    """)
    conn.commit()

    # ── Load existing distributor cache ──────────────────────────────────────
    cur.execute("""
        SELECT dist_key, dist_name, dist_city, dist_state,
               dist_type, dist_onboarding_date, dist_active_flag
        FROM dim_distributor_dw
    """)
    dist_cache = {}
    for r in cur.fetchall():
        bk = (r[1], r[2], r[3])   # name, city, state
        dist_cache[bk] = {
            'dist_key':             r[0],
            'dist_type':            r[4],
            'dist_onboarding_date': r[5],
            'dist_active_flag':     r[6],
        }

    cur.execute("SELECT NVL(MAX(dist_key), 0) FROM dim_distributor_dw")
    next_key = cur.fetchone()[0] + 1

    # ── Classify INSERT vs UPDATE ─────────────────────────────────────────────
    insert_records = []
    update_records = []

    for _, row in dist_df.iterrows():
        bk = (
            row["DISTRIBUTOR_NAME_CLEAN"],
            row["DISTRIBUTOR_CITY_CLEAN"],
            row["DISTRIBUTOR_STATE_CLEAN"],
        )
        if bk not in dist_cache:
            insert_records.append({'dist_key': next_key, 'bk': bk, 'data': row})
            next_key += 1
        else:
            existing = dist_cache[bk]
            ex_date  = existing['dist_onboarding_date']
            new_date = row["ONBOARDING_DATE_CLEAN"]

            date_changed = False
            if ex_date is None and new_date is not None:
                date_changed = True
            elif ex_date is not None and new_date is not None:
                ex_d  = ex_date.date()  if isinstance(ex_date,  datetime) else ex_date
                new_d = new_date.date() if isinstance(new_date, pd.Timestamp) else new_date
                date_changed = ex_d != new_d

            if (
                row["DISTRIBUTOR_TYPE_CLEAN"]  != existing['dist_type'] or
                row["ACTIVE_FLAG_CLEAN"]        != existing['dist_active_flag'] or
                date_changed
            ):
                update_records.append({
                    'dist_key': existing['dist_key'], 'bk': bk, 'data': row
                })

    # ── Load staging ─────────────────────────────────────────────────────────
    staging_data = []
    for rec in insert_records + update_records:
        row = rec['data']
        staging_data.append((
            rec['dist_key'],
            row["DISTRIBUTOR_NAME_CLEAN"][:50],
            row["DISTRIBUTOR_TYPE_CLEAN"][:30],
            row["DISTRIBUTOR_CITY_CLEAN"][:30],
            row["DISTRIBUTOR_STATE_CLEAN"][:30],
            row["ONBOARDING_DATE_CLEAN"],
            row["ACTIVE_FLAG_CLEAN"],
            'INSERT' if rec in insert_records else 'UPDATE',
        ))

    if not staging_data:
        cur.execute("DROP TABLE distributor_staging")
        conn.commit()
        mark_file_processed(source_file)
        # Stats query still runs below via the finally-guarded path
        rows_merged = 0
    else:
        cur.executemany("""
            INSERT INTO distributor_staging (
                dist_key, dist_name, dist_type, dist_city, dist_state,
                dist_onboarding_date, dist_active_flag, operation
            ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)
        """, staging_data)

        # ── MERGE ────────────────────────────────────────────────────────────
        cur.execute("""
            MERGE INTO dim_distributor_dw tgt
            USING distributor_staging stg
            ON (tgt.dist_key = stg.dist_key)
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.dist_type            = stg.dist_type,
                    tgt.dist_onboarding_date = stg.dist_onboarding_date,
                    tgt.dist_active_flag     = stg.dist_active_flag
            WHEN NOT MATCHED THEN
                INSERT (
                    dist_key, dist_name, dist_type, dist_city, dist_state,
                    dist_onboarding_date, dist_active_flag
                )
                VALUES (
                    stg.dist_key, stg.dist_name, stg.dist_type, stg.dist_city, stg.dist_state,
                    stg.dist_onboarding_date, stg.dist_active_flag
                )
        """)
        rows_merged = cur.rowcount
        conn.commit()

        try:
            cur.execute("DROP TABLE distributor_staging")
            conn.commit()
        except Exception:
            pass

        mark_file_processed(source_file)

    # ── Summary stats ─────────────────────────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN dist_active_flag = 'Y' THEN 1 ELSE 0 END),
               SUM(CASE WHEN dist_active_flag = 'N' THEN 1 ELSE 0 END)
        FROM dim_distributor_dw
    """)
    s = cur.fetchone()

    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"file: {source_file} | "
        f"insert: {len(insert_records)} | update: {len(update_records)} | "
        f"merged: {rows_merged} | "
        f"DB total: {s[0]} active: {s[1]} inactive: {s[2]}"
    )

except Exception:
    conn.rollback()
    raise
finally:
    try:
        cur.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
import sys
import os
import re
import pandas as pd
import oracledb
from datetime import datetime

# ========================================
# CONFIG
# ========================================
INCOMING_DIR  = "/opt/airflow/data_extracts/incoming"
# extract_to_csv.py writes store_YYYYMMDD.csv
FILE_PREFIX   = "store_"
PROCESSED_LOG = "/opt/airflow/data_extracts/processed_stores.log"

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
def clean_store_name(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN STORE"
    return ' '.join(str(val).strip().split()).upper()

def clean_address(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    return ' '.join(str(val).strip().split()).upper()

def clean_city(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    return str(val).strip().title()

def clean_state(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    return str(val).strip().title()

def clean_zip(val):
    if pd.isna(val) or val is None:
        return None
    digits = re.sub(r'[^0-9]', '', str(val).strip())
    if len(digits) >= 6:
        return digits[:6]
    return None

def clean_class_of_trade(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "General Trade"
    val_upper = str(val).strip().upper()
    cot_map = {
        "HYPERMARKET":   "Modern Trade - Hypermarket",
        "SUPERMARKET":   "Modern Trade - Supermarket",
        "KIRANA":        "General Trade - Kirana",
        "GENERAL TRADE": "General Trade - Kirana",
        "CONVENIENCE":   "Convenience Store",
        "WHOLESALE":     "Cash & Carry - Wholesale",
        "RETAIL":        "Retail",
    }
    for key, value in cot_map.items():
        if key in val_upper:
            return value
    return val.strip()

def clean_is_chain(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return 'N'
    return 'Y' if str(val).strip().upper() in ('Y','YES','1','TRUE') else 'N'

def clean_chain_name(val, is_chain):
    if is_chain == 'N' or pd.isna(val) or val is None or str(val).strip() == '':
        return None
    return ' '.join(str(val).strip().split()).title()

# ========================================
# GUARD: incoming directory
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

# STORE_ADDRESS_LANE_2, STORE_ZIP, CHAIN_NAME are optional columns —
# their absence should not abort the run.
REQUIRED_COLS = ["STORE_NAME", "STORE_ADDRESS_LANE_1", "STORE_CITY",
                 "STORE_STATE", "STORE_CLASS_OF_TRADE", "IS_CHAIN"]
OPTIONAL_COLS = ["STORE_ADDRESS_LANE_2", "STORE_ZIP", "CHAIN_NAME"]

missing_required = [c for c in REQUIRED_COLS if c not in df.columns]
if missing_required:
    mark_file_processed(source_file)   # prevent retry loop
    raise RuntimeError(f"Missing required columns {missing_required} in '{source_file}'")

# Inject empty columns for any missing optional columns so downstream code is uniform
for col in OPTIONAL_COLS:
    if col not in df.columns:
        df[col] = None

# ========================================
# CLEAN DATA
# ========================================
df["STORE_NAME_CLEAN"]            = df["STORE_NAME"].apply(clean_store_name)
df["STORE_ADDRESS_LANE_1_CLEAN"]  = df["STORE_ADDRESS_LANE_1"].apply(clean_address)
df["STORE_ADDRESS_LANE_2_CLEAN"]  = df["STORE_ADDRESS_LANE_2"].apply(clean_address)
df["STORE_CITY_CLEAN"]            = df["STORE_CITY"].apply(clean_city)
df["STORE_ZIP_CLEAN"]             = df["STORE_ZIP"].apply(clean_zip)
df["STORE_STATE_CLEAN"]           = df["STORE_STATE"].apply(clean_state)
df["STORE_CLASS_OF_TRADE_CLEAN"]  = df["STORE_CLASS_OF_TRADE"].apply(clean_class_of_trade)
df["IS_CHAIN_CLEAN"]              = df["IS_CHAIN"].apply(clean_is_chain)
df["CHAIN_NAME_CLEAN"]            = df.apply(
    lambda row: clean_chain_name(row["CHAIN_NAME"], row["IS_CHAIN_CLEAN"]), axis=1
)

# Chains with no name → demote to independent
chain_no_name = df[(df["IS_CHAIN_CLEAN"] == "Y") & (df["CHAIN_NAME_CLEAN"].isna())]
if len(chain_no_name) > 0:
    df.loc[chain_no_name.index, "IS_CHAIN_CLEAN"] = "N"

df_valid = df[
    df["STORE_NAME_CLEAN"].notna() &
    (df["STORE_NAME_CLEAN"] != "UNKNOWN STORE") &
    df["STORE_ADDRESS_LANE_1_CLEAN"].notna() &
    df["STORE_CITY_CLEAN"].notna() &
    df["STORE_STATE_CLEAN"].notna()
].copy()

if len(df_valid) == 0:
    mark_file_processed(source_file)
    print("No valid store records after cleaning — file marked processed")
    sys.exit(0)

# ========================================
# DATABASE
# ========================================
conn = oracledb.connect(**DB_CONFIG)
cur  = conn.cursor()

try:
    # ── Drop/create staging tables ───────────────────────────────────────────
    # each DROP in its own try/except so the second always runs
    for tbl in ("store_chain_staging", "store_staging"):
        try:
            cur.execute(f"DROP TABLE {tbl}")
        except Exception:
            pass

    cur.execute("""
        CREATE TABLE store_chain_staging (
            chain_key  NUMBER,
            chain_name VARCHAR2(50),
            operation  VARCHAR2(10)
        )
    """)
    cur.execute("""
        CREATE TABLE store_staging (
            store_key            NUMBER,
            store_name           VARCHAR2(50),
            store_address_1      VARCHAR2(100),
            store_address_2      VARCHAR2(100),
            store_city           VARCHAR2(25),
            store_zip            VARCHAR2(10),
            store_state          VARCHAR2(50),
            store_class_of_trade VARCHAR2(50),
            store_is_chain       CHAR(1),
            chain_key            NUMBER,
            operation            VARCHAR2(10)
        )
    """)
    # No commit here — defer to the single commit at the end 

    # ── Store chain dimension ────────────────────────────────────────────────
    cur.execute("SELECT chain_key, chain_name FROM dim_store_chain_dw")
    chain_cache = {r[1]: r[0] for r in cur.fetchall()}

    new_chains = [
        c for c in df_valid["CHAIN_NAME_CLEAN"].dropna().unique()
        if c not in chain_cache
    ]

    if new_chains:
        cur.execute("SELECT NVL(MAX(chain_key), 0) FROM dim_store_chain_dw")
        next_chain_key = cur.fetchone()[0] + 1
        chain_staging  = []
        for i, chain in enumerate(new_chains):
            chain_cache[chain] = next_chain_key + i
            chain_staging.append((next_chain_key + i, chain, 'INSERT'))

        cur.executemany(
            "INSERT INTO store_chain_staging (chain_key, chain_name, operation) VALUES (:1,:2,:3)",
            chain_staging
        )
        cur.execute("""
            MERGE INTO dim_store_chain_dw tgt
            USING store_chain_staging stg
            ON (tgt.chain_key = stg.chain_key)
            WHEN NOT MATCHED THEN
                INSERT (chain_key, chain_name)
                VALUES (stg.chain_key, stg.chain_name)
        """)
        chains_merged = cur.rowcount
    else:
        chains_merged = 0
    # no mid-flow commit here — single commit at the end

    # ── Store dimension ──────────────────────────────────────────────────────
    stores_df = df_valid.drop_duplicates(
        subset=["STORE_NAME_CLEAN","STORE_ADDRESS_LANE_1_CLEAN","STORE_CITY_CLEAN"]
    )[[
        "STORE_NAME_CLEAN","STORE_ADDRESS_LANE_1_CLEAN","STORE_ADDRESS_LANE_2_CLEAN",
        "STORE_CITY_CLEAN","STORE_ZIP_CLEAN","STORE_STATE_CLEAN",
        "STORE_CLASS_OF_TRADE_CLEAN","IS_CHAIN_CLEAN","CHAIN_NAME_CLEAN",
    ]]

    cur.execute("""
        SELECT store_key, store_name, store_address_1, store_address_2,
               store_city, store_zip, store_state, store_class_of_trade,
               store_is_chain, chain_key
        FROM   dim_store_dw
    """)
    store_cache = {}
    for r in cur.fetchall():
        bk = (r[1], r[2], r[4])   # name, address1, city
        store_cache[bk] = {
            'store_key':          r[0],
            'store_address_2':    r[3],
            'store_zip':          r[5],
            'store_state':        r[6],
            'store_class_of_trade': r[7],
            'store_is_chain':     r[8],
            'chain_key':          r[9],
        }

    cur.execute("SELECT NVL(MAX(store_key), 0) FROM dim_store_dw")
    next_store_key = cur.fetchone()[0] + 1

    insert_stores = []
    update_stores = []

    for _, row in stores_df.iterrows():
        bk        = (row["STORE_NAME_CLEAN"], row["STORE_ADDRESS_LANE_1_CLEAN"], row["STORE_CITY_CLEAN"])
        chain_key = chain_cache.get(row["CHAIN_NAME_CLEAN"]) if row["IS_CHAIN_CLEAN"] == "Y" else None

        if bk not in store_cache:
            insert_stores.append({'store_key': next_store_key, 'bk': bk,
                                   'data': row, 'chain_key': chain_key})
            next_store_key += 1
        else:
            ex = store_cache[bk]
            if (
                (row["STORE_ADDRESS_LANE_2_CLEAN"] or '') != (ex['store_address_2'] or '') or
                (row["STORE_ZIP_CLEAN"] or '')             != (ex['store_zip'] or '') or
                row["STORE_STATE_CLEAN"]                   != ex['store_state'] or
                row["STORE_CLASS_OF_TRADE_CLEAN"]          != ex['store_class_of_trade'] or
                row["IS_CHAIN_CLEAN"]                      != ex['store_is_chain'] or
                chain_key                                  != ex['chain_key']
            ):
                update_stores.append({'store_key': ex['store_key'], 'bk': bk,
                                       'data': row, 'chain_key': chain_key})

    staging_data = []
    for rec in insert_stores + update_stores:
        row = rec['data']
        staging_data.append((
            rec['store_key'],
            row["STORE_NAME_CLEAN"][:50],
            row["STORE_ADDRESS_LANE_1_CLEAN"][:100],
            row["STORE_ADDRESS_LANE_2_CLEAN"][:100] if row["STORE_ADDRESS_LANE_2_CLEAN"] else None,
            row["STORE_CITY_CLEAN"][:25],
            row["STORE_ZIP_CLEAN"][:10] if row["STORE_ZIP_CLEAN"] else None,
            row["STORE_STATE_CLEAN"][:50],
            row["STORE_CLASS_OF_TRADE_CLEAN"][:50],
            row["IS_CHAIN_CLEAN"],
            rec['chain_key'],
            'INSERT' if rec in insert_stores else 'UPDATE',
        ))

    stores_merged = 0
    if staging_data:
        cur.executemany("""
            INSERT INTO store_staging (
                store_key, store_name, store_address_1, store_address_2,
                store_city, store_zip, store_state, store_class_of_trade,
                store_is_chain, chain_key, operation
            ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
        """, staging_data)

        cur.execute("""
            MERGE INTO dim_store_dw tgt
            USING store_staging stg
            ON (tgt.store_key = stg.store_key)
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.store_address_2      = stg.store_address_2,
                    tgt.store_zip            = stg.store_zip,
                    tgt.store_state          = stg.store_state,
                    tgt.store_class_of_trade = stg.store_class_of_trade,
                    tgt.store_is_chain       = stg.store_is_chain,
                    tgt.chain_key            = stg.chain_key
            WHEN NOT MATCHED THEN
                INSERT (
                    store_key, store_name, store_address_1, store_address_2,
                    store_city, store_zip, store_state, store_class_of_trade,
                    store_is_chain, chain_key
                )
                VALUES (
                    stg.store_key, stg.store_name, stg.store_address_1, stg.store_address_2,
                    stg.store_city, stg.store_zip, stg.store_state, stg.store_class_of_trade,
                    stg.store_is_chain, stg.chain_key
                )
        """)
        stores_merged = cur.rowcount

    #  single commit after ALL MERGEs complete
    conn.commit()

    # ── Cleanup staging ──────────────────────────────────────────────────────
    # each DROP in its own try/except
    for tbl in ("store_chain_staging", "store_staging"):
        try:
            cur.execute(f"DROP TABLE {tbl}")
        except Exception:
            pass
    try:
        conn.commit()
    except Exception:
        pass

    mark_file_processed(source_file)

    # ── Summary stats ────────────────────────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN store_is_chain = 'Y' THEN 1 ELSE 0 END),
               SUM(CASE WHEN store_is_chain = 'N' THEN 1 ELSE 0 END)
        FROM dim_store_dw
    """)
    s = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM dim_store_chain_dw")
    chain_total = cur.fetchone()[0]

    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"file: {source_file} | "
        f"chains merged: {chains_merged} | stores merged: {stores_merged} "
        f"(insert: {len(insert_stores)} update: {len(update_stores)}) | "
        f"DB stores: {s[0]} chain: {s[1]} independent: {s[2]} | "
        f"DB chains: {chain_total}"
    )

except Exception:
    # rollback uncommitted work before re-raising
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
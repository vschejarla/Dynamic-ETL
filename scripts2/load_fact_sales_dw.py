import sys
import os
import pandas as pd
import oracledb
from decimal import Decimal, InvalidOperation
from datetime import datetime

# ========================================
# CONFIG
# ========================================
INCOMING_DIR  = "/opt/airflow/data_extracts/incoming"
PROCESSED_LOG = "/opt/airflow/data_extracts/processed_files.log"

DB_CONFIG = {
    "user":     "target_dw",
    "password": "target_dw123",
    "dsn":      "host.docker.internal/orcl"
}

# ========================================
# FILE TRACKING
# FIX 1: original used 'filename in f.read().splitlines()' which fails because
# log lines are 'filename|timestamp' — the filename alone never matches.
# Fixed to use startswith() which correctly matches 'filename|timestamp' lines.
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
def normalize_key(value):
    if pd.isna(value) or value is None or value == '':
        return ""
    return str(value).strip().upper()

def clean_numeric(value):
    if pd.isna(value) or value is None or value == '':
        return None
    try:
        if isinstance(value, str):
            value = value.replace('₹','').replace('$','').replace(',','').strip()
        n = Decimal(str(value))
        return float(n) if n >= 0 else None
    except (ValueError, InvalidOperation):
        return None

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
     if f.startswith("sales_") and f.endswith(".csv")],
    reverse=True
)
if not all_files:
    print(f"No 'sales_*.csv' files found in {INCOMING_DIR}")
    sys.exit(0)

source_file = next((f for f in all_files if not is_file_processed(f)), None)
if source_file is None:
    print(f"All {len(all_files)} file(s) already processed — nothing to do")
    sys.exit(0)

file_path = os.path.join(INCOMING_DIR, source_file)

# ========================================
# READ FILE
# ========================================
try:
    df = pd.read_csv(
        file_path, delimiter="|", dtype=str,
        na_values=['', 'NULL', 'null', 'NA']
    )
except Exception as e:
    raise RuntimeError(f"Failed to read '{source_file}': {e}") from e

initial_count = len(df)
if initial_count == 0:
    mark_file_processed(source_file)
    print(f"Empty file '{source_file}' — marked as processed")
    sys.exit(0)

df.columns = df.columns.str.strip().str.upper()

# ========================================
# DATABASE
# ========================================
conn = oracledb.connect(**DB_CONFIG)
cur  = conn.cursor()

try:
    # ── Staging table ────────────────────────────────────────────────────────
    try:
        cur.execute("DROP TABLE fact_sales_staging")
    except Exception:
        pass

    cur.execute("""
        CREATE TABLE fact_sales_staging (
            sales_key       NUMBER,
            date_id         NUMBER,
            store_key       NUMBER,
            product_key     NUMBER,
            dist_key        NUMBER,
            quantity_sold   NUMBER,
            unit_price      NUMBER(10,2),
            gross_amount    NUMBER(12,2),
            discount_amount NUMBER(12,2),
            net_amount      NUMBER(12,2)
        )
    """)
    conn.commit()

    # ── Dimension caches ─────────────────────────────────────────────────────
    cur.execute("""
        SELECT store_key, store_name, store_address_1, store_city FROM dim_store_dw
    """)
    store_cache = {
        (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0]
        for r in cur.fetchall()
    }

    cur.execute("SELECT product_key, product_name, brand FROM dim_product_dw")
    product_rows  = cur.fetchall()
    product_cache = {(normalize_key(r[1]), normalize_key(r[2])): r[0] for r in product_rows}
    # Fallback: product name only (for rows with missing/mismatched brand)
    for r in product_rows:
        nk = normalize_key(r[1])
        if nk not in product_cache:
            product_cache[nk] = r[0]

    cur.execute("SELECT dist_key, dist_name, dist_city, dist_state FROM dim_distributor_dw")
    dist_cache = {
        (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0]
        for r in cur.fetchall()
    }

    cur.execute("SELECT date_id, full_date FROM dim_date_dw")
    date_cache = {pd.to_datetime(r[1]).date(): r[0] for r in cur.fetchall()}

    # Check each dim cache individually so we can report exactly which is empty.
    # Do NOT mark the file processed here — empty dims mean the dimension load
    # tasks haven't run yet.  Exit cleanly (0) so Airflow marks this task
    # SUCCESS and the file stays unprocessed, ready to be retried on the next
    # DAG run once the dimension tables have been populated.
    empty_dims = [name for name, cache in [
        ("dim_store_dw",       store_cache),
        ("dim_product_dw",     product_cache),
        ("dim_distributor_dw", dist_cache),
        ("dim_date_dw",        date_cache),
    ] if not cache]

    if empty_dims:
        try:
            cur.execute("DROP TABLE fact_sales_staging"); conn.commit()
        except Exception:
            pass
        print(f"SKIP: empty dimension table(s): {', '.join(empty_dims)} — "
              f"run dimension load tasks first. File '{source_file}' left unprocessed.")
        sys.exit(0)

    # ── Column mapping (with aliases) ────────────────────────────────────────
    REQUIRED_COLUMNS = {
        'STORE_NAME':          ['STORE_NAME','STORENAME'],
        'STORE_ADDRESS_LANE_1':['STORE_ADDRESS_LANE_1','STORE_ADDRESS','ADDRESS'],
        'STORE_CITY':          ['STORE_CITY','CITY'],
        'PRODUCT_NAME':        ['PRODUCT_NAME','PRODUCTNAME'],
        'BRAND':               ['BRAND','BRAND_NAME'],
        'DISTRIBUTOR_NAME':    ['DISTRIBUTOR_NAME','DIST_NAME'],
        'DISTRIBUTOR_CITY':    ['DISTRIBUTOR_CITY','DIST_CITY'],
        'DISTRIBUTOR_STATE':   ['DISTRIBUTOR_STATE','DIST_STATE'],
        'FULL_DATE':           ['FULL_DATE','SALE_DATE','TRANSACTION_DATE','DATE'],
        'QUANTITY_SOLD':       ['QUANTITY_SOLD','QUANTITY','QTY'],
        'UNIT_PRICE':          ['UNIT_PRICE','SALES_UNIT_PRICE','PRICE'],
        'GROSS_AMOUNT':        ['GROSS_AMOUNT','GROSS_SALES','TOTAL_AMOUNT'],
        'DISCOUNT_AMOUNT':     ['DISCOUNT_AMOUNT','DISCOUNT'],
        'NET_AMOUNT':          ['NET_AMOUNT','NET_SALES','NET'],
    }

    col_map     = {}
    missing_map = []
    for std, aliases in REQUIRED_COLUMNS.items():
        found = next((a for a in aliases if a in df.columns), None)
        if found:
            col_map[std] = found
        else:
            missing_map.append(std)

    if missing_map:
        try:
            cur.execute("DROP TABLE fact_sales_staging"); conn.commit()
        except Exception:
            pass
        mark_file_processed(source_file)
        raise RuntimeError(f"Missing columns {missing_map} in '{source_file}'")

    # ── Dimension key mapping ─────────────────────────────────────────────────
    missing_stats = {"stores": 0, "products": 0, "distributors": 0, "dates": 0}

    def map_store(row):
        k = (normalize_key(row[col_map['STORE_NAME']]),
             normalize_key(row[col_map['STORE_ADDRESS_LANE_1']]),
             normalize_key(row[col_map['STORE_CITY']]))
        v = store_cache.get(k)
        if v is None:
            missing_stats["stores"] += 1
        return v

    def map_product(row):
        exact = (normalize_key(row[col_map['PRODUCT_NAME']]),
                 normalize_key(row[col_map['BRAND']]))
        v = product_cache.get(exact) or product_cache.get(normalize_key(row[col_map['PRODUCT_NAME']]))
        if v is None:
            missing_stats["products"] += 1
        return v

    def map_dist(row):
        k = (normalize_key(row[col_map['DISTRIBUTOR_NAME']]),
             normalize_key(row[col_map['DISTRIBUTOR_CITY']]),
             normalize_key(row[col_map['DISTRIBUTOR_STATE']]))
        v = dist_cache.get(k)
        if v is None:
            missing_stats["distributors"] += 1
        return v

    def map_date(date_str):
        try:
            if pd.isna(date_str):
                missing_stats["dates"] += 1
                return None
            d = pd.to_datetime(str(date_str).strip(), errors='coerce')
            if pd.isna(d):
                missing_stats["dates"] += 1
                return None
            v = date_cache.get(d.date())
            if v is None:
                missing_stats["dates"] += 1
            return v
        except Exception:
            missing_stats["dates"] += 1
            return None

    df["STORE_KEY"]   = df.apply(map_store,   axis=1)
    df["PRODUCT_KEY"] = df.apply(map_product, axis=1)
    df["DIST_KEY"]    = df.apply(map_dist,    axis=1)
    df["DATE_ID"]     = df[col_map['FULL_DATE']].apply(map_date)

    df_valid = df.dropna(subset=["STORE_KEY","PRODUCT_KEY","DIST_KEY","DATE_ID"]).copy()
    if len(df_valid) == 0:
        try:
            cur.execute("DROP TABLE fact_sales_staging"); conn.commit()
        except Exception:
            pass
        # Do NOT mark file processed — zero key matches is a data/timing issue.
        # Leave the file unprocessed so the next DAG run retries it.
        print(f"SKIP: no rows survived dimension key mapping "
              f"(stores missed: {missing_stats['stores']}, "
              f"products: {missing_stats['products']}, "
              f"distributors: {missing_stats['distributors']}, "
              f"dates: {missing_stats['dates']}). "
              f"File '{source_file}' left unprocessed.")
        sys.exit(0)

    for col in ("STORE_KEY","PRODUCT_KEY","DIST_KEY","DATE_ID"):
        df_valid[col] = df_valid[col].astype(int)

    # ── Numeric cleaning ──────────────────────────────────────────────────────
    NUMERIC_COLS = ['QUANTITY_SOLD','UNIT_PRICE','GROSS_AMOUNT','DISCOUNT_AMOUNT','NET_AMOUNT']
    for std in NUMERIC_COLS:
        df_valid[f"{std}_CLEAN"] = df_valid[col_map[std]].apply(clean_numeric)

    df_valid = df_valid.dropna(subset=[f"{c}_CLEAN" for c in NUMERIC_COLS])
    if len(df_valid) == 0:
        try:
            cur.execute("DROP TABLE fact_sales_staging"); conn.commit()
        except Exception:
            pass
        # Do NOT mark processed — numeric failure may be a data quality issue
        # that gets corrected in a resent file. Leave unprocessed to retry.
        print(f"SKIP: no rows survived numeric validation in '{source_file}'. "
              f"File left unprocessed.")
        sys.exit(0)

    # ── Natural key + change detection ───────────────────────────────────────
    # Grain: one row per (date, store, product, distributor)
    df_valid["NATURAL_KEY"] = (
        df_valid["DATE_ID"].astype(str)    + "_" +
        df_valid["STORE_KEY"].astype(str)  + "_" +
        df_valid["PRODUCT_KEY"].astype(str)+ "_" +
        df_valid["DIST_KEY"].astype(str)
    )

    cur.execute("""
        SELECT sales_key,
               date_id||'_'||store_key||'_'||product_key||'_'||dist_key,
               quantity_sold, unit_price, gross_amount, discount_amount, net_amount
        FROM fact_sales_dw
    """)
    existing = {
        r[1]: {'sales_key': r[0], 'qty': r[2], 'price': r[3],
               'gross': r[4], 'disc': r[5], 'net': r[6]}
        for r in cur.fetchall()
    }

    TOL = 0.01
    insert_records = []
    update_records = []

    for _, row in df_valid.iterrows():
        nk = row['NATURAL_KEY']
        rec = {
            'date_id':    int(row['DATE_ID']),
            'store_key':  int(row['STORE_KEY']),
            'product_key':int(row['PRODUCT_KEY']),
            'dist_key':   int(row['DIST_KEY']),
            'qty':        int(row['QUANTITY_SOLD_CLEAN']),
            'price':      float(row['UNIT_PRICE_CLEAN']),
            'gross':      float(row['GROSS_AMOUNT_CLEAN']),
            'disc':       float(row['DISCOUNT_AMOUNT_CLEAN']),
            'net':        float(row['NET_AMOUNT_CLEAN']),
        }
        if nk not in existing:
            insert_records.append(rec)
        else:
            ex = existing[nk]
            # quantity is an integer — use exact equality; floats use tolerance
            if (
                rec['qty']   != ex['qty'] or
                abs(rec['price'] - ex['price']) > TOL or
                abs(rec['gross'] - ex['gross']) > TOL or
                abs(rec['disc']  - ex['disc'])  > TOL or
                abs(rec['net']   - ex['net'])   > TOL
            ):
                rec['sales_key'] = ex['sales_key']
                update_records.append(rec)

    # ── Assign sales keys to inserts ──────────────────────────────────────────
    if insert_records:
        cur.execute("SELECT NVL(MAX(sales_key), 0) FROM fact_sales_dw")
        next_key = cur.fetchone()[0] + 1
        for i, rec in enumerate(insert_records):
            rec['sales_key'] = next_key + i

    all_staging = insert_records + update_records
    if not all_staging:
        try:
            cur.execute("DROP TABLE fact_sales_staging"); conn.commit()
        except Exception:
            pass
        mark_file_processed(source_file)
        # Fall through to summary print
    else:
        staging_data = [
            (r['sales_key'], r['date_id'], r['store_key'], r['product_key'], r['dist_key'],
             r['qty'], r['price'], r['gross'], r['disc'], r['net'])
            for r in all_staging
        ]

        # load all batches then commit ONCE — no mid-loop commits
        BATCH_SIZE = 10000
        try:
            for i in range(0, len(staging_data), BATCH_SIZE):
                cur.executemany("""
                    INSERT INTO fact_sales_staging (
                        sales_key, date_id, store_key, product_key, dist_key,
                        quantity_sold, unit_price, gross_amount, discount_amount, net_amount
                    ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
                """, staging_data[i:i + BATCH_SIZE])
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        # ── MERGE ────────────────────────────────────────────────────────────
        cur.execute("""
            MERGE INTO fact_sales_dw tgt
            USING fact_sales_staging stg
            ON (
                tgt.date_id     = stg.date_id    AND
                tgt.store_key   = stg.store_key  AND
                tgt.product_key = stg.product_key AND
                tgt.dist_key    = stg.dist_key
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.quantity_sold   = stg.quantity_sold,
                    tgt.unit_price      = stg.unit_price,
                    tgt.gross_amount    = stg.gross_amount,
                    tgt.discount_amount = stg.discount_amount,
                    tgt.net_amount      = stg.net_amount
            WHEN NOT MATCHED THEN
                INSERT (
                    sales_key, date_id, store_key, product_key, dist_key,
                    quantity_sold, unit_price, gross_amount, discount_amount, net_amount
                )
                VALUES (
                    stg.sales_key, stg.date_id, stg.store_key, stg.product_key, stg.dist_key,
                    stg.quantity_sold, stg.unit_price, stg.gross_amount,
                    stg.discount_amount, stg.net_amount
                )
        """)
        rows_merged = cur.rowcount
        conn.commit()

        try:
            cur.execute("DROP TABLE fact_sales_staging"); conn.commit()
        except Exception:
            pass

        mark_file_processed(source_file)

    # ── Summary ───────────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*), SUM(net_amount) FROM fact_sales_dw")
    s = cur.fetchone()

    invalid_keys = sum(missing_stats.values())
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"file: {source_file} | "
        f"read: {initial_count:,} | valid: {len(df_valid):,} | "
        f"insert: {len(insert_records):,} | update: {len(update_records):,} | "
        f"key misses: {invalid_keys:,} | "
        f"DB total: {s[0]:,} | net sales: ₹{s[1]:,.2f}"
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
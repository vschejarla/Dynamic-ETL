import os
import pandas as pd
import oracledb
import re
from datetime import datetime

print("🏪 STORE DIMENSION INCREMENTAL LOAD")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ========================================
# CONFIG - CONSISTENT ACROSS ALL SCRIPTS
# ========================================
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"
PROCESSED_LOG = "/opt/airflow/data_extracts/processed_stores.log"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

# ========================================
# FILE TRACKING - NO FILE MOVEMENT
# ========================================
def is_file_processed(filename):
    """Check if file has been processed before"""
    if not os.path.exists(PROCESSED_LOG):
        return False
    with open(PROCESSED_LOG, 'r') as f:
        processed = f.read().splitlines()
    return any(line.startswith(filename) for line in processed)

def mark_file_processed(filename):
    """Mark file as processed"""
    os.makedirs(os.path.dirname(PROCESSED_LOG), exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(PROCESSED_LOG, 'a') as f:
        f.write(f"{filename}|{timestamp}\n")
# ========================================
# DATA CLEANING FUNCTIONS
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
    zip_code = re.sub(r'[^0-9]', '', str(val).strip())
    if len(zip_code) == 6:
        return zip_code
    elif len(zip_code) > 6:
        return zip_code[:6]
    return None

def clean_class_of_trade(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "General Trade"
    
    val_upper = str(val).strip().upper()
    
    cot_map = {
        "HYPERMARKET": "Modern Trade - Hypermarket",
        "SUPERMARKET": "Modern Trade - Supermarket",
        "KIRANA": "General Trade - Kirana",
        "GENERAL TRADE": "General Trade - Kirana",
        "CONVENIENCE": "Convenience Store",
        "WHOLESALE": "Cash & Carry - Wholesale",
        "RETAIL": "Retail"
    }
    
    for key, value in cot_map.items():
        if key in val_upper:
            return value
    
    return val.strip()

def clean_is_chain(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return 'N'
    val_upper = str(val).strip().upper()
    return 'Y' if val_upper in ['Y', 'YES', '1', 'TRUE'] else 'N'

def clean_chain_name(val, is_chain):
    if is_chain == 'N' or pd.isna(val) or val is None or str(val).strip() == '':
        return None
    return ' '.join(str(val).strip().split()).title()

# ========================================
# CHECK INCOMING DIRECTORY
# ========================================
if not os.path.exists(INCOMING_DIR):
    print(f"ℹ️  Incoming directory does not exist: {INCOMING_DIR}")
    print("✅ This is normal if initial load hasn't run yet")
    print("✅ Exiting gracefully - no data to process\n")
    exit(0)

# ========================================
# FIND UNPROCESSED FILES
# ========================================
all_files = sorted([f for f in os.listdir(INCOMING_DIR) if f.startswith("sales_") and f.endswith(".csv")], reverse=True)

if not all_files:
    print(f"ℹ️  No sales CSV files found in: {INCOMING_DIR}")
    print("✅ Exiting gracefully - no data to process\n")
    exit(0)

# Find first unprocessed file
source_file = None
for f in all_files:
    if not is_file_processed(f):
        source_file = f
        break

if source_file is None:
    print(f"ℹ️  All {len(all_files)} sales file(s) already processed for stores")
    print("✅ Exiting gracefully - no new data to process\n")
    exit(0)

file_path = os.path.join(INCOMING_DIR, source_file)
print(f"📄 Processing file: {source_file}")
print(f"📁 Location: {file_path}\n")

# ========================================
# READ AND CLEAN DATA
# ========================================
try:
    df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA'])
    print(f"📊 Initial rows: {len(df):,}")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    exit(1)

if len(df) == 0:
    print("⚠️  Empty file - marking as processed")
    mark_file_processed(source_file)
    print("✅ Exiting gracefully\n")
    exit(0)

df.columns = df.columns.str.strip().str.upper()

required_cols = [
    "STORE_NAME", "STORE_ADDRESS_LANE_1", "STORE_ADDRESS_LANE_2",
    "STORE_CITY", "STORE_ZIP", "STORE_STATE", "STORE_CLASS_OF_TRADE",
    "IS_CHAIN", "CHAIN_NAME"
]

missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    print(f"❌ Missing columns: {missing_cols}")
    print(f"⚠️  Marking file as processed to prevent retry loop")
    mark_file_processed(source_file)
    exit(1)

# Clean data
df["STORE_NAME_CLEAN"] = df["STORE_NAME"].apply(clean_store_name)
df["STORE_ADDRESS_LANE_1_CLEAN"] = df["STORE_ADDRESS_LANE_1"].apply(clean_address)
df["STORE_ADDRESS_LANE_2_CLEAN"] = df["STORE_ADDRESS_LANE_2"].apply(clean_address)
df["STORE_CITY_CLEAN"] = df["STORE_CITY"].apply(clean_city)
df["STORE_ZIP_CLEAN"] = df["STORE_ZIP"].apply(clean_zip)
df["STORE_STATE_CLEAN"] = df["STORE_STATE"].apply(clean_state)
df["STORE_CLASS_OF_TRADE_CLEAN"] = df["STORE_CLASS_OF_TRADE"].apply(clean_class_of_trade)
df["IS_CHAIN_CLEAN"] = df["IS_CHAIN"].apply(clean_is_chain)
df["CHAIN_NAME_CLEAN"] = df.apply(
    lambda row: clean_chain_name(row["CHAIN_NAME"], row["IS_CHAIN_CLEAN"]), 
    axis=1
)

# Validate chain logic
chain_without_name = df[(df["IS_CHAIN_CLEAN"] == "Y") & (df["CHAIN_NAME_CLEAN"].isna())]
if len(chain_without_name) > 0:
    print(f"⚠️  {len(chain_without_name)} chain stores without name - marking as independent")
    df.loc[chain_without_name.index, "IS_CHAIN_CLEAN"] = "N"

# Filter valid rows
df_valid = df[
    (df["STORE_NAME_CLEAN"].notna()) &
    (df["STORE_ADDRESS_LANE_1_CLEAN"].notna()) &
    (df["STORE_CITY_CLEAN"].notna()) &
    (df["STORE_STATE_CLEAN"].notna())
].copy()

print(f"✅ Valid rows: {len(df_valid):,}")
if len(df) > len(df_valid):
    print(f"⚠️  Invalid rows: {len(df) - len(df_valid):,}")

if len(df_valid) == 0:
    print("⚠️  No valid store records after cleaning")
    mark_file_processed(source_file)
    print("✅ File marked as processed - exiting gracefully\n")
    exit(0)

# ========================================
# CONNECT TO DATABASE
# ========================================
print("\n🔌 Connecting to database...")
conn = None
cur = None

try:
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected\n")
    
    # ========================================
    # CREATE STAGING TABLES
    # ========================================
    print("🏗️  Creating staging tables...")
    
    for table in ["store_chain_staging", "store_staging"]:
        try:
            cur.execute(f"DROP TABLE {table}")
        except:
            pass
    
    cur.execute("""
        CREATE TABLE store_chain_staging (
            chain_key NUMBER,
            chain_name VARCHAR2(50),
            operation VARCHAR2(10)
        )
    """)
    
    cur.execute("""
        CREATE TABLE store_staging (
            store_key NUMBER,
            store_name VARCHAR2(50),
            store_address_1 VARCHAR2(100),
            store_address_2 VARCHAR2(100),
            store_city VARCHAR2(25),
            store_zip VARCHAR2(10),
            store_state VARCHAR2(50),
            store_class_of_trade VARCHAR2(50),
            store_is_chain CHAR(1),
            chain_key NUMBER,
            operation VARCHAR2(10)
        )
    """)
    conn.commit()
    print("✅ Staging tables created\n")
    
    # ========================================
    # LOAD STORE CHAIN DIMENSION
    # ========================================
    print("="*60)
    print("STORE CHAIN DIMENSION")
    print("="*60)
    
    # Get unique chains from file
    chains_df = df_valid[["CHAIN_NAME_CLEAN"]].dropna().drop_duplicates()
    chains_df.columns = ["CHAIN_NAME"]
    
    print(f"📦 Unique chains in file: {len(chains_df)}")
    
    # Load existing chains
    cur.execute("SELECT chain_key, chain_name FROM dim_store_chain_dw")
    chain_cache = {r[1]: r[0] for r in cur.fetchall()}
    print(f"📦 Existing chains in DW: {len(chain_cache)}")
    
    # Find new chains
    new_chains = [c for c in chains_df["CHAIN_NAME"] if c not in chain_cache]
    print(f"   ➕ New chains: {len(new_chains)}")
    
    # Insert new chains
    if new_chains:
        cur.execute("SELECT NVL(MAX(chain_key),0) FROM dim_store_chain_dw")
        start_key = cur.fetchone()[0] + 1
        
        chain_data = []
        for i, chain in enumerate(new_chains):
            chain_key = start_key + i
            chain_data.append((chain_key, chain, 'INSERT'))
            chain_cache[chain] = chain_key
        
        cur.executemany(
            "INSERT INTO store_chain_staging (chain_key, chain_name, operation) VALUES (:1,:2,:3)",
            chain_data
        )
        
        cur.execute("""
            MERGE INTO dim_store_chain_dw tgt
            USING store_chain_staging stg
            ON (tgt.chain_key = stg.chain_key)
            WHEN NOT MATCHED THEN
                INSERT (chain_key, chain_name)
                VALUES (stg.chain_key, stg.chain_name)
        """)
        
        conn.commit()
        print(f"✅ Inserted {cur.rowcount} new chains\n")
    else:
        print("ℹ️  No new chains to insert\n")
    
    # ========================================
    # LOAD STORE DIMENSION
    # ========================================
    print("="*60)
    print("STORE DIMENSION")
    print("="*60)
    
    # Deduplicate stores
    stores_df = df_valid[[
        "STORE_NAME_CLEAN", "STORE_ADDRESS_LANE_1_CLEAN", "STORE_ADDRESS_LANE_2_CLEAN",
        "STORE_CITY_CLEAN", "STORE_ZIP_CLEAN", "STORE_STATE_CLEAN",
        "STORE_CLASS_OF_TRADE_CLEAN", "IS_CHAIN_CLEAN", "CHAIN_NAME_CLEAN"
    ]].drop_duplicates(subset=["STORE_NAME_CLEAN", "STORE_ADDRESS_LANE_1_CLEAN", "STORE_CITY_CLEAN"])
    
    print(f"🏪 Unique stores in file: {len(stores_df)}")
    
    # Load existing stores
    cur.execute("""
        SELECT 
            store_key, store_name, store_address_1, store_address_2,
            store_city, store_zip, store_state, store_class_of_trade,
            store_is_chain, chain_key
        FROM dim_store_dw
    """)
    
    store_cache = {}
    for r in cur.fetchall():
        bk = (r[1], r[2], r[4])  # name, address1, city
        store_cache[bk] = {
            'store_key': r[0], 'store_address_2': r[3], 'store_zip': r[5],
            'store_state': r[6], 'store_class_of_trade': r[7],
            'store_is_chain': r[8], 'chain_key': r[9]
        }
    
    print(f"🏪 Existing stores in DW: {len(store_cache)}")
    
    # Classify stores
    insert_stores = []
    update_stores = []
    
    for _, r in stores_df.iterrows():
        bk = (r["STORE_NAME_CLEAN"], r["STORE_ADDRESS_LANE_1_CLEAN"], r["STORE_CITY_CLEAN"])
        chain_key = chain_cache.get(r["CHAIN_NAME_CLEAN"]) if r["IS_CHAIN_CLEAN"] == "Y" else None
        
        if bk not in store_cache:
            insert_stores.append({'bk': bk, 'data': r, 'chain_key': chain_key})
        else:
            existing = store_cache[bk]
            if (
                (r["STORE_ADDRESS_LANE_2_CLEAN"] or '') != (existing['store_address_2'] or '') or
                (r["STORE_ZIP_CLEAN"] or '') != (existing['store_zip'] or '') or
                r["STORE_STATE_CLEAN"] != existing['store_state'] or
                r["STORE_CLASS_OF_TRADE_CLEAN"] != existing['store_class_of_trade'] or
                r["IS_CHAIN_CLEAN"] != existing['store_is_chain'] or
                chain_key != existing['chain_key']
            ):
                update_stores.append({
                    'store_key': existing['store_key'],
                    'bk': bk, 'data': r, 'chain_key': chain_key
                })
    
    print(f"   ➕ New stores: {len(insert_stores)}")
    print(f"   🔄 Updates: {len(update_stores)}")
    
    # Generate keys and prepare data
    if insert_stores:
        cur.execute("SELECT NVL(MAX(store_key),0) FROM dim_store_dw")
        start_key = cur.fetchone()[0] + 1
        for i, store in enumerate(insert_stores):
            store['store_key'] = start_key + i
    
    staging_data = []
    for store in insert_stores + update_stores:
        r = store['data']
        staging_data.append((
            store['store_key'],
            r["STORE_NAME_CLEAN"][:50],
            r["STORE_ADDRESS_LANE_1_CLEAN"][:100],
            r["STORE_ADDRESS_LANE_2_CLEAN"][:100] if r["STORE_ADDRESS_LANE_2_CLEAN"] else None,
            r["STORE_CITY_CLEAN"][:25],
            r["STORE_ZIP_CLEAN"][:10] if r["STORE_ZIP_CLEAN"] else None,
            r["STORE_STATE_CLEAN"][:50],
            r["STORE_CLASS_OF_TRADE_CLEAN"][:50],
            r["IS_CHAIN_CLEAN"],
            store['chain_key'],
            'INSERT' if store in insert_stores else 'UPDATE'
        ))
    
    # Load and merge
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
                    tgt.store_address_2 = stg.store_address_2,
                    tgt.store_zip = stg.store_zip,
                    tgt.store_state = stg.store_state,
                    tgt.store_class_of_trade = stg.store_class_of_trade,
                    tgt.store_is_chain = stg.store_is_chain,
                    tgt.chain_key = stg.chain_key
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
        
        conn.commit()
        print(f"✅ Merged {cur.rowcount} stores\n")
    else:
        print("ℹ️  No changes detected - all data already current\n")
    
    # Cleanup
    print("🧹 Cleaning up...")
    cur.execute("DROP TABLE store_chain_staging")
    cur.execute("DROP TABLE store_staging")
    conn.commit()
    
    # Mark as processed
    mark_file_processed(source_file)
    print(f"✅ File marked as processed\n")
    
    # ========================================
    # STATISTICS
    # ========================================
    print("="*60)
    print("FINAL STATISTICS")
    print("="*60)
    
    cur.execute("SELECT COUNT(*) FROM dim_store_chain_dw")
    print(f"📊 Total chains: {cur.fetchone()[0]}")
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN store_is_chain = 'Y' THEN 1 ELSE 0 END) as chain,
            SUM(CASE WHEN store_is_chain = 'N' THEN 1 ELSE 0 END) as independent
        FROM dim_store_dw
    """)
    stats = cur.fetchone()
    print(f"📊 Total stores: {stats[0]}")
    print(f"   Chain: {stats[1]}, Independent: {stats[2]}")

except Exception as e:
    print(f"❌ Error: {e}")
    raise

finally:
    if cur:
        try:
            cur.close()
        except:
            pass
    if conn:
        try:
            conn.close()
        except:
            pass

print(f"\n🎉 Load completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
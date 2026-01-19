import os
import pandas as pd
import oracledb
import re

print("🏪 STORE DIMENSION LOAD STARTED")

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

# ---------------------------------------------------
# DATA QUALITY & NORMALIZATION HELPERS
# ---------------------------------------------------
def normalize(val):
    """Normalize text values"""
    if pd.isna(val) or val is None or val == '':
        return None
    return str(val).strip().upper()

def clean_store_name(val):
    """Clean and standardize store names"""
    if pd.isna(val) or val is None or val == '':
        return "UNKNOWN STORE"
    
    # Remove extra spaces
    name = ' '.join(str(val).strip().split())
    return name.upper()

def clean_address(val):
    """Clean address fields"""
    if pd.isna(val) or val is None or val == '':
        return None
    
    # Remove extra spaces and normalize
    addr = ' '.join(str(val).strip().split())
    return addr.upper()

def clean_city(val):
    """Clean city names"""
    if pd.isna(val) or val is None or val == '':
        return "UNKNOWN"
    
    city = str(val).strip().title()  # Title case for cities
    return city

def clean_state(val):
    """Clean state names"""
    if pd.isna(val) or val is None or val == '':
        return "UNKNOWN"
    
    state = str(val).strip().title()  # Title case for states
    return state

def clean_zip(val):
    """Clean and validate PIN codes"""
    if pd.isna(val) or val is None or val == '':
        return None
    
    # Remove spaces and non-digits
    zip_code = re.sub(r'[^0-9]', '', str(val).strip())
    
    # Indian PIN codes are 6 digits
    if len(zip_code) == 6:
        return zip_code
    elif len(zip_code) > 6:
        return zip_code[:6]  # Take first 6 digits
    else:
        return None

def clean_class_of_trade(val):
    """Standardize class of trade"""
    if pd.isna(val) or val is None or val == '':
        return "General Trade"
    
    val_upper = str(val).strip().upper()
    
    # Standardization mapping
    cot_map = {
        "HYPERMARKET": "Modern Trade - Hypermarket",
        "HYPER MARKET": "Modern Trade - Hypermarket",
        "MODERN TRADE HYPERMARKET": "Modern Trade - Hypermarket",
        
        "SUPERMARKET": "Modern Trade - Supermarket",
        "SUPER MARKET": "Modern Trade - Supermarket",
        "MODERN TRADE SUPERMARKET": "Modern Trade - Supermarket",
        
        "KIRANA": "General Trade - Kirana",
        "GENERAL TRADE": "General Trade - Kirana",
        "GENERAL TRADE KIRANA": "General Trade - Kirana",
        
        "CONVENIENCE": "Convenience Store",
        "CONVENIENCE STORE": "Convenience Store",
        
        "WHOLESALE": "Cash & Carry - Wholesale",
        "CASH & CARRY": "Cash & Carry - Wholesale",
        "CASH AND CARRY": "Cash & Carry - Wholesale"
    }
    
    # Direct match
    if val_upper in cot_map:
        return cot_map[val_upper]
    
    # Partial match
    for key, value in cot_map.items():
        if key in val_upper:
            return value
    
    return val.strip()

def clean_is_chain(val):
    """Validate and clean chain flag"""
    if pd.isna(val) or val is None or val == '':
        return 'N'
    
    val_upper = str(val).strip().upper()
    
    if val_upper in ['Y', 'YES', '1', 'TRUE']:
        return 'Y'
    else:
        return 'N'

def clean_chain_name(val, is_chain):
    """Clean chain name with business logic"""
    if is_chain == 'N':
        return None
    
    if pd.isna(val) or val is None or val == '':
        return None
    
    # Clean and normalize
    chain = ' '.join(str(val).strip().split())
    return chain.title()  # Title case for chain names

# ---------------------------------------------------
# 1️⃣ READ LATEST FILE
# ---------------------------------------------------
files = sorted(
    [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")],
    reverse=True
)

if not files:
    raise FileNotFoundError("❌ No incoming files found")

latest_file = os.path.join(INCOMING_DIR, files[0])
print(f"📄 Reading: {files[0]}")

# Read with dtype=str to preserve all values
df = pd.read_csv(latest_file, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA'])

print(f"📊 Initial rows: {len(df)}")

# ---------------------------------------------------
# 2️⃣ CLEAN & NORMALIZE STORE DATA
# ---------------------------------------------------
print("\n🧹 Cleaning and normalizing data...")

# Apply cleaning functions
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

# Data quality validation
print("\n📋 Data Quality Checks:")

# Check for required fields
required_fields = ["STORE_NAME_CLEAN", "STORE_ADDRESS_LANE_1_CLEAN", "STORE_CITY_CLEAN", "STORE_STATE_CLEAN"]
for field in required_fields:
    null_count = df[field].isna().sum()
    if null_count > 0:
        print(f"   ⚠️  {field}: {null_count} null values")

# Validate chain logic
chain_stores = df[df["IS_CHAIN_CLEAN"] == "Y"]
chain_without_name = chain_stores[chain_stores["CHAIN_NAME_CLEAN"].isna()]
if len(chain_without_name) > 0:
    print(f"   ⚠️  {len(chain_without_name)} chain stores without chain name - setting to independent")
    df.loc[chain_without_name.index, "IS_CHAIN_CLEAN"] = "N"

# Remove completely invalid rows
df_valid = df[
    df["STORE_NAME_CLEAN"].notna() &
    df["STORE_ADDRESS_LANE_1_CLEAN"].notna() &
    df["STORE_CITY_CLEAN"].notna() &
    df["STORE_STATE_CLEAN"].notna()
].copy()

print(f"✅ Valid rows: {len(df_valid)}")
print(f"❌ Invalid rows removed: {len(df) - len(df_valid)}")

# ---------------------------------------------------
# 3️⃣ CONNECT TO ORACLE
# ---------------------------------------------------
print("\n🔌 Connecting to Oracle...")
conn = oracledb.connect(**DB_CONFIG)
cur = conn.cursor()
print("✅ Connected successfully")

# ===================================================
# 🟡 PART A: STORE CHAIN DIMENSION
# ===================================================
print("\n" + "="*60)
print("PART A: LOADING STORE CHAIN DIMENSION")
print("="*60)

# 4️⃣ UNIQUE CHAIN NAMES FROM FILE
chains_df = (
    df_valid[["CHAIN_NAME_CLEAN"]]
    .dropna()
    .drop_duplicates()
    .rename(columns={"CHAIN_NAME_CLEAN": "CHAIN_NAME"})
)

print(f"📦 Unique chains in file: {len(chains_df)}")

# 5️⃣ FETCH EXISTING CHAINS
cur.execute("SELECT chain_key, chain_name FROM dim_store_chain_dw")
chain_cache = {r[1]: r[0] for r in cur.fetchall()}
print(f"📦 Existing chains in DW: {len(chain_cache)}")

# 6️⃣ INSERT NEW CHAINS
new_chains = [
    c for c in chains_df["CHAIN_NAME"]
    if c not in chain_cache
]

if new_chains:
    cur.execute("SELECT NVL(MAX(chain_key),0) FROM dim_store_chain_dw")
    start_key = cur.fetchone()[0]

    chain_rows = []
    for i, chain in enumerate(new_chains, start=1):
        chain_key = start_key + i
        chain_rows.append((chain_key, chain))
        chain_cache[chain] = chain_key
    
    try:
        cur.executemany(
            "INSERT INTO dim_store_chain_dw (chain_key, chain_name) VALUES (:1,:2)",
            chain_rows
        )
        conn.commit()
        print(f"✅ Inserted {len(chain_rows)} new store chains")
        
        # Display new chains
        for _, chain_name in chain_rows[:5]:  # Show first 5
            print(f"   → {chain_name}")
        if len(chain_rows) > 5:
            print(f"   ... and {len(chain_rows) - 5} more")
    
    except Exception as e:
        conn.rollback()
        print(f"❌ Error inserting chains: {e}")
        raise
else:
    print("ℹ️  No new chains to insert")

# ===================================================
# 🟡 PART B: STORE DIMENSION
# ===================================================
print("\n" + "="*60)
print("PART B: LOADING STORE DIMENSION")
print("="*60)

# 7️⃣ DEDUP STORES
store_cols = [
    "STORE_NAME_CLEAN",
    "STORE_ADDRESS_LANE_1_CLEAN",
    "STORE_ADDRESS_LANE_2_CLEAN",
    "STORE_CITY_CLEAN",
    "STORE_ZIP_CLEAN",
    "STORE_STATE_CLEAN",
    "STORE_CLASS_OF_TRADE_CLEAN",
    "IS_CHAIN_CLEAN",
    "CHAIN_NAME_CLEAN"
]

stores_df = df_valid[store_cols].drop_duplicates(
    subset=["STORE_NAME_CLEAN", "STORE_ADDRESS_LANE_1_CLEAN", "STORE_CITY_CLEAN"]
)

print(f"🏪 Unique stores in file: {len(stores_df)}")

# 8️⃣ FETCH EXISTING STORES
cur.execute("""
    SELECT store_key, store_name, store_address_1, store_city, store_zip
    FROM dim_store_dw
""")

store_cache = {
    (r[1], r[2], r[3]): r[0]  # Business key: name, address1, city
    for r in cur.fetchall()
}

print(f"🏪 Existing stores in DW: {len(store_cache)}")

# 9️⃣ IDENTIFY NEW STORES
new_stores = []

for _, r in stores_df.iterrows():
    bk = (
        r["STORE_NAME_CLEAN"],
        r["STORE_ADDRESS_LANE_1_CLEAN"],
        r["STORE_CITY_CLEAN"]
    )
    if bk not in store_cache:
        new_stores.append(r)

print(f"🆕 New stores to insert: {len(new_stores)}")

# 🔟 INSERT NEW STORES
if new_stores:
    cur.execute("SELECT NVL(MAX(store_key),0) FROM dim_store_dw")
    start_key = cur.fetchone()[0]

    insert_sql = """
        INSERT INTO dim_store_dw (
            store_key,
            store_name,
            store_address_1,
            store_address_2,
            store_city,
            store_zip,
            store_state,
            store_class_of_trade,
            store_is_chain,
            chain_key
        )
        VALUES (
            :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
        )
    """

    data = []
    insert_errors = 0
    
    for i, r in enumerate(new_stores, start=1):
        try:
            store_key = start_key + i

            # Get chain_key if store is part of a chain
            chain_key = None
            if r["IS_CHAIN_CLEAN"] == "Y" and r["CHAIN_NAME_CLEAN"]:
                chain_key = chain_cache.get(r["CHAIN_NAME_CLEAN"])

            data.append((
                store_key,
                r["STORE_NAME_CLEAN"][:50],        # Respect VARCHAR2(50)
                r["STORE_ADDRESS_LANE_1_CLEAN"][:100],  # VARCHAR2(100)
                r["STORE_ADDRESS_LANE_2_CLEAN"][:100] if r["STORE_ADDRESS_LANE_2_CLEAN"] else None,
                r["STORE_CITY_CLEAN"][:25],        # VARCHAR2(25)
                r["STORE_ZIP_CLEAN"][:10] if r["STORE_ZIP_CLEAN"] else None,
                r["STORE_STATE_CLEAN"][:50],       # VARCHAR2(50)
                r["STORE_CLASS_OF_TRADE_CLEAN"][:50],  # VARCHAR2(50)
                r["IS_CHAIN_CLEAN"],
                chain_key
            ))

            # Update cache
            store_cache[
                (
                    r["STORE_NAME_CLEAN"],
                    r["STORE_ADDRESS_LANE_1_CLEAN"],
                    r["STORE_CITY_CLEAN"]
                )
            ] = store_key
        
        except Exception as e:
            insert_errors += 1
            print(f"   ⚠️  Error preparing store {i}: {e}")
            continue

    # Bulk insert
    if data:
        try:
            cur.executemany(insert_sql, data)
            conn.commit()
            print(f"✅ Inserted {len(data)} new stores")
            
            # Display sample stores
            for row in data[:5]:  # Show first 5
                store_name = row[1]
                city = row[4]
                store_type = row[7]
                is_chain = "Chain" if row[8] == 'Y' else "Independent"
                print(f"   → {store_name} | {city} | {store_type} | {is_chain}")
            
            if len(data) > 5:
                print(f"   ... and {len(data) - 5} more")
            
            if insert_errors > 0:
                print(f"   ⚠️  {insert_errors} stores had errors during preparation")
        
        except Exception as e:
            conn.rollback()
            print(f"❌ Error inserting stores: {e}")
            raise
    else:
        print("⚠️  No valid store data to insert")
else:
    print("ℹ️  No new stores to insert")

# ===================================================
# FINAL STATISTICS
# ===================================================
print("\n" + "="*60)
print("LOAD STATISTICS")
print("="*60)

# Chain statistics
cur.execute("""
    SELECT COUNT(*) as total_chains
    FROM dim_store_chain_dw
""")
total_chains = cur.fetchone()[0]
print(f"📊 Total chains in DW: {total_chains}")

# Store statistics
cur.execute("""
    SELECT 
        COUNT(*) as total_stores,
        SUM(CASE WHEN store_is_chain = 'Y' THEN 1 ELSE 0 END) as chain_stores,
        SUM(CASE WHEN store_is_chain = 'N' THEN 1 ELSE 0 END) as independent_stores
    FROM dim_store_dw
""")
stats = cur.fetchone()
print(f"📊 Total stores in DW: {stats[0]}")
print(f"   Chain stores: {stats[1]}")
print(f"   Independent stores: {stats[2]}")

# Store by class of trade
cur.execute("""
    SELECT store_class_of_trade, COUNT(*) as cnt
    FROM dim_store_dw
    GROUP BY store_class_of_trade
    ORDER BY cnt DESC
""")
print(f"\n📊 Stores by Class of Trade:")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

cur.close()
conn.close()

print("\n" + "="*60)
print("🎉 STORE DIMENSION LOAD COMPLETED SUCCESSFULLY")
print("="*60)
import os
import pandas as pd
import oracledb

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

DB_CONFIG = {
    "user": "system",
    "password": "905966Sh@r4107",
    "dsn": "host.docker.internal/orcl"
}

# ---------------------------------------------------
# HELPER
# ---------------------------------------------------
def normalize(val):
    if pd.isna(val):
        return None
    return str(val).strip().upper()

# ---------------------------------------------------
# 1️⃣ READ LATEST FILE
# ---------------------------------------------------
files = sorted(
    [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")],
    reverse=True
)

if not files:
    raise FileNotFoundError("❌ No incoming files")

latest_file = os.path.join(INCOMING_DIR, files[0])
print(f"📄 Reading: {latest_file}")

df = pd.read_csv(latest_file, delimiter="|")

# ---------------------------------------------------
# 2️⃣ NORMALIZE STORE DATA
# ---------------------------------------------------
store_cols = [
    "STORE_NAME",
    "STORE_ADDRESS_LANE_1",
    "STORE_ADDRESS_LANE_2",
    "STORE_CITY",
    "STORE_ZIP",
    "STORE_STATE",
    "STORE_CLASS_OF_TRADE",
    "IS_CHAIN",
    "CHAIN_NAME"
]

for col in store_cols:
    df[col] = df[col].apply(normalize)

# ---------------------------------------------------
# 3️⃣ CONNECT TO ORACLE
# ---------------------------------------------------
conn = oracledb.connect(**DB_CONFIG)
cur = conn.cursor()

# ===================================================
# 🟡 PART A: STORE CHAIN DIMENSION
# ===================================================

# 4️⃣ UNIQUE CHAIN NAMES FROM FILE
chains_df = (
    df[["CHAIN_NAME"]]
    .dropna()
    .drop_duplicates()
)

# 5️⃣ FETCH EXISTING CHAINS
cur.execute("SELECT chain_key, chain_name FROM dim_store_chain_dw")

chain_cache = {r[1]: r[0] for r in cur.fetchall()}
print(f"📦 Existing chains: {len(chain_cache)}")

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

    cur.executemany(
        "INSERT INTO dim_store_chain_dw (chain_key, chain_name) VALUES (:1,:2)",
        chain_rows
    )
    conn.commit()
    print(f"✅ Inserted {len(chain_rows)} new store chains")

# ===================================================
# 🟡 PART B: STORE DIMENSION
# ===================================================

# 7️⃣ DEDUP STORES
stores_df = df[store_cols].drop_duplicates()

# 8️⃣ FETCH EXISTING STORES
cur.execute("""
    SELECT store_key, store_name, store_address_1, store_city, store_zip
    FROM dim_store_dw
""")

store_cache = {
    (r[1], r[2], r[3], r[4]): r[0]
    for r in cur.fetchall()
}

# 9️⃣ IDENTIFY NEW STORES
new_stores = []

for _, r in stores_df.iterrows():
    bk = (
        r["STORE_NAME"],
        r["STORE_ADDRESS_LANE_1"],
        r["STORE_CITY"],
        r["STORE_ZIP"]
    )
    if bk not in store_cache:
        new_stores.append(r)

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
    for i, r in enumerate(new_stores, start=1):
        store_key = start_key + i

        chain_key = (
            chain_cache.get(r["CHAIN_NAME"])
            if r["IS_CHAIN"] == "Y"
            else None
        )

        data.append((
            store_key,
            r["STORE_NAME"],
            r["STORE_ADDRESS_LANE_1"],
            r["STORE_ADDRESS_LANE_2"],
            r["STORE_CITY"],
            r["STORE_ZIP"],
            r["STORE_STATE"],
            r["STORE_CLASS_OF_TRADE"],
            r["IS_CHAIN"],
            chain_key
        ))

        store_cache[
            (
                r["STORE_NAME"],
                r["STORE_ADDRESS_LANE_1"],
                r["STORE_CITY"],
                r["STORE_ZIP"]
            )
        ] = store_key

    cur.executemany(insert_sql, data)
    conn.commit()
    print(f"✅ Inserted {len(data)} new stores")

cur.close()
conn.close()
print("🎉 DIM_STORE_DW + DIM_STORE_CHAIN_DW LOADED SUCCESSFULLY")
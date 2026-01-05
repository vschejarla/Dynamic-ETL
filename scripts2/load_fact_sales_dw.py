import os
from datetime import datetime

import pandas as pd
import oracledb

# -----------------------------------
# CONFIG
# -----------------------------------

INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl",
}

# -----------------------------------
# 1️⃣ READ LATEST FILE
# -----------------------------------

files = sorted(
    [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")],
    reverse=True,
)

if not files:
    raise FileNotFoundError("❌ No incoming files found")

source_file = files[0]
file_path = os.path.join(INCOMING_DIR, source_file)
print(f"📄 Loading FACT file: {file_path}")

df = pd.read_csv(file_path, delimiter="|")
initial_count = len(df)
print(f"📊 Initial record count: {initial_count}")

# Print actual column names for debugging
print(f"📋 CSV Columns: {list(df.columns)}")

# Normalize column names to uppercase and strip whitespace
df.columns = df.columns.str.strip().str.upper()

# -----------------------------------
# 2️⃣ CONNECT TO ORACLE
# -----------------------------------

conn = oracledb.connect(**DB_CONFIG)
cur = conn.cursor()

# -----------------------------------
# 3️⃣ LOAD DIMENSION CACHES WITH NORMALIZATION
# -----------------------------------

print("🔄 Loading dimension caches...")

# Helper function to normalize strings for matching
def normalize_key(value):
    """Normalize string values for consistent matching."""
    if pd.isna(value):
        return ""
    return str(value).strip().upper()

# Store dimension - normalize all key fields
cur.execute(
    """
    SELECT store_key, store_name, store_address_1, store_zip
    FROM dim_store_dw
    """
)
store_cache = {
    (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0] 
    for r in cur.fetchall()
}
print(f"✅ Stores loaded: {len(store_cache)}")

# Product dimension - normalize all key fields
cur.execute(
    """
    SELECT product_key, product_name, brand
    FROM dim_product_dw
    """
)
product_cache = {
    (normalize_key(r[1]), normalize_key(r[2])): r[0] 
    for r in cur.fetchall()
}
print(f"✅ Products loaded: {len(product_cache)}")

# Distributor dimension - normalize all key fields
cur.execute(
    """
    SELECT dist_key, dist_name, dist_city, dist_state
    FROM dim_distributor_dw
    """
)
dist_cache = {
    (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0] 
    for r in cur.fetchall()
}
print(f"✅ Distributors loaded: {len(dist_cache)}")

# Date dimension
cur.execute(
    """
    SELECT date_id, full_date
    FROM dim_date_dw
    """
)
date_cache = {
    pd.to_datetime(r[1]).date(): r[0]
    for r in cur.fetchall()
}
print(f"✅ Dates loaded: {len(date_cache)}")

if date_cache:
    min_date = min(date_cache.keys())
    max_date = max(date_cache.keys())
    print(f"📅 Date range: {min_date} to {max_date}")

# -----------------------------------
# 4️⃣ MAP SURROGATE KEYS WITH ERROR TRACKING
# -----------------------------------

missing_stats = {
    "stores": set(),
    "products": set(),
    "distributors": set(),
    "dates": set(),
}

missing_examples = {
    "stores": [],
    "products": [],
    "distributors": [],
    "dates": [],
}


def safe_lookup(cache, key, cache_name, original_values=None):
    """Safely lookup a key in cache, return None if not found."""
    if key in cache:
        return cache[key]
    missing_stats[cache_name].add(str(key))
    if original_values and len(missing_examples[cache_name]) < 5:
        missing_examples[cache_name].append(original_values)
    return None


# Normalize fact data before lookup
df["STORE_KEY"] = df.apply(
    lambda x: safe_lookup(
        store_cache,
        (
            normalize_key(x["STORE_NAME"]), 
            normalize_key(x["STORE_ADDRESS_LANE_1"]), 
            normalize_key(x["STORE_ZIP"])
        ),
        "stores",
        {
            "name": x["STORE_NAME"],
            "address": x["STORE_ADDRESS_LANE_1"],
            "zip": x["STORE_ZIP"]
        }
    ),
    axis=1,
)

df["PRODUCT_KEY"] = df.apply(
    lambda x: safe_lookup(
        product_cache,
        (normalize_key(x["PRODUCT_NAME"]), normalize_key(x["BRAND"])),
        "products",
        {"name": x["PRODUCT_NAME"], "brand": x["BRAND"]}
    ),
    axis=1,
)

df["DIST_KEY"] = df.apply(
    lambda x: safe_lookup(
        dist_cache,
        (
            normalize_key(x["DISTRIBUTOR_NAME"]), 
            normalize_key(x["DISTRIBUTOR_CITY"]), 
            normalize_key(x["DISTRIBUTOR_STATE"])
        ),
        "distributors",
        {
            "name": x["DISTRIBUTOR_NAME"],
            "city": x["DISTRIBUTOR_CITY"],
            "state": x["DISTRIBUTOR_STATE"]
        }
    ),
    axis=1,
)


def safe_date_lookup(date_str):
    """Handle date lookup with error handling."""
    try:
        date_obj = pd.to_datetime(date_str).date()
        if date_obj in date_cache:
            return date_cache[date_obj]
        missing_stats["dates"].add(str(date_obj))
        if len(missing_examples["dates"]) < 5:
            missing_examples["dates"].append(str(date_obj))
        return None
    except Exception as e:
        print(f"⚠️ Invalid date format: {date_str} - {str(e)}")
        return None


df["DATE_ID"] = df["FULL_DATE"].apply(safe_date_lookup)

# -----------------------------------
# 5️⃣ REPORT MISSING DIMENSION KEYS WITH EXAMPLES
# -----------------------------------

print("\n📋 Missing Dimension Keys Summary:")
print(f"❌ Missing Stores: {len(missing_stats['stores'])}")
if missing_examples["stores"]:
    print("   Sample missing store records:")
    for i, example in enumerate(missing_examples["stores"][:3], 1):
        print(f"   {i}. Name: '{example['name']}', Address: '{example['address']}', Zip: '{example['zip']}'")

print(f"❌ Missing Products: {len(missing_stats['products'])}")
if missing_examples["products"]:
    print("   Sample missing product records:")
    for i, example in enumerate(missing_examples["products"][:3], 1):
        print(f"   {i}. Name: '{example['name']}', Brand: '{example['brand']}'")

print(f"❌ Missing Distributors: {len(missing_stats['distributors'])}")
if missing_examples["distributors"]:
    print("   Sample missing distributor records:")
    for i, example in enumerate(missing_examples["distributors"][:3], 1):
        print(f"   {i}. Name: '{example['name']}', City: '{example['city']}', State: '{example['state']}'")

print(f"❌ Missing Dates: {len(missing_stats['dates'])}")
if missing_examples["dates"]:
    print("   Missing dates:")
    for date in missing_examples["dates"][:5]:
        print(f"   - {date}")

# -----------------------------------
# 6️⃣ FILTER OUT ROWS WITH MISSING KEYS
# -----------------------------------

before_filter = len(df)
df_valid = df.dropna(subset=["STORE_KEY", "PRODUCT_KEY", "DIST_KEY", "DATE_ID"])
after_filter = len(df_valid)

rows_filtered = before_filter - after_filter
if rows_filtered > 0:
    print(
        f"\n⚠️ Filtered out {rows_filtered} rows "
        f"({100 * rows_filtered / before_filter:.1f}%) due to missing dimension keys"
    )

if len(df_valid) == 0:
    print("\n❌ CRITICAL: No valid rows to load after filtering!")
    print("\n🔍 ACTION REQUIRED:")
    print("1. Check the sample records above to identify the mismatch")
    print("2. Verify dimension load scripts are using the same normalization")
    print("3. Ensure dimension tables were loaded from the correct source files")
    print("4. Check for extra spaces, different casing, or special characters")
    print("5. Run this query to see what's in your dimension tables:")
    print("   SELECT COUNT(*), MIN(store_name), MAX(store_name) FROM dim_store_dw;")
    cur.close()
    conn.close()
    exit(1)

df_valid = df_valid.copy()
df_valid["STORE_KEY"] = df_valid["STORE_KEY"].astype(int)
df_valid["PRODUCT_KEY"] = df_valid["PRODUCT_KEY"].astype(int)
df_valid["DIST_KEY"] = df_valid["DIST_KEY"].astype(int)
df_valid["DATE_ID"] = df_valid["DATE_ID"].astype(int)

# -----------------------------------
# 7️⃣ CLEAN NUMERIC COLUMNS
# -----------------------------------

# Map possible column name variations
column_mappings = {
    "QUANTITY_SOLD": ["QUANTITY_SOLD", "QUANTITY", "QTY_SOLD", "QTY"],
    "UNIT_PRICE": ["SALES_UNIT_PRICE", "UNIT_PRICE", "UNITPRICE", "PRICE", "UNIT_COST"],
    "GROSS_AMOUNT": ["GROSS_AMOUNT", "GROSS_SALES", "GROSS", "TOTAL_AMOUNT"],
    "DISCOUNT_AMOUNT": ["DISCOUNT_AMOUNT", "DISCOUNT", "DISC_AMOUNT"],
    "NET_AMOUNT": ["NET_AMOUNT", "NET_SALES", "NET", "NET_TOTAL"],
}

# Find actual column names in the dataframe
numeric_cols = []
for standard_name, possible_names in column_mappings.items():
    found = False
    for possible_name in possible_names:
        if possible_name in df_valid.columns:
            numeric_cols.append(possible_name)
            found = True
            break
    if not found:
        print(f"⚠️ Warning: Could not find column for {standard_name}")
        print(f"   Available columns: {list(df_valid.columns)}")

if not numeric_cols:
    print("❌ ERROR: No numeric columns found!")
    print(f"Available columns in CSV: {list(df_valid.columns)}")
    cur.close()
    conn.close()
    exit(1)

print(f"✅ Found numeric columns: {numeric_cols}")

for col in numeric_cols:
    df_valid[col] = pd.to_numeric(df_valid[col], errors="coerce")

before_numeric_clean = len(df_valid)
df_valid = df_valid.dropna(subset=numeric_cols)
after_numeric_clean = len(df_valid)

if before_numeric_clean > after_numeric_clean:
    print(
        f"⚠️ Filtered out {before_numeric_clean - after_numeric_clean} "
        f"rows due to invalid numeric values"
    )

print(f"\n✅ Valid records after all cleaning: {len(df_valid)}")

# -----------------------------------
# 8️⃣ GENERATE SALES_KEY
# -----------------------------------

cur.execute("SELECT NVL(MAX(sales_key), 0) FROM fact_sales_dw")
start_key = cur.fetchone()[0]
df_valid["SALES_KEY"] = range(start_key + 1, start_key + 1 + len(df_valid))

# -----------------------------------
# 9️⃣ AUDIT COLUMNS (optional)
# -----------------------------------

# df_valid["LOAD_DATE"] = datetime.now()
# df_valid["SOURCE_FILE"] = source_file

# -----------------------------------
# 🔟 INSERT INTO FACT
# -----------------------------------

# Build column list dynamically based on what we found
select_cols = ["SALES_KEY", "DATE_ID", "STORE_KEY", "PRODUCT_KEY", "DIST_KEY"]

# Add numeric columns in the correct order for the INSERT statement
for standard_name in ["QUANTITY_SOLD", "UNIT_PRICE", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT"]:
    for possible_name in column_mappings[standard_name]:
        if possible_name in df_valid.columns:
            select_cols.append(possible_name)
            break

print(f"📝 Inserting columns: {select_cols}")

rows = df_valid[select_cols].values.tolist()

try:
    cur.executemany(
        """
        INSERT INTO fact_sales_dw (
            sales_key,
            date_id,
            store_key,
            product_key,
            dist_key,
            quantity_sold,
            unit_price,
            gross_amount,
            discount_amount,
            net_amount
        ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
        """,
        rows,
    )
    conn.commit()
    print(f"\n🎉 FACT_SALES_DW loaded successfully: {len(rows)} rows inserted")
    print(
        f"📈 Success rate: {len(rows)}/{initial_count} "
        f"({100 * len(rows) / initial_count:.1f}%)"
    )
except Exception as e:
    conn.rollback()
    print(f"\n❌ Error inserting rows: {str(e)}")
    raise
finally:
    cur.close()
    conn.close()
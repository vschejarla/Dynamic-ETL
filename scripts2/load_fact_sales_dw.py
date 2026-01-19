import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import oracledb

print("💰 FACT_SALES_DW LOAD STARTED")

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
# HELPER FUNCTIONS
# -----------------------------------

def normalize_key(value):
    """Normalize string values for consistent matching."""
    if pd.isna(value) or value is None or value == '':
        return ""
    return str(value).strip().upper()

def clean_numeric(value):
    """Clean and validate numeric values."""
    if pd.isna(value) or value is None or value == '':
        return None
    
    try:
        # Remove currency symbols, commas
        if isinstance(value, str):
            value = value.replace('₹', '').replace('$', '').replace(',', '').strip()
        
        # Convert to Decimal for precision
        numeric_val = Decimal(str(value))
        
        # Validate positive values for sales data
        if numeric_val < 0:
            return None
        
        return float(numeric_val)
    except (ValueError, InvalidOperation):
        return None

def validate_business_logic(row):
    """Validate business logic rules."""
    warnings = []
    
    # Rule 1: Gross Amount = Quantity * Unit Price (with tolerance)
    expected_gross = row['QUANTITY_SOLD_CLEAN'] * row['UNIT_PRICE_CLEAN']
    tolerance = 0.10  # 10 cents tolerance for rounding
    if abs(row['GROSS_AMOUNT_CLEAN'] - expected_gross) > tolerance:
        warnings.append(f"Gross mismatch: {row['GROSS_AMOUNT_CLEAN']} vs expected {expected_gross:.2f}")
    
    # Rule 2: Net Amount = Gross Amount - Discount Amount
    expected_net = row['GROSS_AMOUNT_CLEAN'] - row['DISCOUNT_AMOUNT_CLEAN']
    if abs(row['NET_AMOUNT_CLEAN'] - expected_net) > tolerance:
        warnings.append(f"Net mismatch: {row['NET_AMOUNT_CLEAN']} vs expected {expected_net:.2f}")
    
    # Rule 3: Discount should not exceed Gross Amount
    if row['DISCOUNT_AMOUNT_CLEAN'] > row['GROSS_AMOUNT_CLEAN']:
        warnings.append(f"Discount exceeds gross: {row['DISCOUNT_AMOUNT_CLEAN']} > {row['GROSS_AMOUNT_CLEAN']}")
    
    # Rule 4: Reasonable quantity range (1 to 1000 for retail)
    if row['QUANTITY_SOLD_CLEAN'] <= 0 or row['QUANTITY_SOLD_CLEAN'] > 10000:
        warnings.append(f"Unusual quantity: {row['QUANTITY_SOLD_CLEAN']}")
    
    # Rule 5: Reasonable unit price range (₹1 to ₹100,000)
    if row['UNIT_PRICE_CLEAN'] <= 0 or row['UNIT_PRICE_CLEAN'] > 100000:
        warnings.append(f"Unusual unit price: {row['UNIT_PRICE_CLEAN']}")
    
    return warnings

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
print(f"\n📄 Loading FACT file: {source_file}")

# Read with dtype=str to preserve all values
df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA'])
initial_count = len(df)
print(f"📊 Initial record count: {initial_count:,}")

# Normalize column names
df.columns = df.columns.str.strip().str.upper()
print(f"📋 CSV Columns: {list(df.columns)}")

# -----------------------------------
# 2️⃣ CONNECT TO ORACLE
# -----------------------------------

print("\n🔌 Connecting to Oracle...")
conn = oracledb.connect(**DB_CONFIG)
cur = conn.cursor()
print("✅ Connected successfully")

# -----------------------------------
# 3️⃣ LOAD DIMENSION CACHES
# -----------------------------------

print("\n🔄 Loading dimension caches...")

# Store dimension
cur.execute("""
    SELECT store_key, store_name, store_address_1, store_city, store_zip
    FROM dim_store_dw
""")
store_cache = {
    (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0] 
    for r in cur.fetchall()
}
print(f"✅ Stores loaded: {len(store_cache):,}")

# Product dimension
cur.execute("""
    SELECT product_key, product_name, brand
    FROM dim_product_dw
""")
product_cache = {
    (normalize_key(r[1]), normalize_key(r[2])): r[0] 
    for r in cur.fetchall()
}
print(f"✅ Products loaded: {len(product_cache):,}")

# Distributor dimension
cur.execute("""
    SELECT dist_key, dist_name, dist_city, dist_state
    FROM dim_distributor_dw
""")
dist_cache = {
    (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0] 
    for r in cur.fetchall()
}
print(f"✅ Distributors loaded: {len(dist_cache):,}")

# Date dimension
cur.execute("""
    SELECT date_id, full_date
    FROM dim_date_dw
""")
date_cache = {
    pd.to_datetime(r[1]).date(): r[0]
    for r in cur.fetchall()
}
print(f"✅ Dates loaded: {len(date_cache):,}")

if date_cache:
    min_date = min(date_cache.keys())
    max_date = max(date_cache.keys())
    print(f"📅 Date range: {min_date} to {max_date}")

# Check if dimension tables are populated
if not store_cache:
    print("\n❌ CRITICAL: dim_store_dw is empty! Load store dimension first.")
    cur.close()
    conn.close()
    exit(1)

if not product_cache:
    print("\n❌ CRITICAL: dim_product_dw is empty! Load product dimension first.")
    cur.close()
    conn.close()
    exit(1)

if not dist_cache:
    print("\n❌ CRITICAL: dim_distributor_dw is empty! Load distributor dimension first.")
    cur.close()
    conn.close()
    exit(1)

if not date_cache:
    print("\n❌ CRITICAL: dim_date_dw is empty! Load date dimension first.")
    cur.close()
    conn.close()
    exit(1)

# -----------------------------------
# 4️⃣ VALIDATE REQUIRED COLUMNS
# -----------------------------------

print("\n🔍 Validating required columns...")

required_columns = {
    'STORE_NAME': ['STORE_NAME', 'STORENAME'],
    'STORE_ADDRESS_LANE_1': ['STORE_ADDRESS_LANE_1', 'STORE_ADDRESS', 'ADDRESS'],
    'STORE_CITY': ['STORE_CITY', 'CITY'],
    'STORE_ZIP': ['STORE_ZIP', 'ZIP', 'PINCODE', 'PIN'],
    'PRODUCT_NAME': ['PRODUCT_NAME', 'PRODUCTNAME'],
    'BRAND': ['BRAND', 'BRAND_NAME'],
    'DISTRIBUTOR_NAME': ['DISTRIBUTOR_NAME', 'DIST_NAME'],
    'DISTRIBUTOR_CITY': ['DISTRIBUTOR_CITY', 'DIST_CITY'],
    'DISTRIBUTOR_STATE': ['DISTRIBUTOR_STATE', 'DIST_STATE'],
    'FULL_DATE': ['FULL_DATE', 'SALE_DATE', 'TRANSACTION_DATE', 'DATE'],
    'QUANTITY_SOLD': ['QUANTITY_SOLD', 'QUANTITY', 'QTY', 'QTY_SOLD'],
    'UNIT_PRICE': ['UNIT_PRICE', 'SALES_UNIT_PRICE', 'PRICE', 'UNITPRICE'],
    'GROSS_AMOUNT': ['GROSS_AMOUNT', 'GROSS_SALES', 'GROSS', 'TOTAL_AMOUNT'],
    'DISCOUNT_AMOUNT': ['DISCOUNT_AMOUNT', 'DISCOUNT', 'DISC_AMOUNT'],
    'NET_AMOUNT': ['NET_AMOUNT', 'NET_SALES', 'NET', 'NET_TOTAL']
}

# Map columns
column_mapping = {}
missing_columns = []

for standard_name, possible_names in required_columns.items():
    found = False
    for possible_name in possible_names:
        if possible_name in df.columns:
            column_mapping[standard_name] = possible_name
            found = True
            break
    if not found:
        missing_columns.append(standard_name)

if missing_columns:
    print(f"\n❌ CRITICAL: Missing required columns: {missing_columns}")
    print(f"Available columns: {list(df.columns)}")
    cur.close()
    conn.close()
    exit(1)

print(f"✅ All required columns found")
for std, actual in column_mapping.items():
    if std != actual:
        print(f"   {std} → {actual}")

# -----------------------------------
# 5️⃣ MAP SURROGATE KEYS
# -----------------------------------

print("\n🔗 Mapping surrogate keys...")

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

# Map Store Key
df["STORE_KEY"] = df.apply(
    lambda x: safe_lookup(
        store_cache,
        (
            normalize_key(x[column_mapping['STORE_NAME']]), 
            normalize_key(x[column_mapping['STORE_ADDRESS_LANE_1']]), 
            normalize_key(x[column_mapping['STORE_CITY']])
        ),
        "stores",
        {
            "name": x[column_mapping['STORE_NAME']],
            "address": x[column_mapping['STORE_ADDRESS_LANE_1']],
            "city": x[column_mapping['STORE_CITY']]
        }
    ),
    axis=1,
)

# Map Product Key
df["PRODUCT_KEY"] = df.apply(
    lambda x: safe_lookup(
        product_cache,
        (
            normalize_key(x[column_mapping['PRODUCT_NAME']]), 
            normalize_key(x[column_mapping['BRAND']])
        ),
        "products",
        {
            "name": x[column_mapping['PRODUCT_NAME']], 
            "brand": x[column_mapping['BRAND']]
        }
    ),
    axis=1,
)

# Map Distributor Key
df["DIST_KEY"] = df.apply(
    lambda x: safe_lookup(
        dist_cache,
        (
            normalize_key(x[column_mapping['DISTRIBUTOR_NAME']]), 
            normalize_key(x[column_mapping['DISTRIBUTOR_CITY']]), 
            normalize_key(x[column_mapping['DISTRIBUTOR_STATE']])
        ),
        "distributors",
        {
            "name": x[column_mapping['DISTRIBUTOR_NAME']],
            "city": x[column_mapping['DISTRIBUTOR_CITY']],
            "state": x[column_mapping['DISTRIBUTOR_STATE']]
        }
    ),
    axis=1,
)

# Map Date Key
def safe_date_lookup(date_str):
    """Handle date lookup with error handling."""
    try:
        date_obj = pd.to_datetime(date_str, errors='coerce')
        if pd.isna(date_obj):
            return None
        date_key = date_obj.date()
        if date_key in date_cache:
            return date_cache[date_key]
        missing_stats["dates"].add(str(date_key))
        if len(missing_examples["dates"]) < 5:
            missing_examples["dates"].append(str(date_key))
        return None
    except Exception as e:
        return None

df["DATE_ID"] = df[column_mapping['FULL_DATE']].apply(safe_date_lookup)

# -----------------------------------
# 6️⃣ REPORT MISSING DIMENSION KEYS
# -----------------------------------

print("\n📋 Missing Dimension Keys Summary:")
print(f"❌ Missing Stores: {len(missing_stats['stores']):,}")
if missing_examples["stores"]:
    print("   Sample missing store records:")
    for i, example in enumerate(missing_examples["stores"][:3], 1):
        print(f"   {i}. Name: '{example['name']}', Address: '{example['address']}', City: '{example['city']}'")

print(f"❌ Missing Products: {len(missing_stats['products']):,}")
if missing_examples["products"]:
    print("   Sample missing product records:")
    for i, example in enumerate(missing_examples["products"][:3], 1):
        print(f"   {i}. Name: '{example['name']}', Brand: '{example['brand']}'")

print(f"❌ Missing Distributors: {len(missing_stats['distributors']):,}")
if missing_examples["distributors"]:
    print("   Sample missing distributor records:")
    for i, example in enumerate(missing_examples["distributors"][:3], 1):
        print(f"   {i}. Name: '{example['name']}', City: '{example['city']}', State: '{example['state']}'")

print(f"❌ Missing Dates: {len(missing_stats['dates']):,}")
if missing_examples["dates"]:
    print("   Missing dates:")
    for date in missing_examples["dates"][:5]:
        print(f"   - {date}")

# -----------------------------------
# 7️⃣ FILTER ROWS WITH MISSING KEYS
# -----------------------------------

before_filter = len(df)
df_valid = df.dropna(subset=["STORE_KEY", "PRODUCT_KEY", "DIST_KEY", "DATE_ID"]).copy()
after_filter = len(df_valid)

rows_filtered = before_filter - after_filter
if rows_filtered > 0:
    print(f"\n⚠️  Filtered out {rows_filtered:,} rows ({100 * rows_filtered / before_filter:.1f}%) due to missing dimension keys")

if len(df_valid) == 0:
    print("\n❌ CRITICAL: No valid rows to load after filtering!")
    print("\n🔍 ACTION REQUIRED:")
    print("1. Verify dimension tables were loaded correctly")
    print("2. Check normalization consistency across dimension loads")
    print("3. Verify source file has matching dimension values")
    cur.close()
    conn.close()
    exit(1)

# Convert keys to int
df_valid["STORE_KEY"] = df_valid["STORE_KEY"].astype(int)
df_valid["PRODUCT_KEY"] = df_valid["PRODUCT_KEY"].astype(int)
df_valid["DIST_KEY"] = df_valid["DIST_KEY"].astype(int)
df_valid["DATE_ID"] = df_valid["DATE_ID"].astype(int)

# -----------------------------------
# 8️⃣ CLEAN & VALIDATE NUMERIC COLUMNS
# -----------------------------------

print("\n🧹 Cleaning numeric columns...")

numeric_columns = {
    'QUANTITY_SOLD': column_mapping['QUANTITY_SOLD'],
    'UNIT_PRICE': column_mapping['UNIT_PRICE'],
    'GROSS_AMOUNT': column_mapping['GROSS_AMOUNT'],
    'DISCOUNT_AMOUNT': column_mapping['DISCOUNT_AMOUNT'],
    'NET_AMOUNT': column_mapping['NET_AMOUNT']
}

# Clean numeric values
for std_name, actual_col in numeric_columns.items():
    df_valid[f"{std_name}_CLEAN"] = df_valid[actual_col].apply(clean_numeric)

# Filter out rows with invalid numeric values
before_numeric = len(df_valid)
df_valid = df_valid.dropna(subset=[f"{col}_CLEAN" for col in numeric_columns.keys()])
after_numeric = len(df_valid)

if before_numeric > after_numeric:
    print(f"⚠️  Filtered out {before_numeric - after_numeric:,} rows due to invalid numeric values")

print(f"✅ Valid records after numeric cleaning: {len(df_valid):,}")

# -----------------------------------
# 9️⃣ BUSINESS LOGIC VALIDATION
# -----------------------------------

print("\n✓ Validating business logic...")

business_warnings = []
for idx, row in df_valid.iterrows():
    warnings = validate_business_logic(row)
    if warnings:
        business_warnings.extend(warnings)
        if len(business_warnings) <= 5:  # Show first 5
            for warning in warnings:
                print(f"   ⚠️  Row {idx}: {warning}")

if len(business_warnings) > 5:
    print(f"   ... and {len(business_warnings) - 5} more business logic warnings")

# Optional: Filter out rows with critical business logic issues
# For now, we'll just warn but proceed with loading

# -----------------------------------
# 🔟 GENERATE SALES_KEY
# -----------------------------------

print("\n🔑 Generating sales keys...")

cur.execute("SELECT NVL(MAX(sales_key), 0) FROM fact_sales_dw")
start_key = cur.fetchone()[0]
df_valid["SALES_KEY"] = range(start_key + 1, start_key + 1 + len(df_valid))

print(f"   Sales key range: {start_key + 1:,} to {start_key + len(df_valid):,}")

# -----------------------------------
# ⓫ PREPARE DATA FOR INSERT
# -----------------------------------

print("\n📦 Preparing data for insert...")

# Select columns in the correct order
insert_data = df_valid[[
    "SALES_KEY",
    "DATE_ID",
    "STORE_KEY",
    "PRODUCT_KEY",
    "DIST_KEY",
    "QUANTITY_SOLD_CLEAN",
    "UNIT_PRICE_CLEAN",
    "GROSS_AMOUNT_CLEAN",
    "DISCOUNT_AMOUNT_CLEAN",
    "NET_AMOUNT_CLEAN"
]].values.tolist()

# Convert to proper types
prepared_rows = []
for row in insert_data:
    prepared_rows.append((
        int(row[0]),              # sales_key
        int(row[1]),              # date_id
        int(row[2]),              # store_key
        int(row[3]),              # product_key
        int(row[4]),              # dist_key
        int(row[5]),              # quantity_sold
        float(row[6]),            # unit_price
        float(row[7]),            # gross_amount
        float(row[8]),            # discount_amount
        float(row[9])             # net_amount
    ))

# -----------------------------------
# ⓬ BULK INSERT WITH BATCH PROCESSING
# -----------------------------------

print(f"\n💾 Inserting {len(prepared_rows):,} rows into fact_sales_dw...")

BATCH_SIZE = 10000
total_inserted = 0
total_batches = (len(prepared_rows) + BATCH_SIZE - 1) // BATCH_SIZE

try:
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, len(prepared_rows))
        batch = prepared_rows[start_idx:end_idx]
        
        cur.executemany("""
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
        """, batch)
        
        conn.commit()
        total_inserted += len(batch)
        print(f"   ✅ Batch {batch_num + 1}/{total_batches}: {len(batch):,} rows (Total: {total_inserted:,})")
    
    print(f"\n🎉 FACT_SALES_DW LOAD COMPLETED SUCCESSFULLY")
    print(f"📊 Load Summary:")
    print(f"   Source file: {source_file}")
    print(f"   Initial records: {initial_count:,}")
    print(f"   Successfully loaded: {total_inserted:,}")
    print(f"   Success rate: {100 * total_inserted / initial_count:.1f}%")
    print(f"   Records filtered: {initial_count - total_inserted:,}")

except Exception as e:
    conn.rollback()
    print(f"\n❌ Error inserting rows: {str(e)}")
    raise

# -----------------------------------
# ⓭ FINAL STATISTICS
# -----------------------------------

print("\n" + "="*60)
print("FACT TABLE STATISTICS")
print("="*60)

# Total records
cur.execute("SELECT COUNT(*), SUM(net_amount) FROM fact_sales_dw")
stats = cur.fetchone()
print(f"📊 Total records in fact_sales_dw: {stats[0]:,}")
print(f"💰 Total net sales: ₹{stats[1]:,.2f}")

# Sales by date (top 10)
cur.execute("""
    SELECT d.full_date, SUM(f.net_amount) as total_sales, COUNT(*) as txn_count
    FROM fact_sales_dw f
    JOIN dim_date_dw d ON f.date_id = d.date_id
    GROUP BY d.full_date
    ORDER BY d.full_date DESC
    FETCH FIRST 10 ROWS ONLY
""")
print(f"\n📅 Recent Daily Sales:")
for row in cur.fetchall():
    print(f"   {row[0].strftime('%Y-%m-%d')}: ₹{row[1]:,.2f} ({row[2]:,} transactions)")

# Sales by store type (if available)
try:
    cur.execute("""
        SELECT s.store_class_of_trade, 
               COUNT(*) as txn_count,
               SUM(f.net_amount) as total_sales
        FROM fact_sales_dw f
        JOIN dim_store_dw s ON f.store_key = s.store_key
        GROUP BY s.store_class_of_trade
        ORDER BY total_sales DESC
    """)
    print(f"\n🏪 Sales by Store Type:")
    for row in cur.fetchall():
        print(f"   {row[0]}: ₹{row[2]:,.2f} ({row[1]:,} transactions)")
except:
    pass  # store_class_of_trade might not exist

cur.close()
conn.close()

print("\n" + "="*60)
print("🎉 FACT SALES LOAD PROCESS COMPLETED")
print("="*60)
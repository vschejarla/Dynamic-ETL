import os
import pandas as pd
import oracledb
from decimal import Decimal, InvalidOperation
from datetime import datetime

print("💰 FACT SALES INCREMENTAL LOAD")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ========================================
# CONFIG - CONSISTENT ACROSS ALL SCRIPTS
# ========================================
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"
PROCESSED_LOG = "/opt/airflow/data_extracts/processed_files.log"

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
        return filename in f.read().splitlines()

def mark_file_processed(filename):
    """Mark file as processed in log"""
    os.makedirs(os.path.dirname(PROCESSED_LOG), exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(PROCESSED_LOG, 'a') as f:
        f.write(f"{filename}|{timestamp}\n")

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
            value = value.replace('₹', '').replace('$', '').replace(',', '').strip()
        numeric_val = Decimal(str(value))
        if numeric_val < 0:
            return None
        return float(numeric_val)
    except (ValueError, InvalidOperation):
        return None

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
    print(f"ℹ️  All {len(all_files)} sales file(s) already processed")
    print("✅ Exiting gracefully - no new data to process\n")
    exit(0)

file_path = os.path.join(INCOMING_DIR, source_file)
print(f"📄 Processing file: {source_file}")
print(f"📁 Location: {file_path}\n")

# ========================================
# READ DATA
# ========================================
try:
    df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA'])
    initial_count = len(df)
    print(f"📊 Initial records: {initial_count:,}")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    exit(1)

if initial_count == 0:
    print("⚠️  Empty file - marking as processed")
    mark_file_processed(source_file)
    print("✅ Exiting gracefully\n")
    exit(0)

df.columns = df.columns.str.strip().str.upper()

# ========================================
# CONNECT TO DATABASE
# ========================================
print("\n🔌 Connecting to database...")
try:
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

try:
    # ========================================
    # CREATE STAGING TABLE
    # ========================================
    print("🏗️  Creating staging table...")
    
    try:
        cur.execute("DROP TABLE fact_sales_staging")
    except:
        pass
    
    cur.execute("""
        CREATE TABLE fact_sales_staging (
            sales_key NUMBER,
            date_id NUMBER,
            store_key NUMBER,
            product_key NUMBER,
            dist_key NUMBER,
            quantity_sold NUMBER,
            unit_price NUMBER(10,2),
            gross_amount NUMBER(12,2),
            discount_amount NUMBER(12,2),
            net_amount NUMBER(12,2)
        )
    """)
    conn.commit()
    print("✅ Staging table created\n")
    
    # ========================================
    # LOAD DIMENSION CACHES
    # ========================================
    print("🔄 Loading dimension caches...")
    
    # Store cache
    cur.execute("""
        SELECT store_key, store_name, store_address_1, store_city
        FROM dim_store_dw
    """)
    store_cache = {
        (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0]
        for r in cur.fetchall()
    }
    print(f"✅ Stores: {len(store_cache):,}")
    
    # Product cache
    cur.execute("""
        SELECT product_key, product_name, brand
        FROM dim_product_dw
    """)
    product_list = cur.fetchall()
    product_cache = {
        (normalize_key(r[1]), normalize_key(r[2])): r[0]
        for r in product_list
    }
    # Add fallback: product name only (for missing brands)
    for r in product_list:
        name_only_key = normalize_key(r[1])
        if name_only_key not in product_cache:
            product_cache[name_only_key] = r[0]
    print(f"✅ Products: {len(product_cache):,}")
    
    # Distributor cache
    cur.execute("""
        SELECT dist_key, dist_name, dist_city, dist_state
        FROM dim_distributor_dw
    """)
    dist_cache = {
        (normalize_key(r[1]), normalize_key(r[2]), normalize_key(r[3])): r[0]
        for r in cur.fetchall()
    }
    print(f"✅ Distributors: {len(dist_cache):,}")
    
    # Date cache
    cur.execute("""
        SELECT date_id, full_date
        FROM dim_date_dw
    """)
    date_cache = {
        pd.to_datetime(r[1]).date(): r[0]
        for r in cur.fetchall()
    }
    print(f"✅ Dates: {len(date_cache):,}")
    
    if not all([store_cache, product_cache, dist_cache, date_cache]):
        print("\n❌ CRITICAL: One or more dimension tables are empty!")
        print("   Load all dimension tables before loading facts")
        try:
            cur.execute("DROP TABLE fact_sales_staging")
            conn.commit()
        except:
            pass
        raise Exception("Dimension tables are empty - load dimensions first")
    
    # ========================================
    # VALIDATE COLUMNS
    # ========================================
    print("\n🔍 Validating columns...")
    print(f"📋 CSV has {len(df.columns)} columns:")
    for i, col in enumerate(df.columns[:20]):  # Show first 20 columns
        print(f"   {i}: {col}")
    if len(df.columns) > 20:
        print(f"   ... and {len(df.columns) - 20} more")
    print()
    
    required_columns = {
        'STORE_NAME': ['STORE_NAME', 'STORENAME'],
        'STORE_ADDRESS_LANE_1': ['STORE_ADDRESS_LANE_1', 'STORE_ADDRESS', 'ADDRESS'],
        'STORE_CITY': ['STORE_CITY', 'CITY'],
        'PRODUCT_NAME': ['PRODUCT_NAME', 'PRODUCTNAME'],
        'BRAND': ['BRAND', 'BRAND_NAME'],
        'DISTRIBUTOR_NAME': ['DISTRIBUTOR_NAME', 'DIST_NAME'],
        'DISTRIBUTOR_CITY': ['DISTRIBUTOR_CITY', 'DIST_CITY'],
        'DISTRIBUTOR_STATE': ['DISTRIBUTOR_STATE', 'DIST_STATE'],
        'FULL_DATE': ['FULL_DATE', 'SALE_DATE', 'TRANSACTION_DATE', 'DATE'],
        'QUANTITY_SOLD': ['QUANTITY_SOLD', 'QUANTITY', 'QTY'],
        'UNIT_PRICE': ['UNIT_PRICE', 'SALES_UNIT_PRICE', 'PRICE'],
        'GROSS_AMOUNT': ['GROSS_AMOUNT', 'GROSS_SALES', 'TOTAL_AMOUNT'],
        'DISCOUNT_AMOUNT': ['DISCOUNT_AMOUNT', 'DISCOUNT'],
        'NET_AMOUNT': ['NET_AMOUNT', 'NET_SALES', 'NET']
    }
    
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
        print(f"❌ Missing columns: {missing_columns}")
        print(f"⚠️  Marking file as processed to prevent retry loop")
        cur.execute("DROP TABLE fact_sales_staging")
        conn.commit()
        mark_file_processed(source_file)
        cur.close()
        conn.close()
        exit(1)
    
    print(f"✅ All required columns found")
    print(f"\n📌 Column Mapping Used:")
    for standard_name, actual_col in sorted(column_mapping.items()):
        sample_val = str(df[actual_col].iloc[0])[:30] if len(df) > 0 else "N/A"
        print(f"   {standard_name:20} ← {actual_col:25} (sample: {sample_val})")
    
    # ========================================
    # VALIDATE COLUMN CONTENT
    # ========================================
    print("🔍 Validating column contents...")
    
    # Check if date values are in product columns (data quality check)
    if 'FULL_DATE' in column_mapping:
        date_col = column_mapping['FULL_DATE']
        if 'PRODUCT_NAME' in column_mapping:
            prod_col = column_mapping['PRODUCT_NAME']
            # Check if product_name contains date patterns (YYYY-MM-DD)
            date_like = df[prod_col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').sum()
            if date_like > 0:
                print(f"⚠️  WARNING: {date_like} product_name values look like dates!")
                print(f"   This suggests CSV column misalignment")
                print(f"   product_name column: {prod_col}")
                print(f"   date column: {date_col}")
        if 'BRAND' in column_mapping:
            brand_col = column_mapping['BRAND']
            date_like = df[brand_col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').sum()
            if date_like > 0:
                print(f"⚠️  WARNING: {date_like} brand values look like dates!")
    
    print(f"")
    
    missing_stats = {"stores": set(), "products": set(), "distributors": set(), "dates": set()}
    
    def safe_lookup(cache, key, cache_name):
        if key in cache:
            return cache[key]
        missing_stats[cache_name].add(str(key))
        return None
    
    # Map Store
    df["STORE_KEY"] = df.apply(
        lambda x: safe_lookup(
            store_cache,
            (normalize_key(x[column_mapping['STORE_NAME']]), 
             normalize_key(x[column_mapping['STORE_ADDRESS_LANE_1']]), 
             normalize_key(x[column_mapping['STORE_CITY']])),
            "stores"
        ), axis=1
    )
    
    # Map Product with fallback logic
    def map_product(product_name, brand):
        # Try exact match first (name + brand)
        exact_key = (normalize_key(product_name), normalize_key(brand))
        if exact_key in product_cache:
            return product_cache[exact_key]
        # Fallback: name only
        name_key = normalize_key(product_name)
        if name_key in product_cache:
            return product_cache[name_key]
        # Not found
        missing_stats["products"].add(str(product_name))
        return None
    
    df["PRODUCT_KEY"] = df.apply(
        lambda x: map_product(
            x[column_mapping['PRODUCT_NAME']], 
            x[column_mapping['BRAND']]
        ), axis=1
    )
    
    # Map Distributor
    df["DIST_KEY"] = df.apply(
        lambda x: safe_lookup(
            dist_cache,
            (normalize_key(x[column_mapping['DISTRIBUTOR_NAME']]), 
             normalize_key(x[column_mapping['DISTRIBUTOR_CITY']]), 
             normalize_key(x[column_mapping['DISTRIBUTOR_STATE']])),
            "distributors"
        ), axis=1
    )
    
    # Map Date
    def safe_date_lookup(date_str):
        try:
            date_obj = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(date_obj):
                return None
            date_key = date_obj.date()
            if date_key in date_cache:
                return date_cache[date_key]
            missing_stats["dates"].add(str(date_key))
            return None
        except:
            return None
    
    df["DATE_ID"] = df[column_mapping['FULL_DATE']].apply(safe_date_lookup)
    
    # Report missing keys
    print(f"❌ Missing dimension keys:")
    for dim, missing in missing_stats.items():
        if missing:
            count = len(missing)
            print(f"   {dim}: {count:,}")
            if dim == "products" and count > 0 and count <= 20:
                for item in list(missing)[:10]:
                    print(f"      - {item}")
    
    # ========================================
    # FILTER VALID RECORDS
    # ========================================
    before_filter = len(df)
    df_valid = df.dropna(subset=["STORE_KEY", "PRODUCT_KEY", "DIST_KEY", "DATE_ID"]).copy()
    after_filter = len(df_valid)
    
    if before_filter > after_filter:
        print(f"\n⚠️  Filtered {before_filter - after_filter:,} rows with missing dimension keys")
    
    if len(df_valid) == 0:
        print("\n⚠️  No valid rows after dimension key mapping")
        print("   This indicates dimension tables need data")
        cur.execute("DROP TABLE fact_sales_staging")
        conn.commit()
        mark_file_processed(source_file)
        print("✅ File marked as processed - exiting gracefully\n")
        cur.close()
        conn.close()
        exit(0)
    
    # Convert to int
    df_valid["STORE_KEY"] = df_valid["STORE_KEY"].astype(int)
    df_valid["PRODUCT_KEY"] = df_valid["PRODUCT_KEY"].astype(int)
    df_valid["DIST_KEY"] = df_valid["DIST_KEY"].astype(int)
    df_valid["DATE_ID"] = df_valid["DATE_ID"].astype(int)
    
    # ========================================
    # CLEAN NUMERIC COLUMNS
    # ========================================
    print("\n🧹 Cleaning numeric values...")
    
    numeric_columns = {
        'QUANTITY_SOLD': column_mapping['QUANTITY_SOLD'],
        'UNIT_PRICE': column_mapping['UNIT_PRICE'],
        'GROSS_AMOUNT': column_mapping['GROSS_AMOUNT'],
        'DISCOUNT_AMOUNT': column_mapping['DISCOUNT_AMOUNT'],
        'NET_AMOUNT': column_mapping['NET_AMOUNT']
    }
    
    for std_name, actual_col in numeric_columns.items():
        df_valid[f"{std_name}_CLEAN"] = df_valid[actual_col].apply(clean_numeric)
    
    before_numeric = len(df_valid)
    df_valid = df_valid.dropna(subset=[f"{col}_CLEAN" for col in numeric_columns.keys()])
    after_numeric = len(df_valid)
    
    if before_numeric > after_numeric:
        print(f"⚠️  Filtered {before_numeric - after_numeric:,} rows with invalid numeric values")
    
    if len(df_valid) == 0:
        print("\n⚠️  No valid rows after numeric validation")
        cur.execute("DROP TABLE fact_sales_staging")
        conn.commit()
        mark_file_processed(source_file)
        print("✅ File marked as processed - exiting gracefully\n")
        cur.close()
        conn.close()
        exit(0)
    
    print(f"✅ Valid records: {len(df_valid):,}\n")
    
    # ========================================
    # CREATE NATURAL KEY
    # ========================================
    print("🔑 Creating natural keys...")
    
    df_valid["NATURAL_KEY"] = (
        df_valid["DATE_ID"].astype(str) + "_" +
        df_valid["STORE_KEY"].astype(str) + "_" +
        df_valid["PRODUCT_KEY"].astype(str) + "_" +
        df_valid["DIST_KEY"].astype(str)
    )
    
    print(f"✅ Natural key: DATE_ID_STORE_KEY_PRODUCT_KEY_DIST_KEY\n")
    
    # ========================================
    # LOAD EXISTING RECORDS
    # ========================================
    print("🔍 Loading existing records...")
    
    cur.execute("""
        SELECT 
            sales_key,
            date_id || '_' || store_key || '_' || product_key || '_' || dist_key as natural_key,
            quantity_sold, unit_price, gross_amount, discount_amount, net_amount
        FROM fact_sales_dw
    """)
    
    existing_keys = {}
    for row in cur.fetchall():
        existing_keys[row[1]] = {
            'sales_key': row[0], 'quantity_sold': row[2], 'unit_price': row[3],
            'gross_amount': row[4], 'discount_amount': row[5], 'net_amount': row[6]
        }
    
    print(f"✅ Loaded {len(existing_keys):,} existing records\n")
    
    # ========================================
    # CLASSIFY RECORDS (INSERT vs UPDATE)
    # ========================================
    print("🔀 Classifying records...")
    
    insert_records = []
    update_records = []
    tolerance = 0.01
    
    for idx, row in df_valid.iterrows():
        natural_key = row['NATURAL_KEY']
        
        if natural_key in existing_keys:
            existing = existing_keys[natural_key]
            
            changed = (
                abs(existing['quantity_sold'] - row['QUANTITY_SOLD_CLEAN']) > tolerance or
                abs(existing['unit_price'] - row['UNIT_PRICE_CLEAN']) > tolerance or
                abs(existing['gross_amount'] - row['GROSS_AMOUNT_CLEAN']) > tolerance or
                abs(existing['discount_amount'] - row['DISCOUNT_AMOUNT_CLEAN']) > tolerance or
                abs(existing['net_amount'] - row['NET_AMOUNT_CLEAN']) > tolerance
            )
            
            if changed:
                update_records.append({
                    'sales_key': existing['sales_key'],
                    'date_id': int(row['DATE_ID']),
                    'store_key': int(row['STORE_KEY']),
                    'product_key': int(row['PRODUCT_KEY']),
                    'dist_key': int(row['DIST_KEY']),
                    'quantity_sold': int(row['QUANTITY_SOLD_CLEAN']),
                    'unit_price': float(row['UNIT_PRICE_CLEAN']),
                    'gross_amount': float(row['GROSS_AMOUNT_CLEAN']),
                    'discount_amount': float(row['DISCOUNT_AMOUNT_CLEAN']),
                    'net_amount': float(row['NET_AMOUNT_CLEAN'])
                })
        else:
            insert_records.append({
                'date_id': int(row['DATE_ID']),
                'store_key': int(row['STORE_KEY']),
                'product_key': int(row['PRODUCT_KEY']),
                'dist_key': int(row['DIST_KEY']),
                'quantity_sold': int(row['QUANTITY_SOLD_CLEAN']),
                'unit_price': float(row['UNIT_PRICE_CLEAN']),
                'gross_amount': float(row['GROSS_AMOUNT_CLEAN']),
                'discount_amount': float(row['DISCOUNT_AMOUNT_CLEAN']),
                'net_amount': float(row['NET_AMOUNT_CLEAN'])
            })
    
    print(f"   ➕ INSERT: {len(insert_records):,}")
    print(f"   🔄 UPDATE: {len(update_records):,}")
    print(f"   ⏭️  UNCHANGED: {len(df_valid) - len(insert_records) - len(update_records):,}\n")
    
    # ========================================
    # GENERATE SALES KEYS
    # ========================================
    if insert_records:
        cur.execute("SELECT NVL(MAX(sales_key), 0) FROM fact_sales_dw")
        start_key = cur.fetchone()[0]
        
        for idx, record in enumerate(insert_records):
            record['sales_key'] = start_key + idx + 1
    
    # ========================================
    # LOAD STAGING TABLE
    # ========================================
    all_staging_records = insert_records + update_records
    
    if not all_staging_records:
        print("ℹ️  No changes detected - all data already current")
        cur.execute("DROP TABLE fact_sales_staging")
        conn.commit()
        mark_file_processed(source_file)
        print("✅ File marked as processed - exiting gracefully\n")
        cur.close()
        conn.close()
        exit(0)
    
    print(f"💾 Loading {len(all_staging_records):,} records to staging...")
    
    staging_data = []
    for record in all_staging_records:
        staging_data.append((
            int(record['sales_key']),
            int(record['date_id']),
            int(record['store_key']),
            int(record['product_key']),
            int(record['dist_key']),
            int(record['quantity_sold']),
            float(record['unit_price']),
            float(record['gross_amount']),
            float(record['discount_amount']),
            float(record['net_amount'])
        ))
    
    BATCH_SIZE = 10000
    for i in range(0, len(staging_data), BATCH_SIZE):
        batch = staging_data[i:i+BATCH_SIZE]
        cur.executemany("""
            INSERT INTO fact_sales_staging (
                sales_key, date_id, store_key, product_key, dist_key,
                quantity_sold, unit_price, gross_amount, discount_amount, net_amount
            ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
        """, batch)
        conn.commit()
        print(f"   ✅ Batch {i//BATCH_SIZE + 1}: {len(batch):,} records")
    
    # ========================================
    # PERFORM MERGE (CORRECTED - USE NATURAL KEY)
    # ========================================
    print("\n🔄 Executing MERGE...")
    
    cur.execute("""
        MERGE INTO fact_sales_dw tgt
        USING fact_sales_staging stg
        ON (tgt.date_id = stg.date_id AND 
            tgt.store_key = stg.store_key AND 
            tgt.product_key = stg.product_key AND 
            tgt.dist_key = stg.dist_key)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.quantity_sold = stg.quantity_sold,
                tgt.unit_price = stg.unit_price,
                tgt.gross_amount = stg.gross_amount,
                tgt.discount_amount = stg.discount_amount,
                tgt.net_amount = stg.net_amount
        WHEN NOT MATCHED THEN
            INSERT (
                sales_key, date_id, store_key, product_key, dist_key,
                quantity_sold, unit_price, gross_amount, discount_amount, net_amount
            )
            VALUES (
                stg.sales_key, stg.date_id, stg.store_key, stg.product_key, stg.dist_key,
                stg.quantity_sold, stg.unit_price, stg.gross_amount, stg.discount_amount, stg.net_amount
            )
    """)
    
    conn.commit()
    print(f"✅ Merged {cur.rowcount:,} records\n")
    
    # ========================================
    # CLEANUP
    # ========================================
    print("🧹 Cleaning up...")
    cur.execute("DROP TABLE fact_sales_staging")
    conn.commit()
    
    # Mark as processed
    mark_file_processed(source_file)
    print(f"✅ File marked as processed (stays in {INCOMING_DIR})\n")
    
    # ========================================
    # STATISTICS
    # ========================================
    print("="*60)
    print("FINAL STATISTICS")
    print("="*60)
    
    cur.execute("SELECT COUNT(*), SUM(net_amount) FROM fact_sales_dw")
    stats = cur.fetchone()
    print(f"\n📊 Total records: {stats[0]:,}")
    print(f"💰 Total sales: ₹{stats[1]:,.2f}")
    
    print(f"\n📋 This Load:")
    print(f"   Source: {source_file}")
    print(f"   Initial: {initial_count:,}")
    print(f"   Valid: {len(df_valid):,}")
    print(f"   Inserted: {len(insert_records):,}")
    print(f"   Updated: {len(update_records):,}")
    
    cur.execute("""
        SELECT d.full_date, SUM(f.net_amount) as sales, COUNT(*) as txns
        FROM fact_sales_dw f
        JOIN dim_date_dw d ON f.date_id = d.date_id
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
        FETCH FIRST 5 ROWS ONLY
    """)
    
    print(f"\n📅 Recent Daily Sales:")
    for row in cur.fetchall():
        print(f"   {row[0].strftime('%Y-%m-%d')}: ₹{row[1]:,.2f} ({row[2]:,} txns)")

finally:
    try:
        if cur:
            cur.close()
    except:
        pass
    try:
        if conn:
            conn.close()
    except:
        pass

print(f"\n🎉 Load completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

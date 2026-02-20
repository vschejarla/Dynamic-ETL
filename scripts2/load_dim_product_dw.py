import oracledb
import pandas as pd
from decimal import Decimal, InvalidOperation
import re
from datetime import datetime

print("🚀 PRODUCT DIMENSION INCREMENTAL LOAD - Source to Target DW")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

# ========================================
# DATABASE CONFIGURATIONS
# ========================================
SOURCE_DB_CONFIG = {
    "user": "system",
    "password": "oracle123",
    "dsn": "host.docker.internal/orcl"
}

TARGET_DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

# ========================================
# CORRECTED MANUFACTURER-BRAND MAPPING
# ========================================
BRAND_TO_MANUFACTURER = {
    # Grocery - Rice
    "India Gate": "KRBL Limited",
    "Daawat": "LT Foods",
    "Kohinoor": "Kohinoor Foods",
    "Fortune": "Adani Wilmar",
    "Aashirvaad": "ITC Limited",

    # Grocery - Wheat Flour
    "Pillsbury": "General Mills",
    "Nature Fresh": "Generic",
    "Annapurna": "Generic",

    # Grocery - Edible Oil
    "Dhara": "Adani Wilmar",
    "Sundrop": "Ruchi Soya Industries",
    "Saffola": "Marico",
    "Gemini": "Cargill",

    # Grocery - Pulses & Spices
    "Tata Sampann": "Tata Consumer Products",
    "MDH": "MDH",
    "Everest": "Everest",
    "Catch": "Catch",

    # Grocery - Biscuits
    "Parle": "Parle Products",
    "Britannia": "Britannia Industries",
    "Sunfeast": "ITC Limited",
    "Priya Gold": "Priya Gold",

    # Grocery - Noodles
    "Maggi": "Nestlé",
    "Top Ramen": "Nissin Foods",
    "Yippee": "ITC Limited",
    "Knorr": "Unilever",

    # Grocery - Snacks
    "Haldiram's": "Haldiram's",
    "Bikaji": "Bikaji Foods International",
    "Balaji": "Balaji Wafers",
    "Lay's": "PepsiCo",
    "Uncle Chipps": "ITC Limited",
    "Bingo": "ITC Limited",
    "Pringles": "Kellanova",
    "Kurkure": "PepsiCo",

    # Beverage - Soft Drinks
    "Coca Cola": "Coca-Cola",
    "Thums Up": "Coca-Cola",
    "Sprite": "Coca-Cola",
    "Fanta": "Coca-Cola",
    "Limca": "Coca-Cola",
    "Maaza": "Coca-Cola",
    "Pepsi": "PepsiCo",
    "Mountain Dew": "PepsiCo",
    "7 Up": "PepsiCo",
    "Mirinda": "PepsiCo",
    "Frooti": "Parle Agro",
    "Appy Fizz": "Parle Agro",
    "Bailley": "Parle Agro",

    # Beverage - Juice
    "Tropicana": "PepsiCo",
    "Real": "Dabur",
    "Paper Boat": "ITC Limited",
    "Minute Maid": "Coca-Cola",
    "B Natural": "Parle Agro",

    # Beverage - Tea
    "Tata Tea": "Tata Consumer Products",
    "Tetley": "Tata Consumer Products",
    "Red Label": "Hindustan Unilever",
    "Taj Mahal": "Hindustan Unilever",
    "Lipton": "Unilever",
    "Brooke Bond": "Hindustan Unilever",
    "Wagh Bakri": "Wagh Bakri",
    "Girnar": "Girnar",

    # Beverage - Coffee
    "Nescafe": "Nestlé",
    "Bru": "Hindustan Unilever",
    "Tata Coffee": "Tata Consumer Products",
    "Continental": "Continental Coffee",

    # Beverage - Energy Drinks
    "Red Bull": "Red Bull GmbH",
    "Monster": "Monster Beverage",
    "Sting": "PepsiCo",
    "Gatorade": "PepsiCo",
    "Glucon-D": "Heinz India",
    "Powerade": "Coca-Cola",

    # Dairy
    "Amul": "Gujarat Cooperative Milk Marketing Federation",
    "Mother Dairy": "Mother Dairy",
    "Nestle": "Nestlé",
    "Heritage": "Heritage Foods",
    "Danone": "Danone",
    "Go Cheese": "Go Cheese",
    "Kraft": "Kraft Heinz",
    "Nandini": "Karnataka Milk Federation",
    "Govardhan": "Govardhan",

    # Personal Care
    "Clinic Plus": "Hindustan Unilever",
    "Dove": "Hindustan Unilever",
    "Sunsilk": "Hindustan Unilever",
    "Pantene": "Procter & Gamble",
    "Head & Shoulders": "Procter & Gamble",
    "Vivel": "ITC Limited",
    "Lux": "Hindustan Unilever",
    "Lifebuoy": "Hindustan Unilever",
    "Pears": "Procter & Gamble",
    "Dettol": "Reckitt Benckiser",
    "Colgate": "Colgate-Palmolive",
    "Pepsodent": "Hindustan Unilever",
    "Close-Up": "Hindustan Unilever",
    "Dabur": "Dabur",
    "Sensodyne": "GlaxoSmithKline",
    "Garnier": "L'Oréal",
    "Himalaya": "The Himalaya Drug Company",
    "Neutrogena": "Johnson & Johnson",
    "Nivea": "Beiersdorf",
    "Parachute": "Marico",
    "Bajaj": "Bajaj Corp",
    "Navratna": "Patanjali Ayurved",

    # Home Care
    "Surf Excel": "Hindustan Unilever",
    "Rin": "Hindustan Unilever",
    "Ariel": "Procter & Gamble",
    "Tide": "Procter & Gamble",
    "Wheel": "ITC Limited",
    "Vim": "Hindustan Unilever",
    "Finish": "Reckitt Benckiser",
    "Exo": "ITC Limited",
    "Pril": "Henkel",
    "Lizol": "Reckitt Benckiser",
    "Domex": "Hindustan Unilever",
    "Colin": "ITC Limited",
    "Harpic": "Reckitt Benckiser",

    # Baby Care
    "Johnson & Johnson": "Johnson & Johnson",
    "Mee Mee": "ITC Limited",
    "Chicco": "Chicco",
    "Sebamed": "Beiersdorf",
    "Pampers": "Procter & Gamble",
    "Huggies": "Kimberly-Clark",
    "MamyPoko": "Kao Corporation"
}

# ========================================
# CATEGORY/SUBCATEGORY STANDARDIZATION
# ========================================
CATEGORY_STANDARD_MAP = {
    "PERSONALCARE": "PersonalCare", "PERSONAL CARE": "PersonalCare",
    "BABYCARE": "BabyCare", "BABY CARE": "BabyCare",
    "GROCERY": "Grocery", "FOOD": "Grocery",
    "BEVERAGE": "Beverage", "DRINKS": "Beverage",
    "DAIRY": "Dairy",
    "HOMECARE": "HomeCare", "HOME CARE": "HomeCare"
}

SUBCATEGORY_STANDARD_MAP = {
    # Personal Care
    "SHAMPOO": "Shampoo", "SOAP": "Soap", "TOOTHPASTE": "Toothpaste",
    "FACE WASH": "Face Wash", "HAIR OIL": "Hair Oil",

    # Baby Care
    "BABY POWDER": "Baby Powder", "BABY WIPES": "Baby Wipes", "DIAPERS": "Diapers",
    "BABY SOAP": "Baby Soap",

    # Grocery
    "RICE": "Rice", "WHEAT FLOUR": "Wheat Flour", "ATTA": "Wheat Flour",
    "OIL": "Edible Oil", "COOKING OIL": "Edible Oil", "EDIBLE OIL": "Edible Oil",
    "OILS": "Edible Oil",
    "PULSES": "Pulses", "DAL": "Pulses",
    "SPICES": "Spices", "MASALA": "Spices",
    "BISCUITS": "Biscuits", "COOKIES": "Biscuits",
    "NOODLES": "Noodles", "SNACKS": "Snacks",

    # Beverages
    "SOFT DRINK": "Soft Drink", "JUICE": "Juice",
    "TEA": "Tea", "COFFEE": "Coffee", "ENERGY DRINK": "Energy Drink",
    "FLAVOURED MILK": "Flavoured Milk",

    # Dairy
    "MILK": "Milk", "CURD": "Curd", "CHEESE": "Cheese", "BUTTER": "Butter", "PANEER": "Paneer",

    # Home Care
    "DETERGENT": "Detergent", "DISHWASH": "Dishwash",
    "FLOOR CLEANER": "Floor Cleaner", "TOILET CLEANER": "Toilet Cleaner"
}


def clean_text(val):
    if pd.isna(val) or val is None or val == '':
        return None
    return str(val).strip()


def standardize_category(raw):
    if not raw:
        return "General"
    raw_upper = raw.upper().strip()
    if raw_upper in CATEGORY_STANDARD_MAP:
        return CATEGORY_STANDARD_MAP[raw_upper]
    return raw.strip()


def standardize_subcategory(raw):
    if not raw:
        return "General"
    raw_upper = raw.upper().strip()
    if raw_upper in SUBCATEGORY_STANDARD_MAP:
        return SUBCATEGORY_STANDARD_MAP[raw_upper]
    return raw.strip()


def get_manufacturer_for_brand(brand):
    if not brand:
        return "Generic"
    brand_clean = brand.strip()
    if brand_clean in BRAND_TO_MANUFACTURER:
        return BRAND_TO_MANUFACTURER[brand_clean]
    brand_upper = brand_clean.upper()
    for key, mfr in BRAND_TO_MANUFACTURER.items():
        if key.upper() == brand_upper:
            return mfr
    return "Generic"


def clean_price(val):
    if pd.isna(val) or val is None:
        return None
    try:
        val_str = str(val).strip()
        val_str = re.sub(r'[₹$,\s]', '', val_str)
        if not val_str:
            return None
        price = Decimal(val_str)
        if price <= 0 or price > 100000:
            return None
        return float(price)
    except (ValueError, InvalidOperation, TypeError):
        return None


# ========================================
# CONNECT TO DATABASES
# ========================================
print("🔌 Connecting to databases...\n")

try:
    source_conn = oracledb.connect(**SOURCE_DB_CONFIG)
    source_cur = source_conn.cursor()
    print("✅ Connected to SOURCE database (system)\n")
except Exception as e:
    print(f"❌ Source connection failed: {e}")
    exit(1)

try:
    target_conn = oracledb.connect(**TARGET_DB_CONFIG)
    target_cur = target_conn.cursor()
    print("✅ Connected to TARGET data warehouse (target_dw)\n")
except Exception as e:
    print(f"❌ Target connection failed: {e}")
    source_cur.close()
    source_conn.close()
    exit(1)

# ========================================
# EXTRACT FROM SOURCE
# ========================================
print("📥 Extracting products from SOURCE (dim_product)...\n")

try:
    source_cur.execute("""
        SELECT 
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            flavour,
            product_size,
            sku,
            uom,
            unit_price,
            business_stage
        FROM dim_product
        ORDER BY product_id
    """)
    source_data = source_cur.fetchall()
    print(f"✅ Extracted {len(source_data):,} products from source\n")

except Exception as e:
    print(f"❌ Error extracting from source: {e}")
    source_cur.close()
    source_conn.close()
    target_cur.close()
    target_conn.close()
    exit(1)

# ========================================
# TRANSFORM DATA
# ========================================
print("🔄 Transforming data with corrected mappings...\n")

df = pd.DataFrame(source_data, columns=[
    'PRODUCT_ID', 'PRODUCT_NAME', 'CATEGORY', 'SUB_CATEGORY',
    'BRAND', 'FLAVOUR', 'PRODUCT_SIZE', 'SKU', 'UOM', 'UNIT_PRICE', 'BUSINESS_STAGE'
])

df['CATEGORY_CLEAN'] = df['CATEGORY'].apply(standardize_category)
df['SUB_CATEGORY_CLEAN'] = df['SUB_CATEGORY'].apply(standardize_subcategory)
df['MANUFACTURER'] = df['BRAND'].apply(get_manufacturer_for_brand)
df['BRAND_CLEAN'] = df['BRAND'].apply(clean_text)
df['FLAVOUR_CLEAN'] = df['FLAVOUR'].apply(clean_text)
df['PRICE_CLEAN'] = df['UNIT_PRICE'].apply(clean_price)

df_valid = df[
    (df['PRODUCT_NAME'].notna()) &
    (df['BRAND_CLEAN'].notna()) &
    (df['PRICE_CLEAN'].notna())
].copy()

print(f"✅ Valid products after transformation: {len(df_valid):,}\n")

print("📊 Sample Corrected Mappings:")
sample_brands = ['Pringles', "Haldiram's", 'Amul', 'Maggi', 'Fortune']
for brand in sample_brands:
    if brand in df_valid['BRAND_CLEAN'].values:
        mfr = df_valid[df_valid['BRAND_CLEAN'] == brand]['MANUFACTURER'].iloc[0]
        print(f"   {brand:20} → {mfr}")
print()

# ========================================
# CREATE STAGING TABLE IN TARGET
# ========================================
print("🏗️  Creating staging table in target DW...\n")

try:
    target_cur.execute("DROP TABLE product_staging")
    print("   ⚠️  Dropped existing staging table\n")
except:
    pass

target_cur.execute("""
    CREATE TABLE product_staging (
        source_product_id NUMBER,
        product_key       NUMBER,
        product_name      VARCHAR2(100),
        category_key      NUMBER,
        sub_category_key  NUMBER,
        manufacturer_key  NUMBER,
        brand             VARCHAR2(50),
        flavour           VARCHAR2(50),
        product_size      VARCHAR2(20),
        sku               VARCHAR2(50),
        unit_of_measure   VARCHAR2(10),
        unit_price        NUMBER(10,2),
        business_stage    VARCHAR2(20),
        operation         VARCHAR2(10),
        CONSTRAINT pk_staging PRIMARY KEY (product_name, brand, product_size)
    )
""")
target_conn.commit()
print("✅ Staging table created (with PRIMARY KEY to prevent duplicates)\n")

# ========================================
# LOAD DIMENSION TABLES
# ========================================
print("📊 Loading/Updating Dimension Tables...\n")

# --- CATEGORY DIMENSION ---
target_cur.execute("SELECT category_key, category_name FROM dim_category")
category_cache = {r[1]: r[0] for r in target_cur.fetchall()}
target_cur.execute("SELECT NVL(MAX(category_key),0) FROM dim_category")
next_category_key = target_cur.fetchone()[0] + 1

categories_added = 0
for cat in df_valid['CATEGORY_CLEAN'].unique():
    if cat not in category_cache:
        target_cur.execute("INSERT INTO dim_category VALUES (:1,:2)", (next_category_key, cat))
        category_cache[cat] = next_category_key
        next_category_key += 1
        categories_added += 1
        print(f"   ➕ Added category: {cat}")

target_conn.commit()
print(f"   ✅ Categories: {len(category_cache)} (New: {categories_added})\n")

# --- SUBCATEGORY DIMENSION ---
target_cur.execute("SELECT sub_category_key, sub_category_name FROM dim_sub_category")
subcat_cache = {r[1]: r[0] for r in target_cur.fetchall()}
target_cur.execute("SELECT NVL(MAX(sub_category_key),0) FROM dim_sub_category")
next_subcat_key = target_cur.fetchone()[0] + 1

subcats_added = 0
for sub in df_valid['SUB_CATEGORY_CLEAN'].unique():
    if sub not in subcat_cache:
        target_cur.execute("INSERT INTO dim_sub_category VALUES (:1,:2)", (next_subcat_key, sub))
        subcat_cache[sub] = next_subcat_key
        next_subcat_key += 1
        subcats_added += 1
        print(f"   ➕ Added sub-category: {sub}")

target_conn.commit()
print(f"   ✅ Sub-categories: {len(subcat_cache)} (New: {subcats_added})\n")

# --- MANUFACTURER DIMENSION ---
target_cur.execute("SELECT manufacturer_key, manufacturer_name FROM dim_manufacturer")
mfr_cache = {r[1]: r[0] for r in target_cur.fetchall()}
target_cur.execute("SELECT NVL(MAX(manufacturer_key),0) FROM dim_manufacturer")
next_mfr_key = target_cur.fetchone()[0] + 1

mfrs_added = 0
for mfr in df_valid['MANUFACTURER'].unique():
    if mfr not in mfr_cache:
        target_cur.execute("INSERT INTO dim_manufacturer VALUES (:1,:2)", (next_mfr_key, mfr))
        mfr_cache[mfr] = next_mfr_key
        next_mfr_key += 1
        mfrs_added += 1
        print(f"   ➕ Added manufacturer: {mfr}")

target_conn.commit()
print(f"   ✅ Manufacturers: {len(mfr_cache)} (New: {mfrs_added})\n")

# ========================================
# PREPARE PRODUCT DIMENSION MERGE
# ========================================
print("📦 Preparing Product Dimension MERGE...\n")

target_cur.execute("""
    SELECT 
        product_key, product_name, brand, category_key, sub_category_key,
        manufacturer_key, flavour, product_size, sku,
        unit_of_measure, unit_price, business_stage
    FROM dim_product_dw
""")

product_cache = {}
for r in target_cur.fetchall():
    bk = (r[1], r[2], r[7])
    product_cache[bk] = {
        'product_key': r[0],
        'category_key': r[3],
        'sub_category_key': r[4],
        'manufacturer_key': r[5],
        'flavour': r[6],
        'sku': r[8],
        'unit_of_measure': r[9],
        'unit_price': float(r[10]) if r[10] else None,
        'business_stage': r[11]
    }

print(f"📦 Existing products in target DW: {len(product_cache):,}\n")

# ==================================================
# Remove duplicates from source data
# ==================================================
print("🔍 Checking for duplicates in source data...")
df_dedup = df_valid.drop_duplicates(subset=['PRODUCT_NAME', 'BRAND_CLEAN', 'PRODUCT_SIZE'], keep='last')
duplicates_removed = len(df_valid) - len(df_dedup)
if duplicates_removed > 0:
    print(f"   ⚠️  Removed {duplicates_removed} duplicate rows from source")
    df_valid = df_dedup
print()

# Determine INSERT vs UPDATE
insert_products = []
update_products = []

# -------------------------------------------------------
# FIX: Use Python-side next_product_key (not re-queried
#      just before MERGE) so keys are assigned correctly
#      here and reused verbatim in the staging table.
# -------------------------------------------------------
target_cur.execute("SELECT NVL(MAX(product_key),0) FROM dim_product_dw")
next_product_key = target_cur.fetchone()[0] + 1

for _, row in df_valid.iterrows():
    bk = (row['PRODUCT_NAME'], row['BRAND_CLEAN'], row['PRODUCT_SIZE'])

    if bk not in product_cache:
        insert_products.append({
            'product_key': next_product_key,
            'bk': bk,
            'data': row
        })
        next_product_key += 1
    else:
        existing = product_cache[bk]

        price_changed = False
        if existing['unit_price'] is not None and row['PRICE_CLEAN'] is not None:
            price_changed = abs(existing['unit_price'] - row['PRICE_CLEAN']) > 0.01
        else:
            price_changed = True

        changed = (
            existing['category_key'] != category_cache.get(row['CATEGORY_CLEAN']) or
            existing['sub_category_key'] != subcat_cache.get(row['SUB_CATEGORY_CLEAN']) or
            existing['manufacturer_key'] != mfr_cache.get(row['MANUFACTURER']) or
            (existing['flavour'] or '') != (row['FLAVOUR_CLEAN'] or '') or
            existing['sku'] != row['SKU'] or
            existing['unit_of_measure'] != row['UOM'] or
            (existing['business_stage'] or '') != (row['BUSINESS_STAGE'] or '') or
            price_changed
        )

        if changed:
            update_products.append({
                'product_key': existing['product_key'],
                'bk': bk,
                'data': row
            })

print(f"   ➕ New products to insert: {len(insert_products):,}")
print(f"   🔄 Products to update: {len(update_products):,}\n")

# ========================================
# LOAD STAGING TABLE
# ========================================
print("💾 Loading staging table...\n")

staging_data = []

for product in insert_products:
    row = product['data']
    try:
        staging_data.append((
            int(row['PRODUCT_ID']),
            product['product_key'],          # pre-assigned key — used directly in MERGE
            row['PRODUCT_NAME'],
            category_cache.get(row['CATEGORY_CLEAN']),
            subcat_cache.get(row['SUB_CATEGORY_CLEAN']),
            mfr_cache.get(row['MANUFACTURER']),
            row['BRAND_CLEAN'],
            row['FLAVOUR_CLEAN'],
            row['PRODUCT_SIZE'],
            row['SKU'],
            row['UOM'],
            Decimal(str(row['PRICE_CLEAN'])) if row['PRICE_CLEAN'] else None,
            row['BUSINESS_STAGE'],
            'INSERT'
        ))
    except Exception as e:
        print(f"   ⚠️  Error preparing INSERT for {row['PRODUCT_NAME']}: {e}")

for product in update_products:
    row = product['data']
    try:
        staging_data.append((
            int(row['PRODUCT_ID']),
            product['product_key'],          # existing key from product_cache
            row['PRODUCT_NAME'],
            category_cache.get(row['CATEGORY_CLEAN']),
            subcat_cache.get(row['SUB_CATEGORY_CLEAN']),
            mfr_cache.get(row['MANUFACTURER']),
            row['BRAND_CLEAN'],
            row['FLAVOUR_CLEAN'],
            row['PRODUCT_SIZE'],
            row['SKU'],
            row['UOM'],
            Decimal(str(row['PRICE_CLEAN'])) if row['PRICE_CLEAN'] else None,
            row['BUSINESS_STAGE'],
            'UPDATE'
        ))
    except Exception as e:
        print(f"   ⚠️  Error preparing UPDATE for {row['PRODUCT_NAME']}: {e}")

if staging_data:
    BATCH_SIZE = 1000
    total_batches = (len(staging_data) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, len(staging_data))
        batch = staging_data[start_idx:end_idx]

        target_cur.executemany("""
            INSERT INTO product_staging (
                source_product_id, product_key, product_name,
                category_key, sub_category_key, manufacturer_key,
                brand, flavour, product_size, sku,
                unit_of_measure, unit_price, business_stage, operation
            ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)
        """, batch)

        target_conn.commit()
        print(f"   ✅ Batch {batch_num + 1}/{total_batches}: {len(batch):,} records")

    print(f"\n✅ Staging completed: {len(staging_data):,} records\n")

# ========================================
# EXECUTE MERGE  (FIXED)
# ========================================
# KEY FIX: Use stg.product_key directly (pre-assigned in Python above).
# The old code used ROW_NUMBER() + max_key inside the MERGE USING clause,
# which generated new keys for ALL rows (inserts + updates), causing
# ORA-00001 unique constraint violations on the product_key column.
# ========================================
if staging_data:
    print("🔄 Executing MERGE into dim_product_dw...\n")

    try:
        target_cur.execute("""
            MERGE INTO dim_product_dw tgt
            USING (
                SELECT
                    product_key,
                    product_name,
                    category_key,
                    sub_category_key,
                    manufacturer_key,
                    brand,
                    flavour,
                    product_size,
                    sku,
                    unit_of_measure,
                    unit_price,
                    business_stage,
                    operation
                FROM product_staging
            ) stg
            ON (
                tgt.product_name  = stg.product_name
                AND tgt.brand     = stg.brand
                AND tgt.product_size = stg.product_size
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.category_key      = stg.category_key,
                    tgt.sub_category_key  = stg.sub_category_key,
                    tgt.manufacturer_key  = stg.manufacturer_key,
                    tgt.flavour           = stg.flavour,
                    tgt.sku               = stg.sku,
                    tgt.unit_of_measure   = stg.unit_of_measure,
                    tgt.unit_price        = stg.unit_price,
                    tgt.business_stage    = stg.business_stage
                WHERE stg.operation = 'UPDATE'
            WHEN NOT MATCHED THEN
                INSERT (
                    product_key, product_name, category_key, sub_category_key,
                    manufacturer_key, brand, flavour, product_size,
                    sku, unit_of_measure, unit_price, business_stage
                )
                VALUES (
                    stg.product_key, stg.product_name, stg.category_key, stg.sub_category_key,
                    stg.manufacturer_key, stg.brand, stg.flavour, stg.product_size,
                    stg.sku, stg.unit_of_measure, stg.unit_price, stg.business_stage
                )
                WHERE stg.operation = 'INSERT'
        """)

        rows_merged = target_cur.rowcount
        target_conn.commit()

        print(f"✅ MERGE completed successfully!")
        print(f"   Total rows affected: {rows_merged:,}")
        print(f"   - Inserted: {len(insert_products):,}")
        print(f"   - Updated: {len(update_products):,}\n")

    except Exception as e:
        target_conn.rollback()
        print(f"\n❌ Error during MERGE: {str(e)}")
        raise

# ========================================
# CLEANUP
# ========================================
print("🧹 Cleaning up staging table...")
target_cur.execute("DROP TABLE product_staging")
target_conn.commit()
print("✅ Staging table dropped\n")

# ========================================
# VALIDATION
# ========================================
print(f"{'='*70}")
print("✅ VALIDATION CHECKS")
print(f"{'='*70}\n")

critical_brands = [
    ('Pringles', 'Kellanova'),
    ("Haldiram's", "Haldiram's"),
    ('Amul', 'Gujarat Cooperative Milk Marketing Federation'),
    ('Fortune', 'Adani Wilmar'),
    ('Maggi', 'Nestlé')
]

print("📋 Critical Brand → Manufacturer Mappings:\n")
for brand, expected_mfr in critical_brands:
    target_cur.execute("""
        SELECT p.brand, m.manufacturer_name, COUNT(*) as cnt
        FROM dim_product_dw p
        JOIN dim_manufacturer m ON p.manufacturer_key = m.manufacturer_key
        WHERE p.brand = :1
        GROUP BY p.brand, m.manufacturer_name
    """, [brand])

    result = target_cur.fetchone()
    if result:
        actual_mfr = result[1]
        status = "✅" if actual_mfr == expected_mfr else "❌"
        print(f"{status} {brand:20} → {actual_mfr} ({result[2]} products)")
    else:
        print(f"⚠️  {brand:20} → Not found in DW")

# ========================================
# FINAL STATISTICS
# ========================================
print(f"\n{'='*70}")
print("📊 FINAL STATISTICS")
print(f"{'='*70}\n")

target_cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT category_key) as cats,
        COUNT(DISTINCT sub_category_key) as subcats,
        COUNT(DISTINCT manufacturer_key) as mfrs,
        COUNT(DISTINCT brand) as brands,
        COUNT(DISTINCT business_stage) as stages,
        MIN(unit_price) as min_price,
        MAX(unit_price) as max_price,
        AVG(unit_price) as avg_price
    FROM dim_product_dw
""")

stats = target_cur.fetchone()

print(f"📦 dim_product_dw Statistics:")
print(f"   Total products    : {stats[0]:,}")
print(f"   Unique categories : {stats[1]}")
print(f"   Unique sub-cats   : {stats[2]}")
print(f"   Unique manufacturers: {stats[3]}")
print(f"   Unique brands     : {stats[4]}")
print(f"   Business stages   : {stats[5]}")
print(f"   Price range       : ₹{stats[6]:.2f} - ₹{stats[7]:.2f}")
print(f"   Average price     : ₹{stats[8]:.2f}")

print(f"\n📊 Products by Category:")
target_cur.execute("""
    SELECT c.category_name, COUNT(*) as cnt
    FROM dim_product_dw p
    JOIN dim_category c ON p.category_key = c.category_key
    GROUP BY c.category_name
    ORDER BY cnt DESC
""")
for row in target_cur.fetchall():
    print(f"   {row[0]:25} {row[1]:>6,} products")

print(f"\n📊 Products by Business Stage:")
target_cur.execute("""
    SELECT business_stage, COUNT(*) as cnt
    FROM dim_product_dw
    GROUP BY business_stage
    ORDER BY cnt DESC
""")
for row in target_cur.fetchall():
    stage = row[0] if row[0] else 'NULL'
    print(f"   {stage:25} {row[1]:>6,} products")

# ========================================
# CLOSE CONNECTIONS
# ========================================
source_cur.close()
source_conn.close()
target_cur.close()
target_conn.close()

print(f"\n{'='*70}")
print("🎉 INCREMENTAL LOAD COMPLETED SUCCESSFULLY")
print(f"{'='*70}")
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

print("✅ Summary:")
print(f"   • Extracted   : {len(source_data):,} products from source")
print(f"   • Transformed : {len(df_valid):,} valid products")
print(f"   • Inserted    : {len(insert_products):,} new products")
print(f"   • Updated     : {len(update_products):,} existing products")
print(f"   • Total in DW : {stats[0]:,} products")
print(f"\n💡 Next run will process incremental changes!")

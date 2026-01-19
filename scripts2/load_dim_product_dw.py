import os
import random
import pandas as pd
import oracledb
from decimal import Decimal, InvalidOperation
import re
from datetime import datetime

print("🚀 PRODUCT DIMENSION DATA WAREHOUSE LOAD STARTED")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

# ========================================
# CONFIG
# ========================================
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

try:
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected to data warehouse (target_dw)\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

# ========================================
# CATEGORY STANDARDIZATION MAP
# ========================================
CATEGORY_STANDARD_MAP = {
    # Personal Care
    "PERSONALCARE": "PersonalCare", "PERSONAL CARE": "PersonalCare",
    "SKINCARE": "PersonalCare", "SKIN CARE": "PersonalCare",
    "HAIRCARE": "PersonalCare", "HAIR CARE": "PersonalCare",
    "BODYCARE": "PersonalCare", "BODY CARE": "PersonalCare",
    "COSMETICS": "PersonalCare", "BEAUTY": "PersonalCare",
    
    # Baby Care
    "BABYCARE": "BabyCare", "BABY CARE": "BabyCare",
    "INFANT CARE": "BabyCare", "KIDS CARE": "BabyCare",
    
    # Grocery
    "GROCERY": "Grocery", "GROCERIES": "Grocery",
    "FOOD": "Grocery", "FOODS": "Grocery",
    "SNACKS": "Grocery", "SNACK": "Grocery",
    "STAPLES": "Grocery", "DRY GOODS": "Grocery",
    
    # Beverages
    "BEVERAGE": "Beverage", "BEVERAGES": "Beverage",
    "DRINK": "Beverage", "DRINKS": "Beverage",
    "SOFT DRINKS": "Beverage",
    
    # Dairy
    "DAIRY": "Dairy", "MILK": "Dairy",
    "CHEESE": "Dairy", "YOGURT": "Dairy", "BUTTER": "Dairy",
    
    # Home Care
    "HOMECARE": "HomeCare", "HOME CARE": "HomeCare",
    "CLEANING": "HomeCare", "DETERGENT": "HomeCare", "LAUNDRY": "HomeCare"
}

# ========================================
# SUBCATEGORY STANDARDIZATION MAP
# ========================================
SUBCATEGORY_STANDARD_MAP = {
    # Personal Care
    "SHAMPOO": "Shampoo", "ANTI DANDRUFF SHAMPOO": "Shampoo",
    "HAIR SHAMPOO": "Shampoo", "CONDITIONER": "Conditioner",
    "HAIR OIL": "Hair Oil", "COCONUT OIL": "Hair Oil",
    "SOAP": "Soap", "BATHING SOAP": "Soap", "BEAUTY SOAP": "Soap",
    "FACE WASH": "Face Wash", "FACIAL CLEANSER": "Face Wash",
    "TOOTHPASTE": "Toothpaste", "TOOTH PASTE": "Toothpaste",
    "BODY WASH": "Body Wash", "SHOWER GEL": "Body Wash",
    
    # Baby Care
    "BABY POWDER": "Baby Powder", "BABY WIPES": "Baby Wipes",
    "BABY SOAP": "Baby Soap", "DIAPERS": "Diapers",
    "BABY DIAPER": "Diapers", "DIAPER PANTS": "Diapers",
    
    # Grocery
    "RICE": "Rice", "BASMATI RICE": "Rice",
    "ATTA": "Wheat Flour", "WHEAT FLOUR": "Wheat Flour",
    "OIL": "Edible Oil", "COOKING OIL": "Edible Oil",
    "SUNFLOWER OIL": "Edible Oil", "MUSTARD OIL": "Edible Oil",
    "PULSES": "Pulses", "DAL": "Pulses",
    "TOOR DAL": "Pulses", "MOONG DAL": "Pulses",
    "SPICES": "Spices", "MASALA": "Spices",
    "TURMERIC": "Spices", "RED CHILLI": "Spices",
    "BISCUIT": "Biscuits", "BISCUITS": "Biscuits", "COOKIES": "Biscuits",
    "NOODLES": "Noodles", "INSTANT NOODLES": "Noodles",
    
    # Beverages
    "SOFT DRINK": "Soft Drink", "COLD DRINK": "Soft Drink",
    "COLA": "Soft Drink", "JUICE": "Juice", "FRUIT JUICE": "Juice",
    "TEA": "Tea", "GREEN TEA": "Tea", "BLACK TEA": "Tea",
    "COFFEE": "Coffee", "INSTANT COFFEE": "Coffee",
    "ENERGY DRINK": "Energy Drink",
    
    # Dairy
    "MILK": "Milk", "FULL CREAM MILK": "Milk", "TONED MILK": "Milk",
    "CURD": "Curd", "YOGURT": "Curd", "DAHI": "Curd",
    "CHEESE": "Cheese", "CHEESE SLICE": "Cheese",
    "BUTTER": "Butter", "PANEER": "Paneer",
    
    # Home Care
    "DETERGENT": "Detergent", "WASHING POWDER": "Detergent",
    "DISHWASH": "Dishwash", "DISH WASH": "Dishwash",
    "FLOOR CLEANER": "Floor Cleaner", "TOILET CLEANER": "Toilet Cleaner"
}

# ========================================
# MANUFACTURER MAP (INDIAN MARKET)
# ========================================
MANUFACTURER_MAP = {
    "PersonalCare": ["Hindustan Unilever Ltd", "Procter & Gamble", "Dabur India", "Marico Ltd", "Godrej Consumer Products", "Colgate-Palmolive"],
    "BabyCare": ["Johnson & Johnson", "Himalaya Baby Care", "Mee Mee", "Chicco"],
    "Grocery": ["Tata Consumer Products", "Nestle India", "Britannia", "ITC Ltd", "Parle Products", "Haldiram's"],
    "Beverage": ["Coca Cola", "PepsiCo", "Tropicana", "Red Bull", "Tata Consumer Products", "Paper Boat"],
    "Dairy": ["Amul", "Mother Dairy", "Nestle", "Britannia", "Danone India"],
    "HomeCare": ["Hindustan Unilever Ltd", "Procter & Gamble", "Reckitt Benckiser", "Godrej Consumer Products"]
}

# ========================================
# INTELLIGENT CATEGORY INFERENCE
# ========================================
def infer_category_from_subcategory(subcategory):
    """Infer category based on subcategory keywords"""
    if not subcategory:
        return "General"
    
    sc_upper = subcategory.upper()
    
    # Baby Care keywords
    if any(kw in sc_upper for kw in ["BABY", "DIAPER", "INFANT"]):
        return "BabyCare"
    
    # Personal Care keywords
    if any(kw in sc_upper for kw in ["SHAMPOO", "SOAP", "TOOTHPASTE", "FACE WASH", "HAIR OIL"]):
        return "PersonalCare"
    
    # Grocery keywords
    if any(kw in sc_upper for kw in ["RICE", "ATTA", "OIL", "DAL", "SPICES", "BISCUIT", "NOODLES"]):
        return "Grocery"
    
    # Beverage keywords
    if any(kw in sc_upper for kw in ["DRINK", "COLA", "JUICE", "TEA", "COFFEE"]):
        return "Beverage"
    
    # Dairy keywords
    if any(kw in sc_upper for kw in ["MILK", "CURD", "CHEESE", "BUTTER", "PANEER", "YOGURT"]):
        return "Dairy"
    
    # Home Care keywords
    if any(kw in sc_upper for kw in ["DETERGENT", "DISHWASH", "CLEANER"]):
        return "HomeCare"
    
    return "General"

# ========================================
# DATA CLEANING FUNCTIONS
# ========================================
def clean_text(val):
    """Clean and normalize text"""
    if pd.isna(val) or val is None or val == '':
        return None
    return str(val).strip().upper()

def standardize_category(raw, subcategory=None):
    """Standardize category name"""
    raw = clean_text(raw)
    
    if not raw:
        if subcategory:
            return infer_category_from_subcategory(subcategory)
        return "General"
    
    # Direct mapping
    if raw in CATEGORY_STANDARD_MAP:
        return CATEGORY_STANDARD_MAP[raw]
    
    # Partial match
    for key, value in CATEGORY_STANDARD_MAP.items():
        if key in raw or raw in key:
            return value
    
    return raw.title()

def standardize_subcategory(raw):
    """Standardize subcategory name"""
    raw = clean_text(raw)
    
    if not raw:
        return "General"
    
    # Direct mapping
    if raw in SUBCATEGORY_STANDARD_MAP:
        return SUBCATEGORY_STANDARD_MAP[raw]
    
    # Partial match
    for key, value in SUBCATEGORY_STANDARD_MAP.items():
        if key in raw or raw in key:
            return value
    
    return raw.title()

def clean_price(val):
    """Clean and validate price"""
    if pd.isna(val) or val is None or val == '':
        return None
    
    try:
        val_str = str(val).strip()
        val_str = re.sub(r'[₹$,\s]', '', val_str)
        
        if not val_str:
            return None
        
        price = Decimal(val_str)
        
        # Validate range (₹1 to ₹100,000)
        if price <= 0 or price > 100000:
            return None
            
        return float(price)
        
    except (ValueError, InvalidOperation, TypeError):
        return None

def get_manufacturer(category):
    """Get manufacturer based on category"""
    return random.choice(MANUFACTURER_MAP.get(category, ["Generic Manufacturer"]))

def clean_string_field(val, default="Unknown"):
    """Clean string fields with default"""
    if pd.isna(val) or val is None or str(val).strip() == '':
        return default
    return str(val).strip()

# ========================================
# READ LATEST FILE
# ========================================
print("📁 Checking for incoming files...\n")

if not os.path.exists(INCOMING_DIR):
    print(f"❌ Directory not found: {INCOMING_DIR}")
    cur.close()
    conn.close()
    exit(1)

files = sorted([f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")], reverse=True)

if not files:
    print(f"⚠️  No CSV files found in {INCOMING_DIR}")
    cur.close()
    conn.close()
    exit(1)

file_path = os.path.join(INCOMING_DIR, files[0])
print(f"📄 Processing file: {files[0]}")
print(f"   Path: {file_path}\n")

# ========================================
# LOAD AND CLEAN DATA
# ========================================
print("⏳ Loading and cleaning data...\n")

try:
    df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA', 'N/A'])
    print(f"✅ File loaded: {len(df):,} rows\n")
except Exception as e:
    print(f"❌ Error loading file: {e}")
    cur.close()
    conn.close()
    exit(1)

# Data cleaning
print("🧹 Cleaning data...")

# Clean subcategory first
df["SUB_CATEGORY_CLEAN"] = df["SUB_CATEGORY"].apply(standardize_subcategory)

# Clean category with subcategory context
df["CATEGORY_CLEAN"] = df.apply(
    lambda row: standardize_category(row["CATEGORY"], row["SUB_CATEGORY_CLEAN"]), 
    axis=1
)

# Clean price
df["PRODUCT_UNIT_PRICE_CLEAN"] = df["PRODUCT_UNIT_PRICE"].apply(clean_price)

# Filter valid prices
df_valid = df[df["PRODUCT_UNIT_PRICE_CLEAN"].notna()].copy()

print(f"   ✅ Valid rows: {len(df_valid):,}")
print(f"   ❌ Invalid/skipped: {len(df) - len(df_valid):,}\n")

# Clean other fields
df_valid["PRODUCT_NAME"] = df_valid["PRODUCT_NAME"].apply(lambda x: clean_string_field(x, "Unknown Product"))
df_valid["BRAND"] = df_valid["BRAND"].apply(lambda x: clean_string_field(x, "Generic"))
df_valid["FLAVOUR"] = df_valid["FLAVOUR"].apply(lambda x: clean_string_field(x, None))
df_valid["PRODUCT_SIZE"] = df_valid["PRODUCT_SIZE"].apply(lambda x: clean_string_field(x, "Standard"))
df_valid["UOM"] = df_valid["UOM"].apply(lambda x: clean_string_field(x, "Unit"))
df_valid["SQC"] = df_valid["SQC"].apply(lambda x: clean_string_field(x, "A"))

# Generate manufacturer
df_valid["MANUFACTURER"] = df_valid["CATEGORY_CLEAN"].apply(get_manufacturer)

print(f"📦 Final valid rows: {len(df_valid):,}\n")

# ========================================
# LOAD CATEGORY DIMENSION
# ========================================
print("📊 Loading Category Dimension...\n")

cur.execute("SELECT category_key, category_name FROM dim_category")
category_cache = {r[1]: r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(category_key),0) FROM dim_category")
next_category_key = cur.fetchone()[0] + 1

categories_added = 0
for cat in df_valid["CATEGORY_CLEAN"].dropna().unique():
    if cat not in category_cache:
        cur.execute("INSERT INTO dim_category VALUES (:1,:2)", (next_category_key, cat))
        category_cache[cat] = next_category_key
        next_category_key += 1
        categories_added += 1
        print(f"   ➕ Added category: {cat}")

conn.commit()
print(f"\n   ✅ Total categories: {len(category_cache)} (Added: {categories_added})\n")

# ========================================
# LOAD SUB-CATEGORY DIMENSION
# ========================================
print("📊 Loading Sub-Category Dimension...\n")

cur.execute("SELECT sub_category_key, sub_category_name FROM dim_sub_category")
subcat_cache = {r[1]: r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(sub_category_key),0) FROM dim_sub_category")
next_subcat_key = cur.fetchone()[0] + 1

subcats_added = 0
for sub in df_valid["SUB_CATEGORY_CLEAN"].dropna().unique():
    if sub not in subcat_cache:
        cur.execute("INSERT INTO dim_sub_category VALUES (:1,:2)", (next_subcat_key, sub))
        subcat_cache[sub] = next_subcat_key
        next_subcat_key += 1
        subcats_added += 1
        print(f"   ➕ Added sub-category: {sub}")

conn.commit()
print(f"\n   ✅ Total sub-categories: {len(subcat_cache)} (Added: {subcats_added})\n")

# ========================================
# LOAD MANUFACTURER DIMENSION
# ========================================
print("🏭 Loading Manufacturer Dimension...\n")

cur.execute("SELECT manufacturer_key, manufacturer_name FROM dim_manufacturer")
mfr_cache = {r[1]: r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(manufacturer_key),0) FROM dim_manufacturer")
next_mfr_key = cur.fetchone()[0] + 1

mfrs_added = 0
for mfr in df_valid["MANUFACTURER"].unique():
    if mfr not in mfr_cache:
        cur.execute("INSERT INTO dim_manufacturer VALUES (:1,:2)", (next_mfr_key, mfr))
        mfr_cache[mfr] = next_mfr_key
        next_mfr_key += 1
        mfrs_added += 1
        print(f"   ➕ Added manufacturer: {mfr}")

conn.commit()
print(f"\n   ✅ Total manufacturers: {len(mfr_cache)} (Added: {mfrs_added})\n")

# ========================================
# LOAD PRODUCT DIMENSION
# ========================================
print("📦 Loading Product Dimension...\n")

cur.execute("SELECT product_key, product_name, brand FROM dim_product_dw")
product_cache = {(r[1], r[2]): r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(product_key),0) FROM dim_product_dw")
next_product_key = cur.fetchone()[0] + 1

insert_count = 0
error_count = 0
skip_count = 0

for _, row in df_valid.drop_duplicates(subset=["PRODUCT_NAME", "BRAND"]).iterrows():
    bk = (row["PRODUCT_NAME"], row["BRAND"])

    if bk not in product_cache:
        try:
            price_value = Decimal(str(row["PRODUCT_UNIT_PRICE_CLEAN"]))
            
            cur.execute("""
                INSERT INTO dim_product_dw (
                    product_key, product_name, category_key, sub_category_key,
                    manufacturer_key, brand, flavour, product_size,
                    stock_quantity_code, unit_of_measure, unit_price
                ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)
            """, (
                next_product_key,
                row["PRODUCT_NAME"],
                category_cache[row["CATEGORY_CLEAN"]],
                subcat_cache[row["SUB_CATEGORY_CLEAN"]],
                mfr_cache[row["MANUFACTURER"]],
                row["BRAND"],
                row["FLAVOUR"] if row["FLAVOUR"] != "Unknown" else None,
                row["PRODUCT_SIZE"],
                row["SQC"],
                row["UOM"],
                price_value
            ))

            next_product_key += 1
            insert_count += 1
            
            if insert_count % 100 == 0:
                print(f"   ⏳ Processed {insert_count} products...")
            
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Show first 5 errors only
                print(f"   ⚠️  Error inserting {row['PRODUCT_NAME']}: {e}")
            continue
    else:
        skip_count += 1

conn.commit()

# ========================================
# FINAL STATISTICS
# ========================================
print(f"\n{'='*70}")
print(f"LOAD STATISTICS")
print(f"{'='*70}\n")

print(f"📊 Product Dimension Load Summary:")
print(f"   Products inserted: {insert_count:,}")
print(f"   Products skipped (duplicates): {skip_count:,}")
print(f"   Errors: {error_count:,}")

print(f"\n📊 Dimension Tables:")
print(f"   Categories: {len(category_cache)}")
print(f"   Sub-categories: {len(subcat_cache)}")
print(f"   Manufacturers: {len(mfr_cache)}")

# Get table statistics
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT category_key) as cats,
        COUNT(DISTINCT sub_category_key) as subcats,
        COUNT(DISTINCT brand) as brands,
        MIN(unit_price) as min_p,
        MAX(unit_price) as max_p,
        AVG(unit_price) as avg_p
    FROM dim_product_dw
""")

stats = cur.fetchone()

print(f"\n📈 dim_product_dw Table Statistics:")
print(f"   Total products: {stats[0]:,}")
print(f"   Unique categories: {stats[1]}")
print(f"   Unique sub-categories: {stats[2]}")
print(f"   Unique brands: {stats[3]}")
print(f"   Price range: ₹{stats[4]:.2f} - ₹{stats[5]:.2f}")
print(f"   Average price: ₹{stats[6]:.2f}")

# Category breakdown
cur.execute("""
    SELECT c.category_name, COUNT(*) as cnt
    FROM dim_product_dw p
    JOIN dim_category c ON p.category_key = c.category_key
    GROUP BY c.category_name
    ORDER BY cnt DESC
""")

print(f"\n📦 Products by Category:")
for row in cur.fetchall():
    print(f"   {row[0]:.<25} {row[1]:>6,} products")

cur.close()
conn.close()

print(f"\n{'='*70}")
print(f"🎉 PRODUCT DIM LOAD COMPLETED SUCCESSFULLY")
print(f"{'='*70}")
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n💡 Schedule this script weekly to keep the data warehouse current")
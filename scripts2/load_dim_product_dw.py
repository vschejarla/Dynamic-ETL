import os
import random
import pandas as pd
import oracledb

# ---------------------------------------
# Config
# ---------------------------------------
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

print("🚀 DIM PRODUCT LOAD STARTED")

# ---------------------------------------
# Manufacturer Mapping
# ---------------------------------------
MANUFACTURER_MAP = {
    "PersonalCare": ["Hindustan Unilever Ltd", "Procter & Gamble", "Dabur India Ltd"],
    "BabyCare": ["Johnson & Johnson", "Procter & Gamble"],
    "Grocery": ["Tata Consumer Products", "Nestlé India", "Britannia Industries"],
    "Soap": ["Godrej Consumer Products", "Hindustan Unilever Ltd"],
    "Pulses": ["Tata Consumer Products", "Patanjali Ayurved"]
}

def get_manufacturer(category):
    return random.choice(MANUFACTURER_MAP.get(category, ["Generic Manufacturer"]))

# ---------------------------------------
# 1. Read latest incoming file
# ---------------------------------------
files = sorted(
    [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")],
    reverse=True
)

if not files:
    raise FileNotFoundError("❌ No incoming files found")

file_path = os.path.join(INCOMING_DIR, files[0])
print(f"📄 Reading file: {file_path}")

df = pd.read_csv(file_path, delimiter="|")
print(f"📊 Total records in file: {len(df)}")

# ---------------------------------------
# 2. Clean numeric columns
# ---------------------------------------
numeric_cols = ["PRODUCT_UNIT_PRICE"]

for col in numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace("", None)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Drop rows where PRODUCT_UNIT_PRICE is null/invalid
df = df.dropna(subset=["PRODUCT_UNIT_PRICE"])

# Additional validation: ensure positive prices
df = df[df["PRODUCT_UNIT_PRICE"] > 0]

print(f"✅ Valid records after numeric cleaning: {len(df)}")

# ---------------------------------------
# 3. Prepare product dataframe
# CRITICAL: Do NOT normalize - must match raw data in fact table
# ---------------------------------------
product_cols = [
    "PRODUCT_NAME",
    "CATEGORY",
    "SUB_CATEGORY",
    "BRAND",
    "FLAVOUR",
    "PRODUCT_SIZE",
    "SQC",
    "UOM",
    "PRODUCT_UNIT_PRICE"
]

# Only strip whitespace, do NOT convert to uppercase
for col in product_cols:
    if col in df.columns and col != "PRODUCT_UNIT_PRICE":
        df[col] = df[col].astype(str).str.strip()

product_df = df[product_cols].drop_duplicates()
print(f"📊 Unique products in file: {len(product_df)}")

product_df["MANUFACTURER"] = product_df["CATEGORY"].apply(get_manufacturer)

# ---------------------------------------
# 4. DIM_CATEGORY
# ---------------------------------------
cur.execute("SELECT category_key, category_name FROM dim_category")
category_cache = {r[1]: r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(category_key),0) FROM dim_category")
next_category_key = cur.fetchone()[0] + 1

for category in product_df["CATEGORY"].dropna().unique():
    if category not in category_cache and category != 'nan':
        cur.execute(
            "INSERT INTO dim_category VALUES (:1, :2)",
            (next_category_key, category)
        )
        category_cache[category] = next_category_key
        next_category_key += 1

conn.commit()
print(f"📦 Categories in cache: {len(category_cache)}")

# ---------------------------------------
# 5. DIM_SUB_CATEGORY
# ---------------------------------------
cur.execute("SELECT sub_category_key, sub_category_name FROM dim_sub_category")
sub_category_cache = {r[1]: r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(sub_category_key),0) FROM dim_sub_category")
next_sub_category_key = cur.fetchone()[0] + 1

for sub_cat in product_df["SUB_CATEGORY"].dropna().unique():
    if sub_cat not in sub_category_cache and sub_cat != 'nan':
        cur.execute(
            "INSERT INTO dim_sub_category VALUES (:1, :2)",
            (next_sub_category_key, sub_cat)
        )
        sub_category_cache[sub_cat] = next_sub_category_key
        next_sub_category_key += 1

conn.commit()
print(f"📦 Sub-categories in cache: {len(sub_category_cache)}")

# ---------------------------------------
# 6. DIM_MANUFACTURER
# ---------------------------------------
cur.execute("SELECT manufacturer_key, manufacturer_name FROM dim_manufacturer")
manufacturer_cache = {r[1]: r[0] for r in cur.fetchall()}

cur.execute("SELECT NVL(MAX(manufacturer_key),0) FROM dim_manufacturer")
next_manufacturer_key = cur.fetchone()[0] + 1

for mfr in product_df["MANUFACTURER"].dropna().unique():
    if mfr not in manufacturer_cache and mfr != 'nan':
        cur.execute(
            "INSERT INTO dim_manufacturer VALUES (:1, :2)",
            (next_manufacturer_key, mfr)
        )
        manufacturer_cache[mfr] = next_manufacturer_key
        next_manufacturer_key += 1

conn.commit()
print(f"📦 Manufacturers in cache: {len(manufacturer_cache)}")

# ---------------------------------------
# 7. DIM_PRODUCT_DW
# CRITICAL: Business key must match fact table lookup:
# (product_name, brand) - SIMPLE 2-column key
# ---------------------------------------
cur.execute("""
    SELECT product_key, product_name, brand
    FROM dim_product_dw
""")

product_cache = {
    (r[1], r[2]): r[0]
    for r in cur.fetchall()
}
print(f"📦 Existing products: {len(product_cache)}")

cur.execute("SELECT NVL(MAX(product_key),0) FROM dim_product_dw")
next_product_key = cur.fetchone()[0] + 1

inserted_count = 0
skipped_count = 0

for _, row in product_df.iterrows():
    
    # Business key - MUST match what fact table uses
    business_key = (
        row["PRODUCT_NAME"],
        row["BRAND"]
    )

    if business_key not in product_cache:
        try:
            cur.execute(
                """
                INSERT INTO dim_product_dw (
                    product_key,
                    product_name,
                    category_key,
                    sub_category_key,
                    manufacturer_key,
                    brand,
                    flavour,
                    product_size,
                    stock_quantity_code,
                    unit_of_measure,
                    unit_price
                )
                VALUES (
                    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11
                )
                """,
                (
                    next_product_key,
                    row["PRODUCT_NAME"],
                    category_cache.get(row["CATEGORY"]),
                    sub_category_cache.get(row["SUB_CATEGORY"]),
                    manufacturer_cache.get(row["MANUFACTURER"]),
                    row["BRAND"],
                    row["FLAVOUR"] if row["FLAVOUR"] != 'nan' else None,
                    row["PRODUCT_SIZE"] if row["PRODUCT_SIZE"] != 'nan' else None,
                    row["SQC"] if row["SQC"] != 'nan' else None,
                    row["UOM"] if row["UOM"] != 'nan' else None,
                    float(row["PRODUCT_UNIT_PRICE"])
                )
            )

            product_cache[business_key] = next_product_key
            next_product_key += 1
            inserted_count += 1
        except Exception as e:
            print(f"⚠️ Failed to insert product: {row['PRODUCT_NAME']} - {str(e)}")
            skipped_count += 1
            continue
    else:
        skipped_count += 1

conn.commit()

print(f"\n📊 Summary:")
print(f"   Products inserted: {inserted_count}")
print(f"   Products skipped (already exist): {skipped_count}")

# Verify
cur.execute("SELECT COUNT(*) FROM dim_product_dw")
final_count = cur.fetchone()[0]
print(f"   Final row count in dim_product_dw: {final_count}")

cur.close()
conn.close()

print("✅ DIM PRODUCT LOAD COMPLETED SUCCESSFULLY")
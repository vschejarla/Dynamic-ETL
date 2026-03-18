import oracledb
import pandas as pd
from decimal import Decimal, InvalidOperation
import re
from datetime import datetime

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
# BRAND → MANUFACTURER MAPPING
#
# FIXES APPLIED (14 errors corrected):
#   Sundrop      : Ruchi Soya → Adani Wilmar (Sundrop is Adani Wilmar's brand)
#   Uncle Chipps : ITC → PepsiCo (PepsiCo India brand)
#   Wheel        : ITC → Hindustan Unilever (HUL detergent brand)
#   Exo          : ITC → Hindustan Unilever (HUL dishwash brand)
#   Colin        : ITC → Reckitt Benckiser (Reckitt glass cleaner)
#   Paper Boat   : ITC → Hector Beverages (made by Hector Beverages)
#   Navratna     : Patanjali → Emami Limited (Emami hair oil brand)
#   Glucon-D     : Heinz India → Zydus Wellness (acquired by Zydus in 2019)
#   Pears        : P&G → Hindustan Unilever (HUL soap brand in India)
#   B Natural    : Parle Agro → ITC Limited (ITC juice brand)
#   Annapurna    : Generic → Hindustan Unilever (HUL atta/salt brand)
#   Sensodyne    : GlaxoSmithKline → Haleon (GSK Consumer Healthcare → Haleon 2022)
#   Kraft        : Kraft Heinz → Mondelez International (distributed via Mondelez in India)
#   Mee Mee      : ITC → ME N MOMS Pvt Ltd (standalone baby care company)
# ========================================
BRAND_TO_MANUFACTURER = {
    # Grocery - Rice
    "India Gate":       "KRBL Limited",
    "Daawat":           "LT Foods",
    "Kohinoor":         "Kohinoor Foods",
    "Fortune":          "Adani Wilmar",
    "Aashirvaad":       "ITC Limited",

    # Grocery - Wheat Flour
    "Pillsbury":        "General Mills",
    "Nature Fresh":     "Hindustan Unilever",
    "Annapurna":        "Hindustan Unilever",       # FIX: was Generic

    # Grocery - Edible Oil
    "Dhara":            "Adani Wilmar",
    "Sundrop":          "Adani Wilmar",             # FIX: was Ruchi Soya Industries
    "Saffola":          "Marico",
    "Gemini":           "Cargill",

    # Grocery - Pulses & Spices
    "Tata Sampann":     "Tata Consumer Products",
    "MDH":              "MDH",
    "Everest":          "Everest",
    "Catch":            "Catch",

    # Grocery - Biscuits
    "Parle":            "Parle Products",
    "Britannia":        "Britannia Industries",
    "Sunfeast":         "ITC Limited",
    "Priya Gold":       "Priya Gold",

    # Grocery - Noodles
    "Maggi":            "Nestlé",
    "Top Ramen":        "Nissin Foods",
    "Yippee":           "ITC Limited",
    "Knorr":            "Unilever",

    # Grocery - Snacks
    "Haldiram's":       "Haldiram's",
    "Bikaji":           "Bikaji Foods International",
    "Balaji":           "Balaji Wafers",
    "Lay's":            "PepsiCo",
    "Uncle Chipps":     "PepsiCo",                 # FIX: was ITC Limited
    "Bingo":            "ITC Limited",
    "Pringles":         "Kellanova",
    "Kurkure":          "PepsiCo",

    # Beverage - Soft Drinks
    "Coca Cola":        "Coca-Cola",
    "Thums Up":         "Coca-Cola",
    "Sprite":           "Coca-Cola",
    "Fanta":            "Coca-Cola",
    "Limca":            "Coca-Cola",
    "Maaza":            "Coca-Cola",
    "Pepsi":            "PepsiCo",
    "Mountain Dew":     "PepsiCo",
    "7 Up":             "PepsiCo",
    "Mirinda":          "PepsiCo",
    "Frooti":           "Parle Agro",
    "Appy Fizz":        "Parle Agro",
    "Bailley":          "Parle Agro",

    # Beverage - Juice
    "Tropicana":        "PepsiCo",
    "Real":             "Dabur",
    "Paper Boat":       "Hector Beverages",         # FIX: was ITC Limited
    "Minute Maid":      "Coca-Cola",
    "B Natural":        "ITC Limited",              # FIX: was Parle Agro

    # Beverage - Tea
    "Tata Tea":         "Tata Consumer Products",
    "Tetley":           "Tata Consumer Products",
    "Red Label":        "Hindustan Unilever",
    "Taj Mahal":        "Hindustan Unilever",
    "Lipton":           "Unilever",
    "Brooke Bond":      "Hindustan Unilever",
    "Wagh Bakri":       "Wagh Bakri",
    "Girnar":           "Girnar",

    # Beverage - Coffee
    "Nescafe":          "Nestlé",
    "Bru":              "Hindustan Unilever",
    "Tata Coffee":      "Tata Consumer Products",
    "Continental":      "Continental Coffee",

    # Beverage - Energy Drinks
    "Red Bull":         "Red Bull GmbH",
    "Monster":          "Monster Beverage",
    "Sting":            "PepsiCo",
    "Gatorade":         "PepsiCo",
    "Glucon-D":         "Zydus Wellness",           # FIX: was Heinz India (acquired 2019)
    "Powerade":         "Coca-Cola",

    # Dairy
    "Amul":             "Gujarat Cooperative Milk Marketing Federation",
    "Mother Dairy":     "Mother Dairy",
    "Nestle":           "Nestlé",
    "Heritage":         "Heritage Foods",
    "Danone":           "Danone",
    "Go Cheese":        "Go Cheese",
    "Kraft":            "Mondelez International",   # FIX: was Kraft Heinz
    "Nandini":          "Karnataka Milk Federation",
    "Govardhan":        "Govardhan",

    # Personal Care - Shampoo / Hair
    "Clinic Plus":      "Hindustan Unilever",
    "Dove":             "Hindustan Unilever",
    "Sunsilk":          "Hindustan Unilever",
    "Pantene":          "Procter & Gamble",
    "Head & Shoulders": "Procter & Gamble",
    "Vivel":            "ITC Limited",
    "Parachute":        "Marico",
    "Bajaj":            "Bajaj Corp",
    "Navratna":         "Emami Limited",            # FIX: was Patanjali Ayurved
    "Dabur":            "Dabur",

    # Personal Care - Soap
    "Lux":              "Hindustan Unilever",
    "Lifebuoy":         "Hindustan Unilever",
    "Pears":            "Hindustan Unilever",       # FIX: was Procter & Gamble
    "Dettol":           "Reckitt Benckiser",

    # Personal Care - Oral Care
    "Colgate":          "Colgate-Palmolive",
    "Pepsodent":        "Hindustan Unilever",
    "Close-Up":         "Hindustan Unilever",
    "Sensodyne":        "Haleon",                   # FIX: was GlaxoSmithKline (renamed 2022)

    # Personal Care - Skin / Face
    "Garnier":          "L'Oréal",
    "Himalaya":         "The Himalaya Drug Company",
    "Neutrogena":       "Johnson & Johnson",
    "Nivea":            "Beiersdorf",

    # Home Care
    "Surf Excel":       "Hindustan Unilever",
    "Rin":              "Hindustan Unilever",
    "Ariel":            "Procter & Gamble",
    "Tide":             "Procter & Gamble",
    "Wheel":            "Hindustan Unilever",       # FIX: was ITC Limited
    "Vim":              "Hindustan Unilever",
    "Finish":           "Reckitt Benckiser",
    "Exo":              "Hindustan Unilever",       # FIX: was ITC Limited
    "Pril":             "Henkel",
    "Lizol":            "Reckitt Benckiser",
    "Domex":            "Hindustan Unilever",
    "Colin":            "Reckitt Benckiser",        # FIX: was ITC Limited
    "Harpic":           "Reckitt Benckiser",

    # Baby Care
    "Johnson & Johnson": "Johnson & Johnson",
    "Mee Mee":          "ME N MOMS Pvt Ltd",        # FIX: was ITC Limited
    "Chicco":           "Chicco",
    "Sebamed":          "Beiersdorf",
    "Pampers":          "Procter & Gamble",
    "Huggies":          "Kimberly-Clark",
    "MamyPoko":         "Kao Corporation",
}

# ========================================
# CATEGORY / SUBCATEGORY STANDARDIZATION
#
# NOTE: Source (dim_product_daily) produces these exact strings:
#   Categories  : Grocery, Beverage, Dairy, Personal Care, Home Care, Baby Care
#   Subcategories: Oils, Rice, Atta, Pulses, Spices, Biscuits, Noodles, Snacks,
#                  Soft Drink, Juice, Tea, Coffee, Energy Drink,
#                  Milk, Curd, Cheese, Butter, Paneer,
#                  Shampoo, Soap, Toothpaste, Hair Oil, Face Wash,
#                  Detergent, Dishwash, Floor Cleaner, Toilet Cleaner,
#                  Baby Soap, Baby Powder, Baby Wipes, Diapers
#
# All source values resolve correctly through the maps below.
# "Personal Care"→"PersonalCare", "Home Care"→"HomeCare", "Baby Care"→"BabyCare"
# "Oils"→"Edible Oil", "Atta"→"Wheat Flour" are the only non-trivial transforms.
# ========================================
CATEGORY_STANDARD_MAP = {
    "PERSONALCARE":  "PersonalCare",
    "PERSONAL CARE": "PersonalCare",
    "BABYCARE":      "BabyCare",
    "BABY CARE":     "BabyCare",
    "GROCERY":       "Grocery",
    "FOOD":          "Grocery",
    "BEVERAGE":      "Beverage",
    "DRINKS":        "Beverage",
    "DAIRY":         "Dairy",
    "HOMECARE":      "HomeCare",
    "HOME CARE":     "HomeCare",
}

SUBCATEGORY_STANDARD_MAP = {
    # Personal Care
    "SHAMPOO":        "Shampoo",
    "SOAP":           "Soap",
    "TOOTHPASTE":     "Toothpaste",
    "FACE WASH":      "Face Wash",
    "HAIR OIL":       "Hair Oil",
    # Baby Care
    "BABY POWDER":    "Baby Powder",
    "BABY WIPES":     "Baby Wipes",
    "DIAPERS":        "Diapers",
    "BABY SOAP":      "Baby Soap",
    # Grocery
    "RICE":           "Rice",
    "WHEAT FLOUR":    "Wheat Flour",
    "ATTA":           "Wheat Flour",
    "OIL":            "Edible Oil",
    "COOKING OIL":    "Edible Oil",
    "EDIBLE OIL":     "Edible Oil",
    "OILS":           "Edible Oil",
    "PULSES":         "Pulses",
    "DAL":            "Pulses",
    "SPICES":         "Spices",
    "MASALA":         "Spices",
    "BISCUITS":       "Biscuits",
    "COOKIES":        "Biscuits",
    "NOODLES":        "Noodles",
    "SNACKS":         "Snacks",
    # Beverage
    "SOFT DRINK":     "Soft Drink",
    "JUICE":          "Juice",
    "TEA":            "Tea",
    "COFFEE":         "Coffee",
    "ENERGY DRINK":   "Energy Drink",
    "FLAVOURED MILK": "Flavoured Milk",
    # Dairy
    "MILK":           "Milk",
    "CURD":           "Curd",
    "CHEESE":         "Cheese",
    "BUTTER":         "Butter",
    "PANEER":         "Paneer",
    # Home Care
    "DETERGENT":      "Detergent",
    "DISHWASH":       "Dishwash",
    "FLOOR CLEANER":  "Floor Cleaner",
    "TOILET CLEANER": "Toilet Cleaner",
}


# ========================================
# HELPERS
# ========================================
def clean_text(val):
    if pd.isna(val) or val is None or val == '':
        return None
    return str(val).strip()


def standardize_category(raw):
    if not raw:
        return "General"
    return CATEGORY_STANDARD_MAP.get(raw.upper().strip(), raw.strip())


def standardize_subcategory(raw):
    if not raw:
        return "General"
    return SUBCATEGORY_STANDARD_MAP.get(raw.upper().strip(), raw.strip())


def get_manufacturer(brand):
    if not brand:
        return "Generic"
    brand_clean = brand.strip()
    # exact match first, then case-insensitive fallback
    if brand_clean in BRAND_TO_MANUFACTURER:
        return BRAND_TO_MANUFACTURER[brand_clean]
    for key, mfr in BRAND_TO_MANUFACTURER.items():
        if key.upper() == brand_clean.upper():
            return mfr
    return "Generic"


def clean_price(val):
    if pd.isna(val) or val is None:
        return None
    try:
        val_str = re.sub(r'[₹$,\s]', '', str(val).strip())
        if not val_str:
            return None
        price = Decimal(val_str)
        return float(price) if 0 < price <= 100000 else None
    except (ValueError, InvalidOperation, TypeError):
        return None


# ========================================
# CONNECT
# ========================================
try:
    source_conn = oracledb.connect(**SOURCE_DB_CONFIG)
    source_cur  = source_conn.cursor()
except Exception as e:
    raise RuntimeError(f"Source connection failed: {e}") from e

try:
    target_conn = oracledb.connect(**TARGET_DB_CONFIG)
    target_cur  = target_conn.cursor()
except Exception as e:
    source_cur.close(); source_conn.close()
    raise RuntimeError(f"Target connection failed: {e}") from e


def _close_all():
    for obj in (source_cur, source_conn, target_cur, target_conn):
        try:
            obj.close()
        except Exception:
            pass


# ========================================
# EXTRACT
# ========================================
try:
    source_cur.execute("""
        SELECT product_id, product_name, category, sub_category,
               brand, flavour, product_size, sku, uom, unit_price, business_stage
        FROM   dim_product
        ORDER BY product_id
    """)
    source_data = source_cur.fetchall()
except Exception as e:
    _close_all()
    raise RuntimeError(f"Extract failed: {e}") from e

# ========================================
# TRANSFORM
# ========================================
df = pd.DataFrame(source_data, columns=[
    'PRODUCT_ID','PRODUCT_NAME','CATEGORY','SUB_CATEGORY',
    'BRAND','FLAVOUR','PRODUCT_SIZE','SKU','UOM','UNIT_PRICE','BUSINESS_STAGE'
])

df['CATEGORY_CLEAN']     = df['CATEGORY'].apply(standardize_category)
df['SUB_CATEGORY_CLEAN'] = df['SUB_CATEGORY'].apply(standardize_subcategory)
df['MANUFACTURER']       = df['BRAND'].apply(get_manufacturer)
df['BRAND_CLEAN']        = df['BRAND'].apply(clean_text)
df['FLAVOUR_CLEAN']      = df['FLAVOUR'].apply(clean_text)
df['PRICE_CLEAN']        = df['UNIT_PRICE'].apply(clean_price)

df_valid = df[
    df['PRODUCT_NAME'].notna() &
    df['BRAND_CLEAN'].notna() &
    df['PRICE_CLEAN'].notna()
].copy()

# Remove source duplicates (keep last occurrence)
before_dedup = len(df_valid)
df_valid = df_valid.drop_duplicates(
    subset=['PRODUCT_NAME','BRAND_CLEAN','PRODUCT_SIZE'], keep='last'
)
duplicates_removed = before_dedup - len(df_valid)

# ========================================
# STAGING TABLE
# ========================================
try:
    target_cur.execute("DROP TABLE product_staging")
    target_conn.commit()
except Exception:
    pass   # table didn't exist — fine

target_cur.execute("""
    CREATE TABLE product_staging (
        source_product_id  NUMBER,
        product_key        NUMBER,
        product_name       VARCHAR2(100),
        category_key       NUMBER,
        sub_category_key   NUMBER,
        manufacturer_key   NUMBER,
        brand              VARCHAR2(50),
        flavour            VARCHAR2(50),
        product_size       VARCHAR2(20),
        sku                VARCHAR2(50),
        unit_of_measure    VARCHAR2(10),
        unit_price         NUMBER(10,2),
        business_stage     VARCHAR2(20),
        operation          VARCHAR2(10),
        CONSTRAINT pk_staging PRIMARY KEY (product_name, brand, product_size)
    )
""")
target_conn.commit()

# ========================================
# LOAD DIMENSION TABLES  (Category / SubCategory / Manufacturer)
# Helper: upsert into a dim table, return updated cache
# ========================================
def upsert_dim(cur, conn, table, key_col, name_col, values):
    """
    Load all new values into a dimension table.
    Returns {name: key} cache.
    """
    cur.execute(f"SELECT {key_col}, {name_col} FROM {table}")
    cache = {r[1]: r[0] for r in cur.fetchall()}
    cur.execute(f"SELECT NVL(MAX({key_col}), 0) FROM {table}")
    next_key = cur.fetchone()[0] + 1

    new_entries = []
    for val in values:
        if val not in cache:
            cache[val] = next_key
            new_entries.append((next_key, val))
            next_key += 1

    if new_entries:
        cur.executemany(f"INSERT INTO {table} VALUES (:1, :2)", new_entries)
        conn.commit()

    return cache


category_cache = upsert_dim(
    target_cur, target_conn,
    "dim_category", "category_key", "category_name",
    df_valid['CATEGORY_CLEAN'].unique()
)

subcat_cache = upsert_dim(
    target_cur, target_conn,
    "dim_sub_category", "sub_category_key", "sub_category_name",
    df_valid['SUB_CATEGORY_CLEAN'].unique()
)

mfr_cache = upsert_dim(
    target_cur, target_conn,
    "dim_manufacturer", "manufacturer_key", "manufacturer_name",
    df_valid['MANUFACTURER'].unique()
)

# ========================================
# DETERMINE INSERT vs UPDATE
# ========================================
target_cur.execute("""
    SELECT product_key, product_name, brand, category_key, sub_category_key,
           manufacturer_key, flavour, product_size, sku,
           unit_of_measure, unit_price, business_stage
    FROM   dim_product_dw
""")
product_cache = {}
for r in target_cur.fetchall():
    bk = (r[1], r[2], r[7])
    product_cache[bk] = {
        'product_key':      r[0],
        'category_key':     r[3],
        'sub_category_key': r[4],
        'manufacturer_key': r[5],
        'flavour':          r[6],
        'sku':              r[8],
        'unit_of_measure':  r[9],
        'unit_price':       float(r[10]) if r[10] is not None else None,
        'business_stage':   r[11],
    }

target_cur.execute("SELECT NVL(MAX(product_key), 0) FROM dim_product_dw")
next_product_key = target_cur.fetchone()[0] + 1

insert_products = []
update_products = []

for _, row in df_valid.iterrows():
    bk = (row['PRODUCT_NAME'], row['BRAND_CLEAN'], row['PRODUCT_SIZE'])

    # FIX: validate that dimension keys resolved; skip row if any are missing
    cat_key    = category_cache.get(row['CATEGORY_CLEAN'])
    subcat_key = subcat_cache.get(row['SUB_CATEGORY_CLEAN'])
    mfr_key    = mfr_cache.get(row['MANUFACTURER'])
    if cat_key is None or subcat_key is None or mfr_key is None:
        continue   # prevents NULL FK insertion

    if bk not in product_cache:
        insert_products.append({
            'product_key': next_product_key,
            'cat_key':     cat_key,
            'subcat_key':  subcat_key,
            'mfr_key':     mfr_key,
            'bk':          bk,
            'data':        row,
        })
        next_product_key += 1
    else:
        existing = product_cache[bk]

        # FIX: treat both-None price as no change (was triggering spurious updates)
        ep = existing['unit_price']
        np_ = row['PRICE_CLEAN']
        if ep is None and np_ is None:
            price_changed = False
        elif ep is None or np_ is None:
            price_changed = True
        else:
            price_changed = abs(ep - np_) > 0.01

        changed = (
            existing['category_key']      != cat_key    or
            existing['sub_category_key']  != subcat_key or
            existing['manufacturer_key']  != mfr_key    or
            (existing['flavour'] or '')   != (row['FLAVOUR_CLEAN'] or '') or
            existing['sku']               != row['SKU'] or
            existing['unit_of_measure']   != row['UOM'] or
            (existing['business_stage'] or '') != (row['BUSINESS_STAGE'] or '') or
            price_changed
        )

        if changed:
            update_products.append({
                'product_key': existing['product_key'],
                'cat_key':     cat_key,
                'subcat_key':  subcat_key,
                'mfr_key':     mfr_key,
                'bk':          bk,
                'data':        row,
            })

# ========================================
# LOAD STAGING
# ========================================
staging_data = []

for p in insert_products:
    row = p['data']
    try:
        staging_data.append((
            int(row['PRODUCT_ID']),
            p['product_key'],
            row['PRODUCT_NAME'],
            p['cat_key'],
            p['subcat_key'],
            p['mfr_key'],
            row['BRAND_CLEAN'],
            row['FLAVOUR_CLEAN'],
            row['PRODUCT_SIZE'],
            row['SKU'],
            row['UOM'],
            Decimal(str(row['PRICE_CLEAN'])),
            row['BUSINESS_STAGE'],
            'INSERT',
        ))
    except Exception as e:
        print(f"  [WARN] Skipped INSERT for {row['PRODUCT_NAME']}: {e}")

for p in update_products:
    row = p['data']
    try:
        staging_data.append((
            int(row['PRODUCT_ID']),
            p['product_key'],
            row['PRODUCT_NAME'],
            p['cat_key'],
            p['subcat_key'],
            p['mfr_key'],
            row['BRAND_CLEAN'],
            row['FLAVOUR_CLEAN'],
            row['PRODUCT_SIZE'],
            row['SKU'],
            row['UOM'],
            Decimal(str(row['PRICE_CLEAN'])) if row['PRICE_CLEAN'] is not None else None,
            row['BUSINESS_STAGE'],
            'UPDATE',
        ))
    except Exception as e:
        print(f"  [WARN] Skipped UPDATE for {row['PRODUCT_NAME']}: {e}")

# FIX: initialise rows_merged so summary print never hits NameError
# when staging_data is empty (no inserts or updates detected).
rows_merged = 0

if staging_data:
    BATCH_SIZE = 1000
    try:
        for i in range(0, len(staging_data), BATCH_SIZE):
            target_cur.executemany("""
                INSERT INTO product_staging (
                    source_product_id, product_key, product_name,
                    category_key, sub_category_key, manufacturer_key,
                    brand, flavour, product_size, sku,
                    unit_of_measure, unit_price, business_stage, operation
                ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)
            """, staging_data[i:i + BATCH_SIZE])
        # FIX: single commit after all batches — prevents partial commits on failure
        target_conn.commit()
    except Exception as e:
        target_conn.rollback()
        _close_all()
        raise RuntimeError(f"Staging insert failed: {e}") from e

# ========================================
# MERGE INTO dim_product_dw
# NOTE: product_key is pre-assigned in Python (not via ROW_NUMBER inside SQL).
#       This prevents ORA-00001 unique constraint violations that occur when
#       ROW_NUMBER() reassigns keys for ALL staging rows (inserts + updates).
# ========================================
if staging_data:
    try:
        target_cur.execute("""
            MERGE INTO dim_product_dw tgt
            USING (
                SELECT product_key, product_name, category_key, sub_category_key,
                       manufacturer_key, brand, flavour, product_size,
                       sku, unit_of_measure, unit_price, business_stage, operation
                FROM   product_staging
            ) stg
            ON (
                tgt.product_name  = stg.product_name AND
                tgt.brand         = stg.brand         AND
                tgt.product_size  = stg.product_size
            )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.category_key     = stg.category_key,
                    tgt.sub_category_key = stg.sub_category_key,
                    tgt.manufacturer_key = stg.manufacturer_key,
                    tgt.flavour          = stg.flavour,
                    tgt.sku              = stg.sku,
                    tgt.unit_of_measure  = stg.unit_of_measure,
                    tgt.unit_price       = stg.unit_price,
                    tgt.business_stage   = stg.business_stage
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
    except Exception as e:
        target_conn.rollback()
        _close_all()
        raise RuntimeError(f"MERGE failed: {e}") from e

# ========================================
# CLEANUP STAGING
# FIX: wrapped in try/except so a missing table doesn't abort the run
# ========================================
try:
    target_cur.execute("DROP TABLE product_staging")
    target_conn.commit()
except Exception:
    pass

# ========================================
# FINAL STATISTICS
# ========================================
target_cur.execute("""
    SELECT COUNT(*) as total,
           COUNT(DISTINCT category_key)     as cats,
           COUNT(DISTINCT sub_category_key) as subcats,
           COUNT(DISTINCT manufacturer_key) as mfrs,
           COUNT(DISTINCT brand)            as brands,
           COUNT(DISTINCT business_stage)   as stages,
           MIN(unit_price)                  as min_price,
           MAX(unit_price)                  as max_price,
           AVG(unit_price)                  as avg_price
    FROM   dim_product_dw
""")
stats = target_cur.fetchone()

_close_all()

# ========================================
# SUMMARY PRINT  (minimal — all signal, no decoration)
# ========================================
print(
    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
    f"Extracted: {len(source_data):,} | "
    f"Valid: {len(df_valid):,} | "
    f"Dupes removed: {duplicates_removed} | "
    f"Inserted: {len(insert_products):,} | "
    f"Updated: {len(update_products):,} | "
    f"Merged rows: {rows_merged:,} | "
    f"Total in DW: {stats[0]:,}"
)
print(
    f"  DW: {stats[1]} categories | {stats[2]} sub-cats | "
    f"{stats[3]} manufacturers | {stats[4]} brands | "
    f"Price ₹{stats[6]:.0f}–₹{stats[7]:.0f} avg ₹{stats[8]:.0f}"
)
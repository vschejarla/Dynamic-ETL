import random
import oracledb
from datetime import datetime
from decimal import Decimal, InvalidOperation

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

# ========================================
# GET NEXT SALES_ID
# ========================================
cur.execute("SELECT NVL(MAX(sales_id), 0) FROM fact_sales")
start_id = cur.fetchone()[0]

# ========================================
# VALIDATE TODAY EXISTS IN DIM_DATE
# ========================================
today_date_id = int(datetime.now().strftime("%Y%m%d"))
cur.execute(
    "SELECT date_id, day_name, is_weekend FROM dim_date WHERE date_id = :1",
    [today_date_id]
)
row = cur.fetchone()
if not row:
    cur.close(); conn.close()
    raise SystemExit(
        f"date_id {today_date_id} not found in dim_date. "
        "Run dim_date_daily.py first."
    )

date_id, day_name, is_weekend = row

# ========================================
# LOAD DIMENSION DATA
# dim_store_master has no active_flag column — fetch all stores
#  filter products to non-NULL unit_price only
# ========================================
cur.execute("""
    SELECT store_id, store_class_of_trade, is_chain
    FROM   dim_store_master
""")
stores = cur.fetchall()

cur.execute("""
    SELECT product_id, category, sub_category, unit_price
    FROM   dim_product
    WHERE  unit_price IS NOT NULL AND unit_price > 0
""")
products = cur.fetchall()

cur.execute("""
    SELECT distributor_id, distributor_type
    FROM   dim_distributor
    WHERE  active_flag = 'Y'
""")
distributors = cur.fetchall()

if not stores or not products or not distributors:
    cur.close(); conn.close()
    raise ValueError("Missing dimension data. Load dimension tables first.")

# ========================================
# BUSINESS LOGIC
# ========================================

# Quantity ranges per product category
CATEGORY_PURCHASE_QTY = {
    "Grocery":      (1, 5),
    "Beverage":     (2, 12),
    "Dairy":        (1, 6),
    "Personal Care":(1, 4),
    "Baby Care":    (1, 3),
    "Home Care":    (1, 4),
}

def get_quantity_multiplier(store_class):
    """Wholesale/Cash & Carry stores buy in bulk."""
    if "Wholesale" in store_class or "Cash & Carry" in store_class:
        return random.randint(5, 20)
    return 1

def get_weekend_boost():
    """FIX 3: was defined but never applied — now used in quantity calc."""
    return random.uniform(1.2, 1.4) if is_weekend == "Y" else 1.0

def get_seasonal_boost():
    """FIX 3: was defined but never applied — now used in quantity calc."""
    m = datetime.now().month
    if m in (10, 11, 12):       # Festival season
        return random.uniform(1.3, 1.6)
    if m in (4, 5):             # Summer season
        return random.uniform(1.1, 1.3)
    return 1.0

def get_product_for_store(store_class):
    """
    FIX 2: was defined but never called — now used for every transaction.
    Match products to store type based on purchase affinity.
    """
    if "Kirana" in store_class:
        preferred = ["Grocery", "Beverage", "Dairy"]
    elif "Convenience" in store_class:
        preferred = ["Beverage", "Dairy", "Grocery"]
    else:
        # Hypermarket, Supermarket, Wholesale — all categories
        return random.choice(products)

    filtered = [p for p in products if p[1] in preferred]
    return random.choice(filtered) if filtered else random.choice(products)

def calculate_discount(gross_amount, store_class, is_chain):
    """
    Tiered discount:
      wholesale base + volume tier + chain bonus, capped at 20 %.
    """
    base = 0.08 if ("Wholesale" in store_class or "Cash & Carry" in store_class) else 0.0

    if   gross_amount >= 20000: vol = 0.15
    elif gross_amount >= 10000: vol = 0.10
    elif gross_amount >= 5000:  vol = 0.07
    elif gross_amount >= 2000:  vol = 0.05
    else:                       vol = 0.02

    chain = 0.03 if is_chain == "Y" else 0.0
    return min(base + vol + chain, 0.20)

# ========================================
# GENERATE EXACTLY 1000 TRANSACTIONS
#  STORE_TRANSACTION_VOLUME was defined but never used —
#        now store selection is weighted by volume tier so busier
#        store formats appear proportionally more often.
# ========================================
STORE_TRANSACTION_VOLUME = {
    "Modern Trade - Hypermarket":   (8, 15),
    "Modern Trade - Supermarket":   (5, 10),
    "General Trade - Kirana":       (1, 3),
    "Convenience Store":            (3, 6),
    "Cash & Carry - Wholesale":     (2, 4),
}

def _store_weight(store_class):
    """Mid-point of volume range as selection weight."""
    for key, (lo, hi) in STORE_TRANSACTION_VOLUME.items():
        if key in store_class:
            return (lo + hi) / 2
    return 2   # default mid-weight for unknown types

store_weights = [_store_weight(s[1]) for s in stores]

ROWS_PER_DAY   = 1000
weekend_boost  = get_weekend_boost()
seasonal_boost = get_seasonal_boost()

data           = []
sales_id       = start_id
total_gross    = Decimal("0")
total_discount = Decimal("0")
txn_by_type    = {}
gross_by_cat   = {}

for _ in range(ROWS_PER_DAY):
    sales_id += 1

    # weighted store selection
    store_id, store_class, is_chain = random.choices(stores, weights=store_weights, k=1)[0]

    #  product chosen via affinity function
    product_id, category, sub_category, unit_price = get_product_for_store(store_class)

    distributor_id, _ = random.choice(distributors)

    # Quantity with business boosts applied  (FIX 3)
    lo, hi   = CATEGORY_PURCHASE_QTY.get(category, (1, 5))
    base_qty = random.randint(lo, hi)
    quantity = int(base_qty * get_quantity_multiplier(store_class) * weekend_boost * seasonal_boost)
    quantity = max(quantity, 1)

    try:
        unit_dec = Decimal(str(unit_price))
    except InvalidOperation:
        continue   # skip if price is somehow non-numeric despite the DB filter

    gross    = round(Decimal(str(quantity)) * unit_dec, 2)
    disc_pct = calculate_discount(float(gross), store_class, is_chain)
    discount = round(gross * Decimal(str(disc_pct)), 2)
    net      = gross - discount

    data.append((
        sales_id, date_id, store_id, product_id, distributor_id,
        quantity, float(unit_dec), float(gross), float(discount), float(net)
    ))

    total_gross    += gross
    total_discount += discount
    txn_by_type[store_class]  = txn_by_type.get(store_class, 0) + 1
    gross_by_cat[category]    = gross_by_cat.get(category, Decimal("0")) + gross

# ========================================
# BULK INSERT  (single commit; rollback on any failure)
# commit was inside the batch loop — partial commits on failure.
#        Now one commit after all data is staged.
# ========================================
INSERT_SQL = """
INSERT INTO fact_sales (
    sales_id, date_id, store_id, product_id,
    distributor_id, quantity_sold, unit_price,
    gross_amount, discount_amount, net_amount
) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
"""

BATCH_SIZE = 1000
try:
    for i in range(0, len(data), BATCH_SIZE):
        cur.executemany(INSERT_SQL, data[i:i + BATCH_SIZE])
    conn.commit()
except Exception as e:
    conn.rollback()
    cur.close(); conn.close()
    raise RuntimeError(f"fact_sales insert failed: {e}") from e

# ========================================
# POST-INSERT STATS FROM DB
# ========================================
cur.execute("""
    SELECT COUNT(*), SUM(net_amount), AVG(net_amount), MAX(net_amount)
    FROM   fact_sales
    WHERE  date_id = :1
""", [date_id])
db_stats = cur.fetchone()

cur.close()
conn.close()

# ========================================
# SUMMARY 
# ========================================
# guard against zero total_gross to avoid ZeroDivisionError
disc_pct_str = (
    f"{float(total_discount) / float(total_gross) * 100:.2f}%"
    if total_gross > 0 else "N/A"
)

print(
    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
    f"date_id: {date_id} ({day_name}) | "
    f"Inserted: {len(data):,} | "
    f"IDs: {start_id + 1}–{sales_id} | "
    f"Gross: ₹{float(total_gross):,.2f} | "
    f"Discount: {disc_pct_str} | "
    f"Net: ₹{float(total_gross - total_discount):,.2f}"
)
print(
    "  By category: " +
    " | ".join(
        f"{cat}: ₹{float(amt):,.0f}"
        for cat, amt in sorted(gross_by_cat.items(), key=lambda x: x[1], reverse=True)
    )
)
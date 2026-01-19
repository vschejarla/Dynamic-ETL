import random
import oracledb
from datetime import datetime
from decimal import Decimal

print("🛒 FACT_SALES daily increment started")

# -------------------------------
# Oracle Connection
# -------------------------------
conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

# -------------------------------
# Get next SALES_ID
# -------------------------------
cur.execute("SELECT NVL(MAX(sales_id),0) FROM fact_sales")
start_id = cur.fetchone()[0]
print(f"🔢 Starting sales_id: {start_id + 1}")

# -------------------------------
# Get TODAY date_id from DIM_DATE
# -------------------------------
today_date_id = int(datetime.now().strftime("%Y%m%d"))

cur.execute(
    "SELECT date_id, day_name, is_weekend FROM dim_date WHERE date_id = :1",
    [today_date_id]
)
row = cur.fetchone()
if not row:
    print(f"\n❌ date_id {today_date_id} not found in dim_date.")
    print(f"⚠️  This means dim_date doesn't have today's date yet.")
    print(f"💡 Please run dim_date_daily.py first to add today's date to dim_date")
    print(f"\nℹ️  Fact sales can only be generated for dates that exist in dim_date")
    cur.close()
    conn.close()
    exit(0)  # Exit gracefully, not an error - just not ready yet

date_id = row[0]
day_name = row[1]
is_weekend = row[2]

print(f"📅 Processing sales for: {date_id} ({day_name})")
print(f"🏖️  Weekend: {'Yes' if is_weekend == 'Y' else 'No'}")

# -------------------------------
# Fetch Dimension Data with Business Attributes
# -------------------------------

# Stores with class of trade
cur.execute("""
    SELECT store_id, store_class_of_trade, is_chain 
    FROM dim_store_master 
    WHERE 1=1
""")
stores = cur.fetchall()
print(f"🏪 Active stores: {len(stores)}")

# Products with category and price
cur.execute("""
    SELECT product_id, category, sub_category, unit_price 
    FROM dim_product
""")
products = cur.fetchall()
print(f"📦 Available products: {len(products)}")

# Active distributors only
cur.execute("""
    SELECT distributor_id, distributor_type 
    FROM dim_distributor 
    WHERE active_flag = 'Y'
""")
distributors = cur.fetchall()
print(f"🚚 Active distributors: {len(distributors)}")

if not stores or not products or not distributors:
    raise ValueError("❌ Missing dimension data. Please load dimension tables first.")

# -------------------------------
# RETAIL BUSINESS LOGIC
# -------------------------------

# Store type affects transaction volume
STORE_TRANSACTION_VOLUME = {
    "Modern Trade - Hypermarket": (80, 150),    # High volume
    "Modern Trade - Supermarket": (50, 100),    # Medium-high volume
    "General Trade - Kirana": (10, 30),         # Low-medium volume
    "Convenience Store": (30, 60),              # Medium volume
    "Cash & Carry - Wholesale": (20, 40)        # Medium volume (bulk orders)
}

# Category purchase patterns
CATEGORY_PURCHASE_QTY = {
    "Grocery": (1, 5),          # Staples - moderate quantity
    "Beverage": (2, 12),        # Beverages - higher quantity
    "Dairy": (1, 6),            # Perishables - moderate quantity
    "Personal Care": (1, 4),    # Low quantity
    "Baby Care": (1, 3),        # Low quantity
    "Home Care": (1, 4)         # Low quantity
}

# Wholesale vs retail quantity multiplier
def get_quantity_multiplier(store_class, distributor_type):
    """Wholesale stores buy in bulk"""
    if "Wholesale" in store_class or "Cash & Carry" in store_class:
        return random.randint(5, 20)  # Bulk multiplier
    return 1

# Weekend boost (20-40% more transactions)
def get_weekend_boost():
    """More sales on weekends"""
    if is_weekend == 'Y':
        return random.uniform(1.2, 1.4)
    return 1.0

# Seasonal boost (can be expanded with actual date logic)
def get_seasonal_boost(current_month):
    """Festival seasons see higher sales"""
    # Diwali season (Oct-Nov), Christmas (Dec), Summer (Apr-May)
    if current_month in [10, 11, 12]:  # Festival season
        return random.uniform(1.3, 1.6)
    elif current_month in [4, 5]:      # Summer season
        return random.uniform(1.1, 1.3)
    return 1.0

# Discount tiers based on business logic
def calculate_discount(gross_amount, store_class, is_chain):
    """
    Realistic discount structure:
    - Chain stores negotiate better discounts
    - Wholesale gets volume discounts
    - Higher purchase amounts get better discounts
    """
    
    base_discount_pct = 0.0
    
    # Wholesale discount
    if "Wholesale" in store_class or "Cash & Carry" in store_class:
        base_discount_pct = 0.08
    
    # Volume-based discount tiers
    if gross_amount >= 20000:
        volume_discount = 0.15
    elif gross_amount >= 10000:
        volume_discount = 0.10
    elif gross_amount >= 5000:
        volume_discount = 0.07
    elif gross_amount >= 2000:
        volume_discount = 0.05
    else:
        volume_discount = 0.02
    
    # Chain store additional discount (better negotiation power)
    chain_discount = 0.03 if is_chain == 'Y' else 0.0
    
    # Total discount (capped at 20%)
    total_discount_pct = min(base_discount_pct + volume_discount + chain_discount, 0.20)
    
    return total_discount_pct

# Product-Store affinity (some products sell more in certain stores)
def get_product_for_store(store_class, available_products):
    """Match products to store types with weighted probability"""
    
    if "Hypermarket" in store_class or "Supermarket" in store_class:
        # Modern trade sells all categories
        return random.choice(available_products)
    
    elif "Kirana" in store_class:
        # Kirana focuses on Grocery, Beverage, Dairy
        preferred_categories = ["Grocery", "Beverage", "Dairy"]
        filtered = [p for p in available_products if p[1] in preferred_categories]
        if filtered:
            return random.choice(filtered)
        return random.choice(available_products)
    
    elif "Convenience" in store_class:
        # Convenience stores - Beverages, Snacks, Dairy
        preferred_categories = ["Beverage", "Dairy", "Grocery"]
        filtered = [p for p in available_products if p[1] in preferred_categories]
        if filtered:
            return random.choice(filtered)
        return random.choice(available_products)
    
    else:  # Wholesale
        # Wholesale - all categories
        return random.choice(available_products)

# -------------------------------
# Generate Transaction Volume per Store
# -------------------------------

current_month = datetime.now().month
weekend_boost = get_weekend_boost()
seasonal_boost = get_seasonal_boost(current_month)

print(f"\n📊 Sales Multipliers:")
print(f"   Weekend Boost: {weekend_boost:.2f}x")
print(f"   Seasonal Boost: {seasonal_boost:.2f}x")
print(f"\n💰 Generating transactions for TODAY ({datetime.now().date()})...\n")

# -------------------------------
# Insert SQL
# -------------------------------
sql = """
INSERT INTO fact_sales (
    sales_id, date_id, store_id, product_id,
    distributor_id, quantity_sold, unit_price,
    gross_amount, discount_amount, net_amount
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
)
"""

# -------------------------------
# Generate FACT rows
# -------------------------------
data = []
sales_id = start_id
total_gross = 0
total_discount = 0
total_net = 0

transaction_count_by_type = {}
gross_by_category = {}

for store_id, store_class, is_chain in stores:
    # Determine transaction volume for this store
    base_volume = STORE_TRANSACTION_VOLUME.get(
        store_class, 
        (20, 50)
    )
    
    # Apply multipliers
    min_txn = int(base_volume[0] * weekend_boost * seasonal_boost)
    max_txn = int(base_volume[1] * weekend_boost * seasonal_boost)
    
    num_transactions = random.randint(min_txn, max_txn)
    
    # Track by store type
    transaction_count_by_type[store_class] = \
        transaction_count_by_type.get(store_class, 0) + num_transactions
    
    for _ in range(num_transactions):
        sales_id += 1
        
        # Select product based on store type
        product_id, category, sub_category, unit_price = get_product_for_store(
            store_class, 
            products
        )
        
        # Determine quantity based on category and store type
        qty_range = CATEGORY_PURCHASE_QTY.get(category, (1, 5))
        base_qty = random.randint(qty_range[0], qty_range[1])
        
        # Apply bulk multiplier for wholesale
        qty_multiplier = get_quantity_multiplier(store_class, "")
        quantity = base_qty * qty_multiplier
        
        # Calculate amounts
        gross = Decimal(str(quantity)) * Decimal(str(unit_price))
        
        # Calculate discount
        discount_pct = calculate_discount(float(gross), store_class, is_chain)
        discount = gross * Decimal(str(discount_pct))
        net = gross - discount
        
        # Round to 2 decimal places
        gross = round(gross, 2)
        discount = round(discount, 2)
        net = round(net, 2)
        
        # Select distributor (random for now, can add logic)
        distributor_id, distributor_type = random.choice(distributors)
        
        data.append((
            sales_id,
            date_id,
            store_id,
            product_id,
            distributor_id,
            quantity,
            float(unit_price),
            float(gross),
            float(discount),
            float(net)
        ))
        
        # Accumulate totals
        total_gross += float(gross)
        total_discount += float(discount)
        total_net += float(net)
        
        # Track by category
        gross_by_category[category] = \
            gross_by_category.get(category, 0) + float(gross)

# -------------------------------
# Bulk Insert
# -------------------------------
print(f"📝 Inserting {len(data)} transactions for {datetime.now().date()}...")

# Insert in batches of 1000 for performance
batch_size = 1000
for i in range(0, len(data), batch_size):
    batch = data[i:i + batch_size]
    cur.executemany(sql, batch)
    conn.commit()
    print(f"   ✅ Inserted batch {i//batch_size + 1}: {len(batch)} rows")

print(f"\n{'='*70}")
print(f"✅ FACT_SALES daily increment completed for date_id: {date_id}")
print(f"{'='*70}")

print(f"\n📊 Transaction Summary:")
print(f"   Date: {datetime.now().date()} (TODAY)")
print(f"   Total Transactions: {len(data):,}")
print(f"   Sales ID Range: {start_id + 1:,} to {sales_id:,}")
print(f"   Total Gross: ₹{total_gross:,.2f}")
print(f"   Total Discount: ₹{total_discount:,.2f} ({(total_discount/total_gross*100):.2f}%)")
print(f"   Total Net: ₹{total_net:,.2f}")

print(f"\n🏪 Transactions by Store Type:")
for store_type, count in sorted(transaction_count_by_type.items(), 
                                 key=lambda x: x[1], reverse=True):
    print(f"   {store_type}: {count:,} transactions")

print(f"\n📦 Gross Sales by Category:")
for category, amount in sorted(gross_by_category.items(), 
                                key=lambda x: x[1], reverse=True):
    pct = (amount / total_gross * 100)
    print(f"   {category}: ₹{amount:,.2f} ({pct:.1f}%)")

# Final statistics from database
cur.execute("""
    SELECT 
        COUNT(*) as total_records,
        SUM(net_amount) as total_sales,
        AVG(net_amount) as avg_ticket_size,
        MAX(net_amount) as max_transaction
    FROM fact_sales
    WHERE date_id = :1
""", [date_id])

stats = cur.fetchone()
print(f"\n📈 Today's Sales Statistics:")
print(f"   Total Records: {stats[0]:,}")
print(f"   Total Sales: ₹{stats[1]:,.2f}")
print(f"   Average Ticket Size: ₹{stats[2]:,.2f}")
print(f"   Largest Transaction: ₹{stats[3]:,.2f}")

cur.close()
conn.close()

print(f"\n🎉 Sales data generation completed successfully!")
print(f"💡 This script generates sales only for TODAY - no future dates")
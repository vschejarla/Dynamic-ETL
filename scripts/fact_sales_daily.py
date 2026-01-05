import random
import oracledb
from datetime import datetime

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

# -------------------------------
# Get TODAY date_id from DIM_DATE
# -------------------------------
today_date_id = int(datetime.now().strftime("%Y%m%d"))

cur.execute(
    "SELECT date_id FROM dim_date WHERE date_id = :1",
    [today_date_id]
)
row = cur.fetchone()
if not row:
    raise ValueError(f"❌ date_id {today_date_id} not found in dim_date")

date_id = row[0]

# -------------------------------
# Fetch Dimension Keys
# -------------------------------
cur.execute("SELECT store_id FROM dim_store_master")
store_ids = [r[0] for r in cur.fetchall()]

cur.execute("SELECT product_id, unit_price FROM dim_product")
products = cur.fetchall()

cur.execute("SELECT distributor_id FROM dim_distributor")
distributor_ids = [r[0] for r in cur.fetchall()]

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

for i in range(1, 1001):
    sales_id = start_id + i
    product_id, price = random.choice(products)
    qty = random.randint(1, 25)
    gross = qty * price

    if gross >= 10000:
        discount_pct = 0.15
    elif gross >= 5000:
        discount_pct = 0.10
    elif gross >= 2000:
        discount_pct = 0.05
    else:
        discount_pct = 0.0

    discount = round(gross * discount_pct, 2)
    net = round(gross - discount, 2)

    data.append((
        sales_id,
        date_id,                      # ✅ TODAY’s date_id
        random.choice(store_ids),
        product_id,
        random.choice(distributor_ids),
        qty,
        price,
        gross,
        discount,
        net
    ))

# -------------------------------
# Bulk Insert
# -------------------------------
cur.executemany(sql, data)
conn.commit()

cur.close()
conn.close()

print("✅ FACT_SALES daily increment completed for date_id:", date_id)
from faker import Faker
import oracledb
import random

fake = Faker()

conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

cur.execute("SELECT NVL(MAX(product_id),0) FROM dim_product")
start_id = cur.fetchone()[0]

insert_sql = """
INSERT INTO dim_product (
    product_id, product_name, category, sub_category,
    brand, flavour, product_size, sqc, uom, unit_price
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
)
"""

data = []
for i in range(1, 11):
    product_id = start_id + i
    data.append((
        product_id,
        fake.word().capitalize(),
        random.choice(['Grocery','Beverage','Dairy']),
        fake.word().capitalize(),
        fake.company()[:30],
        random.choice(['Vanilla','Chocolate','Masala',None]),
        random.choice(['250g','500g','1kg','1L']),
        random.choice(['EA','PK','BOX']),
        random.choice(['KG','G','LTR','PCS']),
        round(random.uniform(20,500),2)
    ))

cur.executemany(insert_sql, data)
conn.commit()
cur.close()
conn.close()

print("✅ DIM_PRODUCT daily increment completed")

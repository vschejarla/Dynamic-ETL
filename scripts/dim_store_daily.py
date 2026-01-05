from faker import Faker
import oracledb
import random

fake = Faker()

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

cur.execute("SELECT NVL(MAX(store_id),0) FROM dim_store_master")
start_id = cur.fetchone()[0]

sql = """
INSERT INTO dim_store_master (
    store_id, store_name, store_address_lane_1,
    store_address_lane_2, store_city, store_zip,
    store_state, store_class_of_trade, is_chain, chain_name
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
)
"""

data = []
for i in range(1, 11):
    store_id = start_id + i
    is_chain = random.choice(['Y','N'])

    data.append((
        store_id,
        fake.company()[:50],
        fake.street_address(),
        fake.secondary_address(),
        fake.city(),
        fake.postcode(),
        fake.state(),
        random.choice(['Retail','Wholesale','Convenience']),
        is_chain,
        fake.company()[:50] if is_chain == 'Y' else None
    ))

cur.executemany(sql, data)
conn.commit()
cur.close()
conn.close()

print("✅ DIM_STORE_MASTER daily increment completed")

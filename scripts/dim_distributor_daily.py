from faker import Faker
import oracledb
import random
from datetime import date

fake = Faker()

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

cur.execute("SELECT NVL(MAX(distributor_id),0) FROM dim_distributor")
start_id = cur.fetchone()[0]

sql = """
INSERT INTO dim_distributor (
    distributor_id, distributor_name, distributor_type,
    city, state, onboarding_date, active_flag
) VALUES (
    :1,:2,:3,:4,:5,:6,:7
)
"""

data = []
for i in range(1, 11):
    distributor_id = start_id + i
    data.append((
        distributor_id,
        fake.company()[:100],
        random.choice(['National','Regional','Local']),
        fake.city(),
        fake.state(),
        fake.date_between(start_date=date(2019,1,1), end_date=date.today()),
        random.choice(['Y','N'])
    ))

cur.executemany(sql, data)
conn.commit()
cur.close()
conn.close()

print("✅ DIM_DISTRIBUTOR daily increment completed")

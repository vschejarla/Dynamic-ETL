import oracledb
from datetime import datetime, timedelta

print("DIM_DATE daily load started")

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

today = datetime.now().date()

rows_inserted = 0

for i in range(10):   # 👈 10 rows
    current_date = today + timedelta(days=i)

    date_id = int(current_date.strftime("%Y%m%d"))
    full_date = current_date
    day = current_date.day
    day_name = current_date.strftime("%A")
    week_of_year = int(current_date.strftime("%U"))
    month = current_date.month
    month_name = current_date.strftime("%B")
    quarter = (month - 1) // 3 + 1
    year = current_date.year
    is_weekend = "Y" if current_date.weekday() >= 5 else "N"

    # Check if already exists
    cur.execute(
        "SELECT COUNT(*) FROM dim_date WHERE date_id = :1",
        [date_id]
    )

    if cur.fetchone()[0] == 0:
        cur.execute(
            """
            INSERT INTO dim_date (
                date_id, full_date, day, day_name,
                week_of_year, month, month_name,
                quarter, year, is_weekend
            )
            VALUES (
                :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
            )
            """,
            [
                date_id, full_date, day, day_name,
                week_of_year, month, month_name,
                quarter, year, is_weekend
            ]
        )
        rows_inserted += 1

conn.commit()
cur.close()
conn.close()

print(f"✅ Inserted {rows_inserted} rows into DIM_DATE")

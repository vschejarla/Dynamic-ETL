import oracledb
from datetime import datetime, timedelta

print("📅 DIM_DATE_DW full range load started")

conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

# -------------------------------
# Date range to load (inclusive)
# -------------------------------
# Adjust these to match your data history
START_DATE = datetime(2022, 1, 1).date()
END_DATE   = datetime(2026, 12, 31).date()

total_days = (END_DATE - START_DATE).days + 1

for i in range(total_days):
    d = START_DATE + timedelta(days=i)

    date_id = int(d.strftime("%Y%m%d"))
    full_date = d
    day = d.day
    day_name = d.strftime("%A")
    week_of_year = int(d.strftime("%U"))
    month = d.month
    month_name = d.strftime("%B")
    quarter = (month - 1) // 3 + 1
    year = d.year
    is_weekend = "Y" if d.weekday() >= 5 else "N"

    # Business key check on date_id
    cur.execute(
        "SELECT COUNT(*) FROM dim_date_dw WHERE date_id = :1",
        [date_id]
    )
    exists = cur.fetchone()[0]

    if exists == 0:
        cur.execute(
            """
            INSERT INTO dim_date_dw (
                date_id,
                full_date,
                day,
                day_name,
                week_of_year,
                month,
                month_name,
                quarter,
                year,
                is_weekend
            ) VALUES (
                :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
            )
            """,
            [
                date_id,
                full_date,
                day,
                day_name,
                week_of_year,
                month,
                month_name,
                quarter,
                year,
                is_weekend
            ]
        )
        print(f"✅ Inserted date_id {date_id}")
    else:
        # Optional: comment out to reduce log noise
        # print(f"ℹ️ date_id {date_id} already exists")
        pass

conn.commit()
cur.close()
conn.close()

print("🎉 DIM_DATE_DW full range load completed")

import oracledb
from datetime import datetime, timedelta, date

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

# ========================================
# INDIAN HOLIDAYS (2023-2030)
# Keys: YYYYMMDD integer  Value: holiday name
# ========================================
INDIAN_HOLIDAYS = {
    # 2023
    20230126: "Republic Day",        20230218: "Maha Shivaratri",
    20230308: "Holi",                20230407: "Good Friday",
    20230422: "Eid ul-Fitr",         20230815: "Independence Day",
    20230907: "Janmashtami",         20231002: "Gandhi Jayanti",
    20231024: "Dussehra",            20231112: "Diwali",
    20231127: "Guru Nanak Jayanti",  20231225: "Christmas",
    # 2024
    20240126: "Republic Day",        20240308: "Maha Shivaratri",
    20240325: "Holi",                20240329: "Good Friday",
    20240411: "Eid ul-Fitr",         20240417: "Ram Navami",
    20240423: "Mahavir Jayanti",     20240815: "Independence Day",
    20240826: "Janmashtami",         20241002: "Gandhi Jayanti",
    20241012: "Dussehra",            20241031: "Diwali",
    20241101: "Diwali (Day 2)",      20241115: "Guru Nanak Jayanti",
    20241225: "Christmas",
    # 2025
    20250126: "Republic Day",        20250226: "Maha Shivaratri",
    20250314: "Holi",                20250330: "Eid ul-Fitr",
    20250406: "Ram Navami",          20250410: "Mahavir Jayanti",
    20250418: "Good Friday",         20250815: "Independence Day",
    20250816: "Janmashtami",         20251002: "Gandhi Jayanti",
    20251022: "Dussehra",            20251101: "Diwali",
    20251105: "Guru Nanak Jayanti",  20251225: "Christmas",
    # 2026
    20260126: "Republic Day",        20260217: "Maha Shivaratri",
    20260306: "Holi",                20260320: "Eid ul-Fitr",
    20260403: "Good Friday",         20260406: "Mahavir Jayanti",
    20260815: "Independence Day",    20260905: "Janmashtami",
    20261002: "Gandhi Jayanti",      20261012: "Dussehra",
    20261019: "Diwali",              20261124: "Guru Nanak Jayanti",
    20261225: "Christmas",
    # 2027
    20270126: "Republic Day",        20270307: "Maha Shivaratri",
    20270310: "Eid ul-Fitr",         20270325: "Holi",
    20270326: "Good Friday",         20270815: "Independence Day",
    20270825: "Janmashtami",         20271001: "Dussehra",
    20271002: "Gandhi Jayanti",      20271108: "Diwali",
    20271114: "Guru Nanak Jayanti",  20271225: "Christmas",
    # 2028
    20280126: "Republic Day",        20280226: "Maha Shivaratri",
    20280227: "Eid ul-Fitr",         20280312: "Holi",
    20280414: "Good Friday",         20280814: "Janmashtami",
    20280815: "Independence Day",    20281002: "Gandhi Jayanti",
    20281020: "Dussehra",            20281027: "Diwali",
    20281102: "Guru Nanak Jayanti",  20281225: "Christmas",
    # 2029
    20290126: "Republic Day",        20290214: "Maha Shivaratri",
    20290216: "Eid ul-Fitr",         20290301: "Holi",
    20290330: "Good Friday",         20290815: "Independence Day",
    20290903: "Janmashtami",         20291002: "Gandhi Jayanti",
    20291009: "Dussehra",            20291117: "Diwali",
    20291123: "Guru Nanak Jayanti",  20291225: "Christmas",
    # 2030
    20300126: "Republic Day",        20300205: "Eid ul-Fitr",
    20300304: "Maha Shivaratri",     20300320: "Holi",
    20300419: "Good Friday",         20300815: "Independence Day",
    20300823: "Janmashtami",         20300928: "Dussehra",
    20301002: "Gandhi Jayanti",      20301105: "Diwali",
    20301112: "Guru Nanak Jayanti",  20301225: "Christmas",
}

# ========================================
# BUSINESS LOGIC FUNCTIONS
# ========================================
def get_fiscal_quarter(d):
    """Indian FY: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar"""
    m = d.month
    if m >= 4:
        return ((m - 4) // 3) + 1
    return 4

def get_fiscal_year(d):
    """End-year convention: Apr 2024–Mar 2025 → FY 2025"""
    return d.year + 1 if d.month >= 4 else d.year

def is_month_end(d):
    return (d + timedelta(days=1)).month != d.month

def is_quarter_end(d):
    return is_month_end(d) and d.month in (3, 6, 9, 12)

def is_fiscal_quarter_end(d):
    return is_month_end(d) and d.month in (6, 9, 12, 3)

def is_year_end(d):
    return d.month == 12 and d.day == 31

def is_fiscal_year_end(d):
    return d.month == 3 and d.day == 31

def is_business_day(d, date_id):
    return d.weekday() < 5 and date_id not in INDIAN_HOLIDAYS

def get_day_type(d, date_id):
    if date_id in INDIAN_HOLIDAYS:
        return "Holiday"
    return "Weekend" if d.weekday() >= 5 else "Weekday"

# ========================================
# DETERMINE START DATE
# ========================================
cur.execute("SELECT MAX(full_date) FROM dim_date")
last_date_result = cur.fetchone()[0]
today = datetime.now().date()

if last_date_result:
    last_date = last_date_result.date() if isinstance(last_date_result, datetime) else last_date_result
    start_date = last_date + timedelta(days=1)
else:
    start_date = date(datetime.now().year - 2, 1, 1)

if start_date > today:
    print(f"dim_date already current through {last_date} — nothing to load")
    cur.close(); conn.close()
    exit(0)

ROWS_PER_RUN = 10
end_date = min(start_date + timedelta(days=ROWS_PER_RUN - 1), today)

# ========================================
# Load existing date_ids ONCE into a set
# (avoids N per-row SELECT queries inside the loop)
# ========================================
cur.execute(
    "SELECT date_id FROM dim_date WHERE date_id >= :1 AND date_id <= :2",
    [int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))]
)
existing_ids = {r[0] for r in cur.fetchall()}

# ========================================
# BUILD INSERT DATA
# ========================================
INSERT_SQL = """
INSERT INTO dim_date (
    date_id, full_date, day, day_name, day_of_week,
    week_of_year, month, month_name, quarter, year,
    fiscal_quarter, fiscal_year, is_weekend, is_month_end,
    is_quarter_end, is_fiscal_quarter_end, is_year_end,
    is_fiscal_year_end, is_holiday, holiday_name,
    is_business_day
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,
    :11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21
)
"""

data         = []
rows_skipped = 0

for i in range((end_date - start_date).days + 1):
    d       = start_date + timedelta(days=i)
    date_id = int(d.strftime("%Y%m%d"))

    if date_id in existing_ids:
        rows_skipped += 1
        continue

    #use ISO week number via isocalendar() — consistent across year boundaries
    # %W returns 0 for days before the first Monday of the year
    week_of_year = d.isocalendar()[1]

    holiday_name            = INDIAN_HOLIDAYS.get(date_id)
    is_weekend_flag         = "Y" if d.weekday() >= 5 else "N"
    is_business_day_flag    = "Y" if is_business_day(d, date_id) else "N"

    data.append((
        date_id,
        d,
        d.day,
        d.strftime("%A"),
        d.weekday() + 1,           # 1=Monday … 7=Sunday
        week_of_year,
        d.month,
        d.strftime("%B"),
        (d.month - 1) // 3 + 1,   # calendar quarter
        d.year,
        get_fiscal_quarter(d),
        get_fiscal_year(d),
        is_weekend_flag,
        "Y" if is_month_end(d)              else "N",
        "Y" if is_quarter_end(d)            else "N",
        "Y" if is_fiscal_quarter_end(d)     else "N",
        "Y" if is_year_end(d)               else "N",
        "Y" if is_fiscal_year_end(d)        else "N",
        "Y" if holiday_name                 else "N",
        holiday_name,
        is_business_day_flag,
    ))

# ========================================
# INSERT
# ========================================
try:
    if data:
        cur.executemany(INSERT_SQL, data)
    conn.commit()
except Exception as e:
    conn.rollback()
    cur.close(); conn.close()
    raise RuntimeError(f"Insert failed: {e}") from e

# ========================================
# SUMMARY STATISTICS
# ========================================
cur.execute("""
    SELECT COUNT(*),
           MIN(full_date), MAX(full_date),
           SUM(CASE WHEN is_weekend     = 'Y' THEN 1 ELSE 0 END),
           SUM(CASE WHEN is_holiday     = 'Y' THEN 1 ELSE 0 END),
           SUM(CASE WHEN is_business_day= 'Y' THEN 1 ELSE 0 END)
    FROM dim_date
""")
stats = cur.fetchone()

max_date_in_table = None
if stats and stats[0] > 0:
    min_dt = stats[1].date() if isinstance(stats[1], datetime) else stats[1]
    max_date_in_table = stats[2].date() if isinstance(stats[2], datetime) else stats[2]
    days_behind = (today - max_date_in_table).days
    coverage = "CURRENT" if days_behind <= 0 else f"BEHIND by {days_behind} day(s)"

    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Inserted: {len(data)} | Skipped: {rows_skipped} | "
        f"Range loaded: {start_date} → {end_date}"
    )
    print(
        f"  DB total: {stats[0]:,} | span: {min_dt} → {max_date_in_table} | "
        f"biz days: {stats[5]:,} | holidays: {stats[4]:,} | weekends: {stats[3]:,} | "
        f"coverage: {coverage}"
    )

    if max_date_in_table < today:
        remaining = (today - max_date_in_table).days
        runs_needed = (remaining + ROWS_PER_RUN - 1) // ROWS_PER_RUN
        print(f"  {remaining} days remaining — run {runs_needed} more time(s) to reach today")
else:
    print(f"Inserted: {len(data)} | Skipped: {rows_skipped} | Range: {start_date} → {end_date}")

cur.close()
conn.close()
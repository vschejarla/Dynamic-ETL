import oracledb
from datetime import datetime, timedelta

print("🗓️ DIM_DATE Daily Auto-Increment Started")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

conn = oracledb.connect(
    user="system",
<<<<<<< HEAD
    password="905966Sh@r4107",
=======
    password="oracle123",
>>>>>>> etl-update
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()
print("✅ Connected to Oracle Database\n")

# ========================================
# INDIAN HOLIDAYS DATABASE (2023-2030)
# ========================================
INDIAN_HOLIDAYS = {
    # 2023
    20230126: "Republic Day", 20230218: "Maha Shivaratri", 20230308: "Holi",
    20230407: "Good Friday", 20230422: "Eid ul-Fitr", 20230815: "Independence Day",
    20230907: "Janmashtami", 20231002: "Gandhi Jayanti", 20231024: "Dussehra",
    20231112: "Diwali", 20231127: "Guru Nanak Jayanti", 20231225: "Christmas",
    
    # 2024
    20240126: "Republic Day", 20240308: "Maha Shivaratri", 20240325: "Holi",
    20240329: "Good Friday", 20240411: "Eid ul-Fitr", 20240417: "Ram Navami",
    20240423: "Mahavir Jayanti", 20240815: "Independence Day", 20240826: "Janmashtami",
    20241002: "Gandhi Jayanti", 20241012: "Dussehra", 20241031: "Diwali",
    20241101: "Diwali (Day 2)", 20241115: "Guru Nanak Jayanti", 20241225: "Christmas",
    
    # 2025
    20250126: "Republic Day", 20250226: "Maha Shivaratri", 20250314: "Holi",
    20250330: "Eid ul-Fitr", 20250406: "Ram Navami", 20250410: "Mahavir Jayanti",
    20250418: "Good Friday", 20250815: "Independence Day", 20250816: "Janmashtami",
    20251002: "Gandhi Jayanti", 20251022: "Dussehra", 20251101: "Diwali",
    20251105: "Guru Nanak Jayanti", 20251225: "Christmas",
    
    # 2026
    20260126: "Republic Day", 20260217: "Maha Shivaratri", 20260306: "Holi",
    20260320: "Eid ul-Fitr", 20260403: "Good Friday", 20260406: "Mahavir Jayanti",
    20260815: "Independence Day", 20260905: "Janmashtami", 20261002: "Gandhi Jayanti",
    20261012: "Dussehra", 20261019: "Diwali", 20261124: "Guru Nanak Jayanti",
    20261225: "Christmas",
    
    # 2027
    20270126: "Republic Day", 20270307: "Maha Shivaratri", 20270325: "Holi",
    20270310: "Eid ul-Fitr", 20270326: "Good Friday", 20270815: "Independence Day",
    20270825: "Janmashtami", 20271002: "Gandhi Jayanti", 20271001: "Dussehra",
    20271108: "Diwali", 20271114: "Guru Nanak Jayanti", 20271225: "Christmas",
    
    # 2028
    20280126: "Republic Day", 20280226: "Maha Shivaratri", 20280312: "Holi",
    20280227: "Eid ul-Fitr", 20280414: "Good Friday", 20280815: "Independence Day",
    20280814: "Janmashtami", 20281002: "Gandhi Jayanti", 20281020: "Dussehra",
    20281027: "Diwali", 20281102: "Guru Nanak Jayanti", 20281225: "Christmas",
    
    # 2029
    20290126: "Republic Day", 20290214: "Maha Shivaratri", 20290301: "Holi",
    20290216: "Eid ul-Fitr", 20290330: "Good Friday", 20290815: "Independence Day",
    20290903: "Janmashtami", 20291002: "Gandhi Jayanti", 20291009: "Dussehra",
    20291117: "Diwali", 20291123: "Guru Nanak Jayanti", 20291225: "Christmas",
    
    # 2030
    20300126: "Republic Day", 20300304: "Maha Shivaratri", 20300320: "Holi",
    20300205: "Eid ul-Fitr", 20300419: "Good Friday", 20300815: "Independence Day",
    20300823: "Janmashtami", 20301002: "Gandhi Jayanti", 20300928: "Dussehra",
    20301105: "Diwali", 20301112: "Guru Nanak Jayanti", 20301225: "Christmas"
}

# ========================================
# BUSINESS LOGIC FUNCTIONS
# ========================================
def get_fiscal_quarter(d):
    """Indian fiscal year: April-March"""
    month = d.month
    if month >= 4:
        return ((month - 4) // 3) + 1
    else:
        return 4

def get_fiscal_year(d):
    """FY format: 2025 for Apr 2024 - Mar 2025"""
    if d.month >= 4:
        return d.year + 1
    else:
        return d.year

def is_month_end(d):
    """Check if last day of month"""
    next_day = d + timedelta(days=1)
    return next_day.month != d.month

def is_quarter_end(d):
    """Check if last day of calendar quarter"""
    return is_month_end(d) and d.month in [3, 6, 9, 12]

def is_fiscal_quarter_end(d):
    """Check if last day of fiscal quarter"""
    return is_month_end(d) and d.month in [6, 9, 12, 3]

def is_year_end(d):
    """Check if last day of calendar year"""
    return d.month == 12 and d.day == 31

def is_fiscal_year_end(d):
    """Check if last day of fiscal year (March 31)"""
    return d.month == 3 and d.day == 31

def is_business_day(d, date_id):
    """Not weekend and not holiday"""
    is_weekend = d.weekday() >= 5
    is_holiday = date_id in INDIAN_HOLIDAYS
    return not (is_weekend or is_holiday)

def get_day_type(d, date_id):
    """Classify day for retail analytics"""
    if date_id in INDIAN_HOLIDAYS:
        return "Holiday"
    elif d.weekday() >= 5:
        return "Weekend"
    else:
        return "Weekday"

# ========================================
# GET LAST DATE FROM TABLE
# ========================================
cur.execute("SELECT MAX(full_date) FROM dim_date")
last_date_result = cur.fetchone()[0]

# Get today's date
today = datetime.now().date()

if last_date_result:
    # Convert datetime to date if needed
    if isinstance(last_date_result, datetime):
        last_date = last_date_result.date()
    else:
        last_date = last_date_result
    
    # Continue from next day after last date
    start_date = last_date + timedelta(days=1)
    print(f"📅 Last date in table: {last_date.strftime('%Y-%m-%d')}")
    print(f"📅 Starting from: {start_date.strftime('%Y-%m-%d')}")
else:
    # Table is empty - start from 2 years ago
    current_year = datetime.now().year
    start_date = datetime(current_year - 2, 1, 1).date()
    print(f"📅 Table is empty. Starting from: {start_date.strftime('%Y-%m-%d')}")

# ========================================
# CONFIGURATION: LOAD 10 ROWS PER RUN
# ========================================
ROWS_PER_RUN = 10  # Load exactly 10 dates each time this script runs

# Calculate end date (10 days from start)
end_date = start_date + timedelta(days=ROWS_PER_RUN - 1)

# CRITICAL: Do not go beyond today's date
if start_date > today:
    print(f"\n✅ Date dimension is already current!")
    print(f"   Last date: {last_date.strftime('%Y-%m-%d')}")
    print(f"   Today: {today}")
    print(f"\nℹ️  No new dates to load - already at current date")
    print(f"💡 This script loads historical and current dates only, not future dates\n")
    cur.close()
    conn.close()
    exit(0)

# Limit end_date to today (no future dates)
if end_date > today:
    end_date = today
    actual_rows = (end_date - start_date).days + 1
    print(f"⚠️  Limiting load to today: {today}")
    print(f"   Will load {actual_rows} rows instead of {ROWS_PER_RUN}")

print(f"\n📅 Loading dates: {start_date} to {end_date}")
print(f"   Today's date: {today}")
print(f"\n{'='*70}")
print(f"DATE RECORDS BEING LOADED:")
print(f"{'='*70}\n")

# ========================================
# INSERT WITH FULL BUSINESS LOGIC
# ========================================
rows_inserted = 0
rows_skipped = 0

for i in range((end_date - start_date).days + 1):
    d = start_date + timedelta(days=i)

    # Calculate all attributes
    date_id = int(d.strftime("%Y%m%d"))
    full_date = d
    day = d.day
    day_name = d.strftime("%A")
    day_of_week = d.weekday() + 1  # 1=Monday, 7=Sunday
    week_of_year = int(d.strftime("%W"))  # Monday-based weeks
    month = d.month
    month_name = d.strftime("%B")
    quarter = (month - 1) // 3 + 1
    year = d.year
    
    # Business flags
    fiscal_quarter = get_fiscal_quarter(d)
    fiscal_year = get_fiscal_year(d)
    is_weekend = "Y" if d.weekday() >= 5 else "N"
    is_month_end_flag = "Y" if is_month_end(d) else "N"
    is_quarter_end_flag = "Y" if is_quarter_end(d) else "N"
    is_fiscal_quarter_end_flag = "Y" if is_fiscal_quarter_end(d) else "N"
    is_year_end_flag = "Y" if is_year_end(d) else "N"
    is_fiscal_year_end_flag = "Y" if is_fiscal_year_end(d) else "N"
    holiday_name = INDIAN_HOLIDAYS.get(date_id, None)
    is_holiday = "Y" if holiday_name else "N"
    is_business_day_flag = "Y" if is_business_day(d, date_id) else "N"
    day_type = get_day_type(d, date_id)

    # Check if already exists (to avoid duplicates)
    cur.execute("SELECT COUNT(*) FROM dim_date WHERE date_id = :1", [date_id])

    if cur.fetchone()[0] == 0:
        cur.execute(
            """
            INSERT INTO dim_date (
                date_id, full_date, day, day_name, day_of_week,
                week_of_year, month, month_name, quarter, year,
                fiscal_quarter, fiscal_year, is_weekend, is_month_end,
                is_quarter_end, is_fiscal_quarter_end, is_year_end,
                is_fiscal_year_end, is_holiday, holiday_name, is_business_day
            )
            VALUES (
                :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,
                :11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21
            )
            """,
            [
                date_id, full_date, day, day_name, day_of_week,
                week_of_year, month, month_name, quarter, year,
                fiscal_quarter, fiscal_year, is_weekend, is_month_end_flag,
                is_quarter_end_flag, is_fiscal_quarter_end_flag, is_year_end_flag,
                is_fiscal_year_end_flag, is_holiday, holiday_name, is_business_day_flag
            ]
        )
        
        # Display with icons and details
        status_icon = "📅"
        if holiday_name:
            status_icon = "🎉"
        elif is_weekend == "Y":
            status_icon = "🏖️"
        elif is_business_day_flag == "Y":
            status_icon = "💼"
        
        # Check if this is today
        date_marker = ""
        if d == today:
            date_marker = " 👈 TODAY"
        elif d < today:
            days_ago = (today - d).days
            date_marker = f" (Past: {days_ago} day{'s' if days_ago > 1 else ''} ago)"
        
        print(f"  {status_icon} {date_id}: {day_name}, {month_name} {day}, {year}{date_marker}")
        print(f"     Type: {day_type} | Q{quarter} | FY{fiscal_year}-Q{fiscal_quarter} | Week {week_of_year}")
        
        # Show special attributes
        special_attrs = []
        if holiday_name:
            special_attrs.append(f"🎊 Holiday: {holiday_name}")
        if is_month_end_flag == "Y":
            special_attrs.append("📊 Month End")
        if is_quarter_end_flag == "Y":
            special_attrs.append("📊 Quarter End")
        if is_fiscal_quarter_end_flag == "Y":
            special_attrs.append("📊 Fiscal Quarter End")
        if is_year_end_flag == "Y":
            special_attrs.append("📊 Year End")
        if is_fiscal_year_end_flag == "Y":
            special_attrs.append("📊 Fiscal Year End (Mar 31)")
        
        for attr in special_attrs:
            print(f"     {attr}")
        
        print()
        
        rows_inserted += 1
    else:
        print(f"  ⏭️  {date_id}: Already exists, skipping")
        rows_skipped += 1

conn.commit()

# ========================================
# SUMMARY
# ========================================
print(f"{'='*70}")
print(f"✅ DIM_DATE DAILY AUTO-INCREMENT COMPLETED")
print(f"{'='*70}")
print(f"\n📊 This Run Summary:")
print(f"   Inserted: {rows_inserted} rows")
if rows_skipped > 0:
    print(f"   Skipped: {rows_skipped} rows (already exist)")
print(f"   Date range: {start_date} to {end_date}")
print(f"{'='*70}")

# Table statistics
cur.execute("""
    SELECT 
        COUNT(*) as total,
        MIN(full_date) as min_date,
        MAX(full_date) as max_date,
        SUM(CASE WHEN is_weekend = 'Y' THEN 1 ELSE 0 END) as weekends,
        SUM(CASE WHEN is_holiday = 'Y' THEN 1 ELSE 0 END) as holidays,
        SUM(CASE WHEN is_business_day = 'Y' THEN 1 ELSE 0 END) as business_days
    FROM dim_date
""")

stats = cur.fetchone()
if stats and stats[0] > 0:
    # Convert datetime to date if needed
    max_date_in_table = stats[2].date() if isinstance(stats[2], datetime) else stats[2]
    min_date_in_table = stats[1].date() if isinstance(stats[1], datetime) else stats[1]
    
    days_from_today = (max_date_in_table - today).days
    
    if days_from_today >= 0:
        coverage_status = "✅ CURRENT"
        coverage_desc = "(up to today)"
    else:
        coverage_status = "⚠️ BEHIND"
        coverage_desc = f"({abs(days_from_today)} days behind today)"
    
    print(f"\n📈 Overall Table Statistics:")
    print(f"   Total dates in dim_date: {stats[0]:,}")
    print(f"   Date range: {min_date_in_table.strftime('%Y-%m-%d')} to {max_date_in_table.strftime('%Y-%m-%d')}")
    print(f"   Coverage status: {coverage_status} {coverage_desc}")
    print(f"   ")
    print(f"   Weekends: {stats[3]:,}")
    print(f"   Holidays: {stats[4]:,}")
    print(f"   Business days: {stats[5]:,}")

# Check how many more runs needed to reach today
if max_date_in_table < today:
    remaining_days = (today - max_date_in_table).days
    remaining_runs = (remaining_days + ROWS_PER_RUN - 1) // ROWS_PER_RUN  # Ceiling division
    print(f"\n💡 Recommendation:")
    print(f"   {remaining_days} days remaining to reach today ({today})")
    print(f"   Run this script {remaining_runs} more time{'s' if remaining_runs > 1 else ''} ")
    print(f"   (at {ROWS_PER_RUN} rows per run)")
else:
    print(f"\n✅ Date dimension is fully current (up to {today})")
    print(f"   No future dates will be loaded")

cur.close()
conn.close()

print(f"\n{'='*70}")
print(f"🎉 Process Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}")
print(f"💡 Schedule this script to run daily in Airflow for continuous updates")
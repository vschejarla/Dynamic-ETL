import oracledb
from datetime import datetime, timedelta

print("📅 DIM_DATE_DW AUTOMATED LOAD STARTED")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

<<<<<<< HEAD
# ========================================
# CONFIG
# ========================================
DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}
=======
conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()
>>>>>>> 129265d43c32c9775d030d55e90451d72fc10756

# Auto-calculate range: Past 2 years up to TODAY (no future)
PAST_BUFFER_YEARS = 2

# ========================================
# INDIAN HOLIDAYS (2023-2030)
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
    next_day = d + timedelta(days=1)
    return next_day.month != d.month

def is_quarter_end(d):
    return is_month_end(d) and d.month in [3, 6, 9, 12]

def is_fiscal_quarter_end(d):
    return is_month_end(d) and d.month in [6, 9, 12, 3]

def is_year_end(d):
    return d.month == 12 and d.day == 31

def is_fiscal_year_end(d):
    return d.month == 3 and d.day == 31

def is_business_day(d, date_id):
    is_weekend = d.weekday() >= 5
    is_holiday = date_id in INDIAN_HOLIDAYS
    return not (is_weekend or is_holiday)

# ========================================
# CONNECTION
# ========================================
try:
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected to data warehouse (target_dw)\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

# ========================================
# AUTO-DETERMINE DATE RANGE
# ========================================
print("🔍 Analyzing data to determine date range...\n")

# Get today's date
today = datetime.now().date()
current_year = today.year

# Check if fact_sales exists and has data
cur.execute("""
    SELECT COUNT(*) 
    FROM user_tables 
    WHERE table_name = 'FACT_SALES'
""")
fact_table_exists = cur.fetchone()[0] > 0

if fact_table_exists:
    cur.execute("SELECT COUNT(*) FROM fact_sales")
    fact_row_count = cur.fetchone()[0]
else:
    fact_row_count = 0

# Determine date range
if fact_table_exists and fact_row_count > 0:
    print(f"📊 Found {fact_row_count:,} records in fact_sales")
    
    try:
        cur.execute("""
            SELECT 
                MIN(TO_DATE(TO_CHAR(fs.date_id), 'YYYYMMDD')),
                MAX(TO_DATE(TO_CHAR(fs.date_id), 'YYYYMMDD'))
            FROM fact_sales fs
        """)
        result = cur.fetchone()
        
        if result[0] and result[1]:
            data_start_date = result[0].date() if isinstance(result[0], datetime) else result[0]
            data_end_date = result[1].date() if isinstance(result[1], datetime) else result[1]
            
            print(f"📅 Data date range: {data_start_date} to {data_end_date}")
            
            # Apply buffers - but END DATE is TODAY (no future)
            START_DATE = datetime(data_start_date.year - PAST_BUFFER_YEARS, 1, 1).date()
            END_DATE = today  # NO FUTURE DATES
            
            print(f"📅 With {PAST_BUFFER_YEARS}-year past buffer, up to TODAY")
        else:
            raise ValueError("No valid dates found")
    
    except Exception as e:
        print(f"⚠️  Could not determine date range from data: {e}")
        print("Using current year logic instead...")
        START_DATE = datetime(current_year - PAST_BUFFER_YEARS, 1, 1).date()
        END_DATE = today
else:
    # No data yet - use past 2 years to today
    print("ℹ️  No fact data found, using past 2 years to today")
    START_DATE = datetime(current_year - PAST_BUFFER_YEARS, 1, 1).date()
    END_DATE = today

print(f"\n{'='*70}")
print(f"DATE DIMENSION LOAD RANGE:")
print(f"{'='*70}")
print(f"   Start: {START_DATE} (Jan 1, {START_DATE.year})")
print(f"   End:   {END_DATE} (TODAY)")
print(f"   Total days: {(END_DATE - START_DATE).days + 1:,}")
print(f"   Years: {START_DATE.year} to {END_DATE.year}")
print(f"   ℹ️  No future dates will be loaded")
print(f"{'='*70}\n")

# ========================================
# CHECK EXISTING DATA
# ========================================
cur.execute("SELECT COUNT(*) FROM dim_date_dw")
existing_count = cur.fetchone()[0]

if existing_count > 0:
    cur.execute("SELECT MIN(full_date), MAX(full_date) FROM dim_date_dw")
    result = cur.fetchone()
    existing_start = result[0].date() if isinstance(result[0], datetime) else result[0]
    existing_end = result[1].date() if isinstance(result[1], datetime) else result[1]
    
    print(f"📦 Existing dates in dim_date_dw: {existing_count:,}")
    print(f"   Range: {existing_start} to {existing_end}\n")
else:
    print(f"📦 dim_date_dw is empty - will perform full load\n")

# ========================================
# LOAD DATE DIMENSION
# ========================================
print(f"⚙️  Loading date dimension...")
print(f"{'='*70}\n")

total_days = (END_DATE - START_DATE).days + 1
inserted_count = 0
skipped_count = 0

BATCH_SIZE = 1000
batch_data = []
batch_number = 0

for i in range(total_days):
    d = START_DATE + timedelta(days=i)

    date_id = int(d.strftime("%Y%m%d"))
    
    # Check if exists
    cur.execute("SELECT COUNT(*) FROM dim_date_dw WHERE date_id = :1", [date_id])
    exists = cur.fetchone()[0]

    if exists == 0:
        # Calculate all attributes
        full_date = d
        day = d.day
        day_name = d.strftime("%A")
        day_of_week = d.weekday() + 1
        week_of_year = int(d.strftime("%W"))  # Monday-based
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
        
        batch_data.append((
            date_id, full_date, day, day_name, day_of_week,
            week_of_year, month, month_name, quarter, year,
            fiscal_quarter, fiscal_year, is_weekend, is_month_end_flag,
            is_quarter_end_flag, is_fiscal_quarter_end_flag, is_year_end_flag,
            is_fiscal_year_end_flag, is_holiday, holiday_name, is_business_day_flag
        ))
        
        inserted_count += 1
        
        # Commit batch
        if len(batch_data) >= BATCH_SIZE:
            batch_number += 1
            cur.executemany("""
                INSERT INTO dim_date_dw (
                    date_id, full_date, day, day_name, day_of_week,
                    week_of_year, month, month_name, quarter, year,
                    fiscal_quarter, fiscal_year, is_weekend, is_month_end,
                    is_quarter_end, is_fiscal_quarter_end, is_year_end,
                    is_fiscal_year_end, is_holiday, holiday_name, is_business_day
                ) VALUES (
                    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,
                    :11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21
                )
            """, batch_data)
            conn.commit()
            
            # Calculate progress
            progress = (inserted_count / total_days) * 100
            print(f"   ✅ Batch #{batch_number}: {len(batch_data)} dates inserted")
            print(f"      Progress: {inserted_count:,}/{total_days:,} ({progress:.1f}%)")
            print(f"      Date range: {batch_data[0][1]} to {batch_data[-1][1]}\n")
            
            batch_data = []
    else:
        skipped_count += 1

# Insert remaining batch
if batch_data:
    batch_number += 1
    cur.executemany("""
        INSERT INTO dim_date_dw (
            date_id, full_date, day, day_name, day_of_week,
            week_of_year, month, month_name, quarter, year,
            fiscal_quarter, fiscal_year, is_weekend, is_month_end,
            is_quarter_end, is_fiscal_quarter_end, is_year_end,
            is_fiscal_year_end, is_holiday, holiday_name, is_business_day
        ) VALUES (
            :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,
            :11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21
        )
    """, batch_data)
    conn.commit()
    print(f"   ✅ Batch #{batch_number} (final): {len(batch_data)} dates inserted")
    print(f"      Date range: {batch_data[0][1]} to {batch_data[-1][1]}\n")

# ========================================
# FINAL STATISTICS
# ========================================
print(f"\n{'='*70}")
print(f"LOAD STATISTICS")
print(f"{'='*70}\n")

print(f"📊 Date Dimension Load Summary:")
print(f"   Dates inserted: {inserted_count:,}")
print(f"   Dates skipped (already exist): {skipped_count:,}")
print(f"   Total processed: {total_days:,}")
print(f"   Batches committed: {batch_number}")

# Get final table statistics
cur.execute("""
    SELECT 
        COUNT(*) as total_dates,
        MIN(full_date) as min_date,
        MAX(full_date) as max_date,
        SUM(CASE WHEN is_weekend = 'Y' THEN 1 ELSE 0 END) as weekend_count,
        SUM(CASE WHEN is_weekend = 'N' THEN 1 ELSE 0 END) as weekday_count,
        SUM(CASE WHEN is_holiday = 'Y' THEN 1 ELSE 0 END) as holiday_count,
        SUM(CASE WHEN is_business_day = 'Y' THEN 1 ELSE 0 END) as business_day_count
    FROM dim_date_dw
""")

stats = cur.fetchone()
max_date_in_table = stats[2].date() if isinstance(stats[2], datetime) else stats[2]
min_date_in_table = stats[1].date() if isinstance(stats[1], datetime) else stats[1]

print(f"\n{'='*70}")
print(f"📈 dim_date_dw Table Statistics:")
print(f"{'='*70}")
print(f"   Total dates: {stats[0]:,}")
print(f"   Date range: {min_date_in_table} to {max_date_in_table}")
print(f"   Coverage: Up to {'TODAY ✅' if max_date_in_table == today else max_date_in_table}")
print(f"   ")
print(f"   Weekends: {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
print(f"   Weekdays: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
print(f"   Holidays: {stats[5]:,} ({stats[5]/stats[0]*100:.1f}%)")
print(f"   Business days: {stats[6]:,} ({stats[6]/stats[0]*100:.1f}%)")

# Year-wise breakdown
cur.execute("""
    SELECT year, COUNT(*) as cnt
    FROM dim_date_dw
    GROUP BY year
    ORDER BY year
""")

print(f"\n{'='*70}")
print(f"📅 Year-wise Date Count:")
print(f"{'='*70}")
for row in cur.fetchall():
    year_dates = row[1]
    is_leap = (row[0] % 4 == 0 and row[0] % 100 != 0) or (row[0] % 400 == 0)
    expected = 366 if is_leap else 365
    
    # For current year, it might be partial
    if row[0] == current_year:
        status = "✅"
        print(f"   {status} {row[0]}: {year_dates} days (Partial year - up to {today})")
    else:
        status = "✅" if year_dates == expected else "⚠️"
        leap_indicator = " (Leap Year)" if is_leap else ""
        print(f"   {status} {row[0]}: {year_dates} days (Expected: {expected}){leap_indicator}")

cur.close()
conn.close()

print(f"\n{'='*70}")
print(f"🎉 DIM_DATE_DW LOAD COMPLETED SUCCESSFULLY")
print(f"{'='*70}")
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n💡 This script loads dates up to TODAY only - no future dates")
print(f"💡 Run this script weekly to keep the data warehouse current")
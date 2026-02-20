import oracledb
from datetime import datetime, timedelta

print("📅 DIM_DATE_DW INCREMENTAL LOAD")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

# ========================================
# CONFIG - CONSISTENT ACROSS ALL SCRIPTS
# ========================================
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"
PROCESSED_LOG = "/opt/airflow/data_extracts/processed_files.log"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

PAST_BUFFER_YEARS = 2  # Load 2 years of historical data

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
    20240126: "Republic Day", 20240308: "Maha Shivaratmi", 20240325: "Holi",
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
    20290126: "Republic Day", 20290214: "Maha Shivaratmi", 20290301: "Holi",
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
conn = None
cur = None

try:
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected to data warehouse\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

try:
    # ========================================
    # DETERMINE DATE RANGE (ONLY UP TO TODAY)
    # ========================================
    print("🔍 Analyzing existing date dimension...\n")
    
    today = datetime.now().date()
    current_year = today.year
    
    # Check existing dates
    cur.execute("SELECT COUNT(*), MIN(full_date), MAX(full_date) FROM dim_date_dw")
    result = cur.fetchone()
    existing_count = result[0]
    
    if existing_count > 0:
        existing_start = result[1].date() if isinstance(result[1], datetime) else result[1]
        existing_end = result[2].date() if isinstance(result[2], datetime) else result[2]
        
        print(f"📦 Existing dates: {existing_count:,} records")
        print(f"   Range: {existing_start} to {existing_end}")
    else:
        existing_start = None
        existing_end = None
        print(f"📦 Date dimension is empty")
    
    # Define required range
    required_start = datetime(current_year - PAST_BUFFER_YEARS, 1, 1).date()
    required_end = today  # CRITICAL: Only load up to today
    
    print(f"\n📅 Today's date: {today}")
    print(f"📅 Required range: {required_start} to {required_end}")
    
    # Determine what to load
    load_ranges = []
    
    if existing_count == 0:
        # Initial load
        print(f"\n🔄 Initial load required")
        load_ranges.append((required_start, required_end, "INITIAL LOAD"))
    elif existing_end < required_end:
        # Need to add recent dates
        load_start = existing_end + timedelta(days=1)
        print(f"\n🔄 Incremental load required")
        print(f"   Adding dates: {load_start} to {required_end}")
        load_ranges.append((load_start, required_end, "INCREMENTAL"))
    else:
        print(f"\n✅ Date dimension is current (up to {existing_end})")
        print(f"✅ No new data to load\n")
        load_ranges = []  # Empty - nothing to load
    
    # ========================================
    # LOAD DATES (ONLY IF NEEDED)
    # ========================================
    if load_ranges:
        def load_date_range(start_date, end_date, range_name):
            """Load a range of dates"""
            print(f"\n⚙️  Loading {range_name}...")
            print(f"   Range: {start_date} to {end_date}")
            
            total_days = (end_date - start_date).days + 1
            inserted_count = 0
            
            BATCH_SIZE = 1000
            batch_data = []
            
            for i in range(total_days):
                d = start_date + timedelta(days=i)
                date_id = int(d.strftime("%Y%m%d"))
                
                # Calculate attributes
                full_date = d
                day = d.day
                day_name = d.strftime("%A")
                day_of_week = d.weekday() + 1
                week_of_year = int(d.strftime("%W"))
                month = d.month
                month_name = d.strftime("%B")
                quarter = (month - 1) // 3 + 1
                year = d.year
                
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
                    
                    progress = (inserted_count / total_days) * 100
                    print(f"   ✅ Progress: {inserted_count:,}/{total_days:,} ({progress:.1f}%)")
                    batch_data = []
            
            # Insert remaining
            if batch_data:
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
                print(f"   ✅ Final batch: {len(batch_data)} records")
            
            return inserted_count
        
        # Execute load
        total_inserted = 0
        for start_date, end_date, range_name in load_ranges:
            total_inserted += load_date_range(start_date, end_date, range_name)
        
        # ========================================
        # FINAL STATISTICS (AFTER LOAD)
        # ========================================
        print(f"\n{'='*70}")
        print(f"LOAD COMPLETE")
        print(f"{'='*70}")
        
        print(f"\n📊 Summary:")
        print(f"   Records inserted: {total_inserted:,}")
    
    # ========================================
    # DIMENSION STATISTICS (ALWAYS SHOW)
    # ========================================
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            MIN(full_date) as min_date,
            MAX(full_date) as max_date,
            SUM(CASE WHEN is_business_day = 'Y' THEN 1 ELSE 0 END) as business_days
        FROM dim_date_dw
    """)
    
    stats = cur.fetchone()
    max_date_in_table = stats[2].date() if isinstance(stats[2], datetime) else stats[2]
    min_date_in_table = stats[1].date() if isinstance(stats[1], datetime) else stats[1]
    
    print(f"\n📈 Dimension Statistics:")
    print(f"   Total records: {stats[0]:,}")
    print(f"   Range: {min_date_in_table} to {max_date_in_table}")
    print(f"   Business days: {stats[3]:,}")
    
    days_behind = (today - max_date_in_table).days
    if days_behind == 0:
        print(f"\n✅ Dimension is CURRENT (up to today)")
    else:
        print(f"\n⚠️  Dimension is {days_behind} day(s) behind")

except Exception as e:
    print(f"\n❌ Error during execution: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

finally:
    # Safe cleanup - always runs
    try:
        if cur:
            cur.close()
    except:
        pass
    
    try:
        if conn:
            conn.close()
    except:
        pass

print(f"\n🎉 Script completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
import os
import pandas as pd
import oracledb
from datetime import datetime

print("🚚 DISTRIBUTOR DIMENSION INCREMENTAL LOAD")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ========================================
# CONFIG - CONSISTENT ACROSS ALL SCRIPTS
# ========================================
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"
PROCESSED_LOG = "/opt/airflow/data_extracts/processed_distributors.log"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

# ========================================
# FILE TRACKING - NO FILE MOVEMENT
# ========================================
def is_file_processed(filename):
    """Check if file has been processed before"""
    if not os.path.exists(PROCESSED_LOG):
        return False
    with open(PROCESSED_LOG, 'r') as f:
        processed = f.read().splitlines()
    return any(line.startswith(filename) for line in processed)

def mark_file_processed(filename):
    """Mark file as processed"""
    os.makedirs(os.path.dirname(PROCESSED_LOG), exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(PROCESSED_LOG, 'a') as f:
        f.write(f"{filename}|{timestamp}\n")

# ========================================
# DATA CLEANING
# ========================================
def clean_distributor_name(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN DISTRIBUTOR"
    return ' '.join(str(val).strip().split()).upper()

def clean_city(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    return str(val).strip().title()

def clean_state(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "UNKNOWN"
    return str(val).strip().title()

def clean_distributor_type(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return "Local"
    
    val_upper = str(val).strip().upper()
    type_map = {
        "NATIONAL": "National", "NAT": "National", "PAN INDIA": "National",
        "REGIONAL": "Regional", "REG": "Regional", "MULTI STATE": "Regional",
        "LOCAL": "Local", "CITY": "Local", "DISTRICT": "Local"
    }
    
    for key, value in type_map.items():
        if key in val_upper:
            return value
    return val.strip().title()

def clean_active_flag(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return 'Y'
    val_upper = str(val).strip().upper()
    return 'Y' if val_upper in ['Y', 'YES', '1', 'TRUE', 'ACTIVE', 'A'] else 'N'

def clean_onboarding_date(val):
    if pd.isna(val) or val is None or val == '':
        return None
    try:
        parsed_date = pd.to_datetime(val, errors='coerce')
        if pd.isna(parsed_date):
            return None
        min_date = datetime(1990, 1, 1)
        max_date = datetime.now()
        if parsed_date < min_date or parsed_date > max_date:
            return None
        return parsed_date
    except:
        return None

# ========================================
# CHECK INCOMING DIRECTORY
# ========================================
if not os.path.exists(INCOMING_DIR):
    print(f"ℹ️  Incoming directory does not exist: {INCOMING_DIR}")
    print("✅ This is normal if initial load hasn't run yet")
    print("✅ Exiting gracefully - no data to process\n")
    exit(0)

# ========================================
# FIND UNPROCESSED FILES
# ========================================
all_files = sorted([f for f in os.listdir(INCOMING_DIR) if f.startswith("sales_") and f.endswith(".csv")], reverse=True)

if not all_files:
    print(f"ℹ️  No sales CSV files found in: {INCOMING_DIR}")
    print("✅ Exiting gracefully - no data to process\n")
    exit(0)

# Find first unprocessed file
source_file = None
for f in all_files:
    if not is_file_processed(f):
        source_file = f
        break

if source_file is None:
    print(f"ℹ️  All {len(all_files)} sales file(s) already processed for distributors")
    print("✅ Exiting gracefully - no new data to process\n")
    exit(0)

file_path = os.path.join(INCOMING_DIR, source_file)
print(f"📄 Processing file: {source_file}")
print(f"📁 Location: {file_path}\n")

# ========================================
# READ AND CLEAN DATA
# ========================================
try:
    df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA'])
    print(f"📊 Initial rows: {len(df):,}")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    exit(1)

if len(df) == 0:
    print("⚠️  Empty file - marking as processed")
    mark_file_processed(source_file)
    print("✅ Exiting gracefully\n")
    exit(0)

df.columns = df.columns.str.strip().str.upper()

required_cols = [
    "DISTRIBUTOR_NAME", "DISTRIBUTOR_CITY", "DISTRIBUTOR_STATE",
    "DISTRIBUTOR_TYPE", "ONBOARDING_DATE", "ACTIVE_FLAG"
]

missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    print(f"❌ Missing columns: {missing_cols}")
    print(f"⚠️  Marking file as processed to prevent retry loop")
    mark_file_processed(source_file)
    exit(1)

# Clean data
df["DISTRIBUTOR_NAME_CLEAN"] = df["DISTRIBUTOR_NAME"].apply(clean_distributor_name)
df["DISTRIBUTOR_CITY_CLEAN"] = df["DISTRIBUTOR_CITY"].apply(clean_city)
df["DISTRIBUTOR_STATE_CLEAN"] = df["DISTRIBUTOR_STATE"].apply(clean_state)
df["DISTRIBUTOR_TYPE_CLEAN"] = df["DISTRIBUTOR_TYPE"].apply(clean_distributor_type)
df["ACTIVE_FLAG_CLEAN"] = df["ACTIVE_FLAG"].apply(clean_active_flag)
df["ONBOARDING_DATE_CLEAN"] = df["ONBOARDING_DATE"].apply(clean_onboarding_date)

# Filter valid rows
df_valid = df[
    (df["DISTRIBUTOR_NAME_CLEAN"].notna()) &
    (df["DISTRIBUTOR_NAME_CLEAN"] != "UNKNOWN DISTRIBUTOR") &
    (df["DISTRIBUTOR_CITY_CLEAN"].notna()) &
    (df["DISTRIBUTOR_STATE_CLEAN"].notna())
].copy()

print(f"✅ Valid rows: {len(df_valid):,}")
if len(df) > len(df_valid):
    print(f"⚠️  Invalid rows: {len(df) - len(df_valid):,}")

# Deduplicate
dist_df = df_valid[[
    "DISTRIBUTOR_NAME_CLEAN", "DISTRIBUTOR_CITY_CLEAN", "DISTRIBUTOR_STATE_CLEAN",
    "DISTRIBUTOR_TYPE_CLEAN", "ONBOARDING_DATE_CLEAN", "ACTIVE_FLAG_CLEAN"
]].drop_duplicates(subset=["DISTRIBUTOR_NAME_CLEAN", "DISTRIBUTOR_CITY_CLEAN", "DISTRIBUTOR_STATE_CLEAN"])

print(f"🚚 Unique distributors: {len(dist_df)}\n")

if len(dist_df) == 0:
    print("⚠️  No valid distributor records after cleaning")
    mark_file_processed(source_file)
    print("✅ File marked as processed - exiting gracefully\n")
    exit(0)

# ========================================
# CONNECT TO DATABASE
# ========================================
print("🔌 Connecting to database...")
conn = None
cur = None

try:
    conn = oracledb.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected\n")
    
    # ========================================
    # CREATE STAGING TABLE
    # ========================================
    print("🏗️  Creating staging table...")
    
    try:
        cur.execute("DROP TABLE distributor_staging")
    except:
        pass
    
    cur.execute("""
        CREATE TABLE distributor_staging (
            dist_key NUMBER,
            dist_name VARCHAR2(50),
            dist_type VARCHAR2(30),
            dist_city VARCHAR2(30),
            dist_state VARCHAR2(30),
            dist_onboarding_date DATE,
            dist_active_flag CHAR(1),
            operation VARCHAR2(10)
        )
    """)
    conn.commit()
    print("✅ Staging table created\n")
    
    # ========================================
    # LOAD EXISTING DISTRIBUTORS
    # ========================================
    cur.execute("""
        SELECT dist_key, dist_name, dist_city, dist_state,
               dist_type, dist_onboarding_date, dist_active_flag
        FROM dim_distributor_dw
    """)
    
    dist_cache = {}
    for r in cur.fetchall():
        bk = (r[1], r[2], r[3])  # name, city, state
        dist_cache[bk] = {
            'dist_key': r[0], 'dist_type': r[4],
            'dist_onboarding_date': r[5], 'dist_active_flag': r[6]
        }
    
    print(f"📦 Existing distributors: {len(dist_cache)}")
    
    # Get next key
    cur.execute("SELECT NVL(MAX(dist_key),0) FROM dim_distributor_dw")
    next_key = cur.fetchone()[0] + 1
    
    # ========================================
    # CLASSIFY RECORDS
    # ========================================
    insert_records = []
    update_records = []
    
    for _, r in dist_df.iterrows():
        bk = (r["DISTRIBUTOR_NAME_CLEAN"], r["DISTRIBUTOR_CITY_CLEAN"], r["DISTRIBUTOR_STATE_CLEAN"])
        
        if bk not in dist_cache:
            insert_records.append({'dist_key': next_key, 'bk': bk, 'data': r})
            next_key += 1
        else:
            existing = dist_cache[bk]
            
            # Compare dates safely
            existing_date = existing['dist_onboarding_date']
            new_date = r["ONBOARDING_DATE_CLEAN"]
            
            date_changed = False
            if existing_date is None and new_date is not None:
                date_changed = True
            elif existing_date is not None and new_date is not None:
                if isinstance(existing_date, datetime):
                    existing_date = existing_date.date()
                if isinstance(new_date, pd.Timestamp):
                    new_date = new_date.date()
                date_changed = existing_date != new_date
            
            if (
                r["DISTRIBUTOR_TYPE_CLEAN"] != existing['dist_type'] or
                r["ACTIVE_FLAG_CLEAN"] != existing['dist_active_flag'] or
                date_changed
            ):
                update_records.append({
                    'dist_key': existing['dist_key'], 'bk': bk, 'data': r
                })
    
    print(f"   ➕ New: {len(insert_records)}")
    print(f"   🔄 Updates: {len(update_records)}\n")
    
    # ========================================
    # LOAD STAGING AND MERGE
    # ========================================
    staging_data = []
    
    for record in insert_records + update_records:
        r = record['data']
        staging_data.append((
            record['dist_key'],
            r["DISTRIBUTOR_NAME_CLEAN"][:50],
            r["DISTRIBUTOR_TYPE_CLEAN"][:30],
            r["DISTRIBUTOR_CITY_CLEAN"][:30],
            r["DISTRIBUTOR_STATE_CLEAN"][:30],
            r["ONBOARDING_DATE_CLEAN"],
            r["ACTIVE_FLAG_CLEAN"],
            'INSERT' if record in insert_records else 'UPDATE'
        ))
    
    if staging_data:
        print(f"💾 Loading {len(staging_data)} records...")
        
        cur.executemany("""
            INSERT INTO distributor_staging (
                dist_key, dist_name, dist_type, dist_city, dist_state,
                dist_onboarding_date, dist_active_flag, operation
            ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)
        """, staging_data)
        
        cur.execute("""
            MERGE INTO dim_distributor_dw tgt
            USING distributor_staging stg
            ON (tgt.dist_key = stg.dist_key)
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.dist_type = stg.dist_type,
                    tgt.dist_onboarding_date = stg.dist_onboarding_date,
                    tgt.dist_active_flag = stg.dist_active_flag
            WHEN NOT MATCHED THEN
                INSERT (
                    dist_key, dist_name, dist_type, dist_city, dist_state,
                    dist_onboarding_date, dist_active_flag
                )
                VALUES (
                    stg.dist_key, stg.dist_name, stg.dist_type, stg.dist_city, stg.dist_state,
                    stg.dist_onboarding_date, stg.dist_active_flag
                )
        """)
        
        conn.commit()
        print(f"✅ Merged {cur.rowcount} records\n")
    else:
        print("ℹ️  No changes detected - all data already current\n")
    
    # Cleanup
    print("🧹 Cleaning up...")
    cur.execute("DROP TABLE distributor_staging")
    conn.commit()
    
    # Mark as processed
    mark_file_processed(source_file)
    print(f"✅ File marked as processed\n")
    
    # ========================================
    # STATISTICS
    # ========================================
    print("="*60)
    print("FINAL STATISTICS")
    print("="*60)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN dist_active_flag = 'Y' THEN 1 ELSE 0 END) as active,
            SUM(CASE WHEN dist_active_flag = 'N' THEN 1 ELSE 0 END) as inactive
        FROM dim_distributor_dw
    """)
    stats = cur.fetchone()
    print(f"📊 Total distributors: {stats[0]}")
    print(f"   Active: {stats[1]}, Inactive: {stats[2]}")
    
    cur.execute("""
        SELECT dist_type, COUNT(*) as cnt
        FROM dim_distributor_dw
        GROUP BY dist_type
        ORDER BY cnt DESC
    """)
    print(f"\n📊 By Type:")
    for row in cur.fetchall():
        print(f"   {row[0]}: {row[1]}")

except Exception as e:
    print(f"❌ Error: {e}")
    raise

finally:
    if cur:
        try:
            cur.close()
        except:
            pass
    if conn:
        try:
            conn.close()
        except:
            pass

print(f"\n🎉 Load completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
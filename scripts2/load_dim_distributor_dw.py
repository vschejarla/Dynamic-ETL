import os
import pandas as pd
import oracledb
from datetime import datetime
import re

print("🚚 DISTRIBUTOR DIMENSION LOAD STARTED")

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

DB_CONFIG = {
    "user": "target_dw",
    "password": "target_dw123",
    "dsn": "host.docker.internal/orcl"
}

# ---------------------------------------------------
# DATA QUALITY & NORMALIZATION HELPERS
# ---------------------------------------------------
def clean_distributor_name(val):
    """Clean and standardize distributor names"""
    if pd.isna(val) or val is None or val == '':
        return "UNKNOWN DISTRIBUTOR"
    
    # Remove extra spaces
    name = ' '.join(str(val).strip().split())
    return name.upper()

def clean_city(val):
    """Clean city names"""
    if pd.isna(val) or val is None or val == '':
        return "UNKNOWN"
    
    city = str(val).strip().title()  # Title case for cities
    return city

def clean_state(val):
    """Clean state names"""
    if pd.isna(val) or val is None or val == '':
        return "UNKNOWN"
    
    state = str(val).strip().title()  # Title case for states
    return state

def clean_distributor_type(val):
    """Standardize distributor type"""
    if pd.isna(val) or val is None or val == '':
        return "Local"
    
    val_upper = str(val).strip().upper()
    
    # Standardization mapping
    type_map = {
        "NATIONAL": "National",
        "NAT": "National",
        "PAN INDIA": "National",
        "ALL INDIA": "National",
        
        "REGIONAL": "Regional",
        "REG": "Regional",
        "MULTI STATE": "Regional",
        "MULTI-STATE": "Regional",
        
        "LOCAL": "Local",
        "CITY": "Local",
        "DISTRICT": "Local",
        "STATE": "Local"
    }
    
    # Direct match
    if val_upper in type_map:
        return type_map[val_upper]
    
    # Partial match
    for key, value in type_map.items():
        if key in val_upper:
            return value
    
    return val.strip().title()

def clean_active_flag(val):
    """Validate and clean active flag"""
    if pd.isna(val) or val is None or val == '':
        return 'Y'  # Default to active
    
    val_upper = str(val).strip().upper()
    
    if val_upper in ['Y', 'YES', '1', 'TRUE', 'ACTIVE', 'A']:
        return 'Y'
    elif val_upper in ['N', 'NO', '0', 'FALSE', 'INACTIVE', 'I']:
        return 'N'
    else:
        return 'Y'  # Default to active if unclear

def clean_onboarding_date(val):
    """Parse and validate onboarding date"""
    if pd.isna(val) or val is None or val == '':
        return None
    
    try:
        # Try to parse the date
        if isinstance(val, (pd.Timestamp, datetime)):
            parsed_date = val
        else:
            parsed_date = pd.to_datetime(val, errors='coerce')
        
        if pd.isna(parsed_date):
            return None
        
        # Validate date range (1990 to today)
        min_date = datetime(1990, 1, 1)
        max_date = datetime.now()
        
        if parsed_date < min_date or parsed_date > max_date:
            return None
        
        return parsed_date
    
    except:
        return None

def validate_distributor_type_location(dist_type, city, state):
    """Business logic validation for distributor type vs location"""
    # National distributors should be in major metros
    if dist_type == "National":
        major_metros = [
            "MUMBAI", "DELHI", "NEW DELHI", "BANGALORE", "CHENNAI", 
            "HYDERABAD", "KOLKATA", "AHMEDABAD", "PUNE"
        ]
        city_upper = city.upper()
        
        # Check if in major metro
        is_metro = any(metro in city_upper for metro in major_metros)
        
        if not is_metro:
            # Could downgrade to Regional or flag as warning
            return "Regional", f"⚠️  National distributor not in major metro: {city}"
    
    return dist_type, None

# ---------------------------------------------------
# 1️⃣ READ LATEST FILE
# ---------------------------------------------------
files = sorted(
    [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")],
    reverse=True
)

if not files:
    raise FileNotFoundError("❌ No incoming files found")

latest_file = os.path.join(INCOMING_DIR, files[0])
print(f"📄 Reading: {files[0]}")

# Read with dtype=str to preserve all values
df = pd.read_csv(latest_file, delimiter="|", dtype=str, na_values=['', 'NULL', 'null', 'NA'])

print(f"📊 Initial rows: {len(df)}")

# ---------------------------------------------------
# 2️⃣ CLEAN & NORMALIZE DISTRIBUTOR DATA
# ---------------------------------------------------
print("\n🧹 Cleaning and normalizing data...")

# Required columns
required_cols = [
    "DISTRIBUTOR_NAME",
    "DISTRIBUTOR_CITY",
    "DISTRIBUTOR_STATE",
    "DISTRIBUTOR_TYPE",
    "ONBOARDING_DATE",
    "ACTIVE_FLAG"
]

# Check if all required columns exist
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    raise ValueError(f"❌ Missing required columns: {missing_cols}")

# Apply cleaning functions
df["DISTRIBUTOR_NAME_CLEAN"] = df["DISTRIBUTOR_NAME"].apply(clean_distributor_name)
df["DISTRIBUTOR_CITY_CLEAN"] = df["DISTRIBUTOR_CITY"].apply(clean_city)
df["DISTRIBUTOR_STATE_CLEAN"] = df["DISTRIBUTOR_STATE"].apply(clean_state)
df["DISTRIBUTOR_TYPE_CLEAN"] = df["DISTRIBUTOR_TYPE"].apply(clean_distributor_type)
df["ACTIVE_FLAG_CLEAN"] = df["ACTIVE_FLAG"].apply(clean_active_flag)
df["ONBOARDING_DATE_CLEAN"] = df["ONBOARDING_DATE"].apply(clean_onboarding_date)

# Business logic validation
validation_warnings = []
validated_types = []

for idx, row in df.iterrows():
    validated_type, warning = validate_distributor_type_location(
        row["DISTRIBUTOR_TYPE_CLEAN"],
        row["DISTRIBUTOR_CITY_CLEAN"],
        row["DISTRIBUTOR_STATE_CLEAN"]
    )
    validated_types.append(validated_type)
    if warning:
        validation_warnings.append(warning)

df["DISTRIBUTOR_TYPE_CLEAN"] = validated_types

# Data quality validation
print("\n📋 Data Quality Checks:")

# Check for required fields
null_count_name = df["DISTRIBUTOR_NAME_CLEAN"].isna().sum()
null_count_city = df["DISTRIBUTOR_CITY_CLEAN"].isna().sum()
null_count_state = df["DISTRIBUTOR_STATE_CLEAN"].isna().sum()

if null_count_name > 0:
    print(f"   ⚠️  DISTRIBUTOR_NAME: {null_count_name} null values")
if null_count_city > 0:
    print(f"   ⚠️  DISTRIBUTOR_CITY: {null_count_city} null values")
if null_count_state > 0:
    print(f"   ⚠️  DISTRIBUTOR_STATE: {null_count_state} null values")

# Show validation warnings
if validation_warnings:
    print(f"\n📢 Business Logic Warnings:")
    for warning in validation_warnings[:5]:  # Show first 5
        print(f"   {warning}")
    if len(validation_warnings) > 5:
        print(f"   ... and {len(validation_warnings) - 5} more warnings")

# Date validation
null_dates = df["ONBOARDING_DATE_CLEAN"].isna().sum()
if null_dates > 0:
    print(f"   ℹ️  {null_dates} invalid/missing onboarding dates (will use NULL)")

# Active flag distribution
active_count = (df["ACTIVE_FLAG_CLEAN"] == "Y").sum()
inactive_count = (df["ACTIVE_FLAG_CLEAN"] == "N").sum()
print(f"   ℹ️  Active: {active_count}, Inactive: {inactive_count}")

# Remove completely invalid rows (missing critical fields)
df_valid = df[
    (df["DISTRIBUTOR_NAME_CLEAN"].notna()) &
    (df["DISTRIBUTOR_NAME_CLEAN"] != "UNKNOWN DISTRIBUTOR") &
    (df["DISTRIBUTOR_CITY_CLEAN"].notna()) &
    (df["DISTRIBUTOR_STATE_CLEAN"].notna())
].copy()

print(f"\n✅ Valid rows: {len(df_valid)}")
print(f"❌ Invalid rows removed: {len(df) - len(df_valid)}")

# ---------------------------------------------------
# 3️⃣ DEDUP DISTRIBUTORS
# ---------------------------------------------------
dist_cols = [
    "DISTRIBUTOR_NAME_CLEAN",
    "DISTRIBUTOR_CITY_CLEAN",
    "DISTRIBUTOR_STATE_CLEAN",
    "DISTRIBUTOR_TYPE_CLEAN",
    "ONBOARDING_DATE_CLEAN",
    "ACTIVE_FLAG_CLEAN"
]

# Deduplicate based on business key
dist_df = df_valid[dist_cols].drop_duplicates(
    subset=["DISTRIBUTOR_NAME_CLEAN", "DISTRIBUTOR_CITY_CLEAN", "DISTRIBUTOR_STATE_CLEAN"]
)

print(f"\n🚚 Unique distributors in file: {len(dist_df)}")

# ---------------------------------------------------
# 4️⃣ CONNECT TO ORACLE
# ---------------------------------------------------
print("\n🔌 Connecting to Oracle...")
conn = oracledb.connect(**DB_CONFIG)
cur = conn.cursor()
print("✅ Connected successfully")

# ---------------------------------------------------
# 5️⃣ CACHE EXISTING DISTRIBUTORS
# ---------------------------------------------------
cur.execute("""
    SELECT
        dist_name,
        dist_city,
        dist_state,
        dist_key
    FROM dim_distributor_dw
""")

dist_cache = {
    (r[0], r[1], r[2]): r[3]
    for r in cur.fetchall()
}

print(f"📦 Existing distributors in DW: {len(dist_cache)}")

# Get next key
cur.execute("SELECT NVL(MAX(dist_key),0) FROM dim_distributor_dw")
next_key = cur.fetchone()[0] + 1

# ---------------------------------------------------
# 6️⃣ DETECT & INSERT NEW ROWS
# ---------------------------------------------------
new_rows = []

for _, r in dist_df.iterrows():
    key = (
        r["DISTRIBUTOR_NAME_CLEAN"],
        r["DISTRIBUTOR_CITY_CLEAN"],
        r["DISTRIBUTOR_STATE_CLEAN"]
    )

    if key not in dist_cache:
        # Prepare row for insert
        new_rows.append((
            next_key,
            r["DISTRIBUTOR_NAME_CLEAN"][:50],      # VARCHAR2(50)
            r["DISTRIBUTOR_TYPE_CLEAN"][:30],      # VARCHAR2(30)
            r["DISTRIBUTOR_CITY_CLEAN"][:30],      # VARCHAR2(30)
            r["DISTRIBUTOR_STATE_CLEAN"][:30],     # VARCHAR2(30)
            r["ONBOARDING_DATE_CLEAN"],            # DATE (can be None)
            r["ACTIVE_FLAG_CLEAN"]                 # CHAR(1)
        ))
        
        # Update cache
        dist_cache[key] = next_key
        next_key += 1

print(f"🆕 New distributors to insert: {len(new_rows)}")

# ---------------------------------------------------
# 7️⃣ BULK INSERT
# ---------------------------------------------------
if new_rows:
    try:
        cur.executemany("""
            INSERT INTO dim_distributor_dw (
                dist_key,
                dist_name,
                dist_type,
                dist_city,
                dist_state,
                dist_onboarding_date,
                dist_active_flag
            ) VALUES (:1,:2,:3,:4,:5,:6,:7)
        """, new_rows)

        conn.commit()
        print(f"✅ Inserted {len(new_rows)} new distributors")
        
        # Display sample distributors
        print("\n📋 Sample of inserted distributors:")
        for row in new_rows[:5]:  # Show first 5
            dist_name = row[1]
            dist_type = row[2]
            location = f"{row[3]}, {row[4]}"
            status = "Active" if row[6] == 'Y' else "Inactive"
            print(f"   → {dist_name} | {dist_type} | {location} | {status}")
        
        if len(new_rows) > 5:
            print(f"   ... and {len(new_rows) - 5} more")
    
    except Exception as e:
        conn.rollback()
        print(f"❌ Error inserting distributors: {e}")
        raise
else:
    print("ℹ️  No new distributors to insert")

# ---------------------------------------------------
# 8️⃣ MAP SURROGATE KEY BACK TO DATAFRAME
# ---------------------------------------------------
print("\n🔗 Mapping surrogate keys back to dataframe...")

df_valid["DIST_KEY"] = df_valid.apply(
    lambda x: dist_cache.get(
        (x["DISTRIBUTOR_NAME_CLEAN"], x["DISTRIBUTOR_CITY_CLEAN"], x["DISTRIBUTOR_STATE_CLEAN"]),
        None
    ),
    axis=1
)

# Check for unmapped records
unmapped_count = df_valid["DIST_KEY"].isna().sum()
if unmapped_count > 0:
    print(f"   ⚠️  {unmapped_count} records could not be mapped to dist_key")
else:
    print(f"   ✅ All {len(df_valid)} records mapped successfully")

# ===================================================
# FINAL STATISTICS
# ===================================================
print("\n" + "="*60)
print("LOAD STATISTICS")
print("="*60)

# Distributor statistics
cur.execute("""
    SELECT 
        COUNT(*) as total_distributors,
        SUM(CASE WHEN dist_active_flag = 'Y' THEN 1 ELSE 0 END) as active_count,
        SUM(CASE WHEN dist_active_flag = 'N' THEN 1 ELSE 0 END) as inactive_count
    FROM dim_distributor_dw
""")
stats = cur.fetchone()
print(f"📊 Total distributors in DW: {stats[0]}")
print(f"   Active: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
print(f"   Inactive: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")

# Distributor by type
cur.execute("""
    SELECT dist_type, COUNT(*) as cnt
    FROM dim_distributor_dw
    GROUP BY dist_type
    ORDER BY cnt DESC
""")
print(f"\n📊 Distributors by Type:")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

# Distributor by state (top 10)
cur.execute("""
    SELECT dist_state, COUNT(*) as cnt
    FROM dim_distributor_dw
    GROUP BY dist_state
    ORDER BY cnt DESC
    FETCH FIRST 10 ROWS ONLY
""")
print(f"\n📊 Top 10 States by Distributor Count:")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

cur.close()
conn.close()

print("\n" + "="*60)
print("🎉 DISTRIBUTOR DIMENSION LOAD COMPLETED SUCCESSFULLY")
print("="*60)
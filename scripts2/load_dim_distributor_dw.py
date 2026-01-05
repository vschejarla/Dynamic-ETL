import os
import pandas as pd
import oracledb

INCOMING_DIR = "/opt/airflow/data_extracts/incoming"

# -----------------------------
# Read latest file
# -----------------------------
files = sorted(
    [f for f in os.listdir(INCOMING_DIR) if f.endswith(".csv")],
    reverse=True
)

if not files:
    raise FileNotFoundError("❌ No incoming files found")

latest_file = os.path.join(INCOMING_DIR, files[0])
print("📄 Processing:", latest_file)

df = pd.read_csv(latest_file, delimiter="|")

# -----------------------------
# Normalization helpers
# -----------------------------
def norm(x):
    return str(x).strip().upper()

def flag_norm(x):
    return "Y" if str(x).upper() in ["Y", "YES", "ACTIVE", "1"] else "N"

# -----------------------------
# Columns
# -----------------------------
dist_cols = [
    "DISTRIBUTOR_NAME",
    "DISTRIBUTOR_CITY",
    "DISTRIBUTOR_STATE",
    "DISTRIBUTOR_TYPE",
    "ONBOARDING_DATE",
    "ACTIVE_FLAG"
]

# Normalize text columns only
for c in [
    "DISTRIBUTOR_NAME",
    "DISTRIBUTOR_CITY",
    "DISTRIBUTOR_STATE",
    "DISTRIBUTOR_TYPE"
]:
    df[c] = df[c].apply(norm)

df["ACTIVE_FLAG"] = df["ACTIVE_FLAG"].apply(flag_norm)

# Convert date
df["ONBOARDING_DATE"] = pd.to_datetime(
    df["ONBOARDING_DATE"],
    errors="coerce"
)

dist_df = df[dist_cols].drop_duplicates()

# -----------------------------
# Oracle connection
# -----------------------------
conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

# -----------------------------
# Cache existing distributors
# -----------------------------
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

cur.execute("SELECT NVL(MAX(dist_key),0) FROM dim_distributor_dw")
next_key = cur.fetchone()[0] + 1

new_rows = []

# -----------------------------
# Detect & insert new rows
# -----------------------------
for _, r in dist_df.iterrows():
    key = (
        r["DISTRIBUTOR_NAME"],
        r["DISTRIBUTOR_CITY"],
        r["DISTRIBUTOR_STATE"]
    )

    if key not in dist_cache:
        dist_cache[key] = next_key
        new_rows.append((
            next_key,
            r["DISTRIBUTOR_NAME"],
            r["DISTRIBUTOR_TYPE"],
            r["DISTRIBUTOR_CITY"],
            r["DISTRIBUTOR_STATE"],
            r["ONBOARDING_DATE"],
            r["ACTIVE_FLAG"]
        ))
        next_key += 1

if new_rows:
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
else:
    print("ℹ️ No new distributors")

# -----------------------------
# Map surrogate key back
# -----------------------------
df["DIST_KEY"] = df.apply(
    lambda x: dist_cache[
        (x["DISTRIBUTOR_NAME"], x["DISTRIBUTOR_CITY"], x["DISTRIBUTOR_STATE"])
    ],
    axis=1
)

cur.close()
conn.close()

print("🎯 Distributor dimension load completed successfully")
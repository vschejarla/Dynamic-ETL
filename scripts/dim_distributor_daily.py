import random
import oracledb
from datetime import date, timedelta

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

cur.execute("SELECT NVL(MAX(distributor_id), 0) FROM dim_distributor")
start_id = cur.fetchone()[0]

# ========================================
# INDIAN DISTRIBUTION GEOGRAPHY
# ========================================
INDIAN_DISTRIBUTION_HUBS = {
    "Maharashtra":    ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad"],
    "Karnataka":      ["Bangalore", "Mysore", "Hubli", "Mangalore"],
    "Tamil Nadu":     ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli"],
    "Telangana":      ["Hyderabad", "Warangal"],
    "Andhra Pradesh": ["Visakhapatnam", "Vijayawada", "Guntur"],
    "Gujarat":        ["Ahmedabad", "Surat", "Vadodara", "Rajkot"],
    "West Bengal":    ["Kolkata", "Howrah", "Siliguri"],
    "Delhi":          ["New Delhi", "Delhi"],
    "Uttar Pradesh":  ["Lucknow", "Kanpur", "Noida", "Ghaziabad", "Agra"],
    "Madhya Pradesh": ["Indore", "Bhopal", "Jabalpur"],
    "Rajasthan":      ["Jaipur", "Jodhpur", "Udaipur"],
    "Punjab":         ["Ludhiana", "Amritsar", "Jalandhar"],
    "Haryana":        ["Gurugram", "Faridabad", "Panipat"],
    "Kerala":         ["Kochi", "Thiruvananthapuram", "Kozhikode"],
    "Bihar":          ["Patna", "Gaya"],
    "Jharkhand":      ["Ranchi", "Jamshedpur"],
    "Odisha":         ["Bhubaneswar", "Cuttack"],
}

DISTRIBUTOR_NAME_PATTERNS = {
    "National": [
        "{name} Distribution Services",
        "{name} Logistics Pvt Ltd",
        "{name} Supply Chain Solutions",
        "{name} National Distributors",
        "{name} Trading Company",
        "All India {name} Distributors",
    ],
    "Regional": [
        "{region} {name} Traders",
        "{region} {name} Distribution",
        "{name} {region} Supplies",
        "{region} {name} Agencies",
        "{name} Distribution - {region}",
    ],
    "Local": [
        "{city} {name} Distributors",
        "{name} Traders - {city}",
        "{name} & Sons",
        "{name} Agencies",
        "New {name} Enterprises",
        "Sri {name} Trading Co",
    ],
}

BUSINESS_NAMES = [
    "Sharma","Kumar","Gupta","Singh","Patel","Shah","Mehta","Agarwal",
    "Jain","Reddy","Verma","Krishna","Srinivasan","Malhotra","Chopra",
    "Kapoor","Nair","Rao","Desai","Trivedi","Bhatia","Goyal","Bansal",
]
COMPANY_PREFIXES = [
    "Rajesh","Suresh","Ramesh","Mahesh","Ganesh","Dinesh",
    "Venkat","Prakash","Rakesh","Mohan","Ravi","Vijay",
    "Sanjay","Ajay","Anil","Ashok","Deepak","Manoj",
]

REGIONS = {
    "North":   ["Punjab","Haryana","Delhi","Uttar Pradesh","Rajasthan"],
    "South":   ["Tamil Nadu","Karnataka","Kerala","Telangana","Andhra Pradesh"],
    "West":    ["Maharashtra","Gujarat"],
    "East":    ["West Bengal","Odisha","Bihar","Jharkhand"],
    "Central": ["Madhya Pradesh"],
}

# ========================================
# HELPER FUNCTIONS
# ========================================
def get_region_for_state(state):
    for region, states in REGIONS.items():
        if state in states:
            return region
    return "India"

def get_distributor_type():
    return random.choices(
        ["National", "Regional", "Local"],
        weights=[15, 30, 55]
    )[0]

def get_location(distributor_type):
    if distributor_type == "National":
        metro_states = ["Maharashtra","Karnataka","Tamil Nadu","Delhi","Gujarat","Telangana"]
        state = random.choice(metro_states)
        metro_cities = {
            "Maharashtra": ["Mumbai","Pune"],
            "Karnataka":   ["Bangalore"],
            "Tamil Nadu":  ["Chennai"],
            "Telangana":   ["Hyderabad"],
            "Delhi":       ["New Delhi"],
            "Gujarat":     ["Ahmedabad","Surat"],
        }
        city = random.choice(metro_cities[state])
    else:
        state = random.choice(list(INDIAN_DISTRIBUTION_HUBS.keys()))
        city  = random.choice(INDIAN_DISTRIBUTION_HUBS[state])
    return city, state

def generate_name(distributor_type, city, state):
    pattern = random.choice(DISTRIBUTOR_NAME_PATTERNS[distributor_type])
    name    = random.choice(COMPANY_PREFIXES) + " " + random.choice(BUSINESS_NAMES)
    if distributor_type == "National":
        return pattern.format(name=name)
    elif distributor_type == "Regional":
        return pattern.format(name=name, region=get_region_for_state(state))
    else:
        return pattern.format(name=name, city=city)

def get_onboarding_date(distributor_type):
    ranges = {
        "National": (date(2015, 1, 1), date(2020, 12, 31)),
        "Regional": (date(2018, 1, 1), date(2023, 12, 31)),
        "Local":    (date(2016, 1, 1), date.today()),
    }
    start, end = ranges[distributor_type]
    return start + timedelta(days=random.randint(0, (end - start).days))

def get_active_flag(distributor_type, onboarding_date):
    years_active = (date.today() - onboarding_date).days / 365.25
    thresholds = {
        "National": 0.95,
        "Regional": 0.90,
    }
    if distributor_type in thresholds:
        return "Y" if random.random() < thresholds[distributor_type] else "N"
    # Local: adjust by tenure
    if years_active < 1:
        p = 0.95
    elif years_active > 8:
        p = 0.70
    else:
        p = 0.80
    return "Y" if random.random() < p else "N"

# ========================================
# GENERATE & INSERT
# ========================================
INSERT_SQL = """
INSERT INTO dim_distributor (
    distributor_id, distributor_name, distributor_type,
    city, state, onboarding_date, active_flag
) VALUES (:1,:2,:3,:4,:5,:6,:7)
"""

DAILY_COUNT = 10
data = []

for i in range(1, DAILY_COUNT + 1):
    dist_id   = start_id + i
    dist_type = get_distributor_type()
    city, state = get_location(dist_type)
    data.append((
        dist_id,
        generate_name(dist_type, city, state)[:50],   # VARCHAR2(50) guard
        dist_type[:30],
        city[:30],
        state[:30],
        get_onboarding_date(dist_type),
        get_active_flag(dist_type, get_onboarding_date(dist_type)),
    ))

# FIX: wrap insert in try/except with connection cleanup in finally
try:
    cur.executemany(INSERT_SQL, data)
    conn.commit()
except Exception as e:
    conn.rollback()
    raise RuntimeError(f"Insert failed: {e}") from e
finally:
    cur.close()
    conn.close()

# ========================================
# SUMMARY
# ========================================
type_counts  = {}
active_count = 0
for d in data:
    type_counts[d[2]] = type_counts.get(d[2], 0) + 1
    active_count += 1 if d[6] == "Y" else 0

print(
    f"Inserted: {len(data)} distributors | "
    f"IDs: {start_id + 1}–{start_id + DAILY_COUNT} | "
    f"Active: {active_count} | Inactive: {len(data) - active_count} | "
    + " | ".join(f"{t}: {c}" for t, c in type_counts.items())
)
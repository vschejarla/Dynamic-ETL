from faker import Faker
import oracledb
import random
from datetime import date, timedelta

fake = Faker('en_IN')  # Indian locale

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

# Get the next distributor_id
cur.execute("SELECT NVL(MAX(distributor_id),0) FROM dim_distributor")
start_id = cur.fetchone()[0]

print(f"🔢 Starting distributor_id: {start_id + 1}")

# -------------------------------
# REAL INDIAN DISTRIBUTION BUSINESS LOGIC
# -------------------------------

# Indian States with Major Distribution Hubs
INDIAN_DISTRIBUTION_HUBS = {
    # Metro Cities - National Hubs
    "Maharashtra": ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad"],
    "Karnataka": ["Bangalore", "Mysore", "Hubli", "Mangalore"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli"],
    "Telangana": ["Hyderabad", "Warangal"],
    "Andhra Pradesh": ["Visakhapatnam", "Vijayawada", "Guntur"],
    "Gujarat": ["Ahmedabad", "Surat", "Vadodara", "Rajkot"],
    "West Bengal": ["Kolkata", "Howrah", "Siliguri"],
    "Delhi": ["New Delhi", "Delhi"],
    "Uttar Pradesh": ["Lucknow", "Kanpur", "Noida", "Ghaziabad", "Agra"],
    "Madhya Pradesh": ["Indore", "Bhopal", "Jabalpur"],
    "Rajasthan": ["Jaipur", "Jodhpur", "Udaipur"],
    "Punjab": ["Ludhiana", "Amritsar", "Jalandhar"],
    "Haryana": ["Gurugram", "Faridabad", "Panipat"],
    "Kerala": ["Kochi", "Thiruvananthapuram", "Kozhikode"],
    "Bihar": ["Patna", "Gaya"],
    "Jharkhand": ["Ranchi", "Jamshedpur"],
    "Odisha": ["Bhubaneswar", "Cuttack"]
}

# Distributor Name Patterns by Type
DISTRIBUTOR_NAME_PATTERNS = {
    "National": [
        "{name} Distribution Services",
        "{name} Logistics Pvt Ltd",
        "{name} Supply Chain Solutions",
        "{name} National Distributors",
        "{name} Trading Company",
        "All India {name} Distributors"
    ],
    "Regional": [
        "{region} {name} Traders",
        "{region} {name} Distribution",
        "{name} {region} Supplies",
        "{region} {name} Agencies",
        "{name} Distribution - {region}"
    ],
    "Local": [
        "{city} {name} Distributors",
        "{name} Traders - {city}",
        "{name} & Sons",
        "{name} Agencies",
        "New {name} Enterprises",
        "Sri {name} Trading Co"
    ]
}

# Common Indian Business Names/Surnames
BUSINESS_NAMES = [
    "Sharma", "Kumar", "Gupta", "Singh", "Patel", "Shah", "Mehta", "Agarwal",
    "Jain", "Reddy", "Verma", "Krishna", "Srinivasan", "Malhotra", "Chopra",
    "Kapoor", "Nair", "Rao", "Desai", "Trivedi", "Bhatia", "Goyal", "Bansal"
]

# Company Name Prefixes
COMPANY_PREFIXES = [
    "Rajesh", "Suresh", "Ramesh", "Mahesh", "Ganesh", "Dinesh",
    "Venkat", "Prakash", "Rakesh", "Mohan", "Ravi", "Vijay",
    "Sanjay", "Ajay", "Anil", "Ashok", "Deepak", "Manoj"
]

# Regional Identifiers
REGIONS = {
    "North": ["Punjab", "Haryana", "Delhi", "Uttar Pradesh", "Rajasthan"],
    "South": ["Tamil Nadu", "Karnataka", "Kerala", "Telangana", "Andhra Pradesh"],
    "West": ["Maharashtra", "Gujarat"],
    "East": ["West Bengal", "Odisha", "Bihar", "Jharkhand"],
    "Central": ["Madhya Pradesh"]
}

def get_region_for_state(state):
    """Get region name for a state"""
    for region, states in REGIONS.items():
        if state in states:
            return region
    return "India"

def get_distributor_type_with_weights():
    """Get distributor type with realistic distribution"""
    types = ["National", "Regional", "Local"]
    weights = [15, 30, 55]  # Local distributors are most common
    return random.choices(types, weights=weights)[0]

def get_location_based_on_type(distributor_type):
    """Get appropriate location based on distributor type"""
    
    if distributor_type == "National":
        # National distributors based in major metros
        metro_states = ["Maharashtra", "Karnataka", "Tamil Nadu", "Delhi", "Gujarat", "Telangana"]
        state = random.choice(metro_states)
        # Prefer tier-1 cities
        if state == "Maharashtra":
            city = random.choice(["Mumbai", "Pune"])
        elif state == "Karnataka":
            city = "Bangalore"
        elif state == "Tamil Nadu":
            city = "Chennai"
        elif state == "Telangana":
            city = "Hyderabad"
        elif state == "Delhi":
            city = "New Delhi"
        elif state == "Gujarat":
            city = random.choice(["Ahmedabad", "Surat"])
        else:
            city = random.choice(INDIAN_DISTRIBUTION_HUBS[state])
    
    elif distributor_type == "Regional":
        # Regional distributors in tier-1 and tier-2 cities
        state = random.choice(list(INDIAN_DISTRIBUTION_HUBS.keys()))
        city = random.choice(INDIAN_DISTRIBUTION_HUBS[state])
    
    else:  # Local
        # Local distributors spread across all cities
        state = random.choice(list(INDIAN_DISTRIBUTION_HUBS.keys()))
        city = random.choice(INDIAN_DISTRIBUTION_HUBS[state])
    
    return city, state

def generate_distributor_name(distributor_type, city, state):
    """Generate realistic distributor name based on type"""
    
    pattern = random.choice(DISTRIBUTOR_NAME_PATTERNS[distributor_type])
    
    if distributor_type == "National":
        name = random.choice(COMPANY_PREFIXES) + " " + random.choice(BUSINESS_NAMES)
        distributor_name = pattern.format(name=name)
    
    elif distributor_type == "Regional":
        region = get_region_for_state(state)
        name = random.choice(COMPANY_PREFIXES) + " " + random.choice(BUSINESS_NAMES)
        distributor_name = pattern.format(name=name, region=region)
    
    else:  # Local
        name = random.choice(COMPANY_PREFIXES) + " " + random.choice(BUSINESS_NAMES)
        distributor_name = pattern.format(name=name, city=city)
    
    return distributor_name

def get_onboarding_date(distributor_type):
    """Generate realistic onboarding date based on distributor type"""
    
    # National distributors - older, established (2015-2020)
    # Regional distributors - mid-range (2018-2023)
    # Local distributors - newer and older mix (2015-2024)
    
    if distributor_type == "National":
        start_date = date(2015, 1, 1)
        end_date = date(2020, 12, 31)
    elif distributor_type == "Regional":
        start_date = date(2018, 1, 1)
        end_date = date(2023, 12, 31)
    else:  # Local
        start_date = date(2016, 1, 1)
        end_date = date.today()
    
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return start_date + timedelta(days=random_days)

def get_active_flag(distributor_type, onboarding_date):
    """Determine active status based on business logic"""
    
    # Calculate years since onboarding
    years_active = (date.today() - onboarding_date).days / 365.25
    
    # Business logic for active status
    if distributor_type == "National":
        # National distributors - 95% active (rarely deactivated)
        return 'Y' if random.random() < 0.95 else 'N'
    
    elif distributor_type == "Regional":
        # Regional distributors - 90% active
        return 'Y' if random.random() < 0.90 else 'N'
    
    else:  # Local
        # Local distributors - 80% active (higher churn)
        # Recently onboarded (<1 year) - 95% active
        if years_active < 1:
            return 'Y' if random.random() < 0.95 else 'N'
        # Very old (>8 years) - 70% active
        elif years_active > 8:
            return 'Y' if random.random() < 0.70 else 'N'
        else:
            return 'Y' if random.random() < 0.80 else 'N'

def generate_distributor_data(distributor_id):
    """Generate complete distributor data with business logic"""
    
    # Determine distributor type
    distributor_type = get_distributor_type_with_weights()
    
    # Get appropriate location
    city, state = get_location_based_on_type(distributor_type)
    
    # Generate name
    distributor_name = generate_distributor_name(distributor_type, city, state)
    
    # Get onboarding date
    onboarding_date = get_onboarding_date(distributor_type)
    
    # Determine active status
    active_flag = get_active_flag(distributor_type, onboarding_date)
    
    return (
        distributor_id,
        distributor_name[:50],  # VARCHAR2(50) constraint
        distributor_type[:30],
        city[:30],
        state[:30],
        onboarding_date,
        active_flag
    )

# -------------------------------
# GENERATE DISTRIBUTOR DATA
# -------------------------------

sql = """
INSERT INTO dim_distributor (
    distributor_id, distributor_name, distributor_type,
    city, state, onboarding_date, active_flag
) VALUES (
    :1,:2,:3,:4,:5,:6,:7
)
"""

data = []
distributor_count = 10  # Number of distributors to generate daily

print(f"\n🚚 Generating {distributor_count} new distributors...\n")

for i in range(1, distributor_count + 1):
    distributor_id = start_id + i
    distributor_data = generate_distributor_data(distributor_id)
    data.append(distributor_data)
    
    # Print details
    status_emoji = "✅" if distributor_data[6] == 'Y' else "❌"
    print(f"  → Distributor {distributor_id}:")
    print(f"     Name: {distributor_data[1]}")
    print(f"     Type: {distributor_data[2]}")
    print(f"     Location: {distributor_data[3]}, {distributor_data[4]}")
    print(f"     Onboarded: {distributor_data[5].strftime('%d-%b-%Y')}")
    print(f"     Status: {status_emoji} {'Active' if distributor_data[6] == 'Y' else 'Inactive'}")
    print()

# Insert data
cur.executemany(sql, data)
conn.commit()

print(f"✅ DIM_DISTRIBUTOR daily increment completed")
print(f"📊 Inserted {len(data)} new distributors")
print(f"🔢 Distributor ID range: {start_id + 1} to {start_id + distributor_count}")

# Statistics
type_stats = {}
active_count = 0
inactive_count = 0

for d in data:
    dist_type = d[2]
    type_stats[dist_type] = type_stats.get(dist_type, 0) + 1
    if d[6] == 'Y':
        active_count += 1
    else:
        inactive_count += 1

print(f"\n📈 Distributor Statistics:")
print(f"   National: {type_stats.get('National', 0)}")
print(f"   Regional: {type_stats.get('Regional', 0)}")
print(f"   Local: {type_stats.get('Local', 0)}")
print(f"   Active: {active_count}")
print(f"   Inactive: {inactive_count}")

cur.close()
conn.close()
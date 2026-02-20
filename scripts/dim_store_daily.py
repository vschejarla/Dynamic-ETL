from faker import Faker
import oracledb
import random

fake = Faker('en_IN')  # Indian locale

<<<<<<< HEAD
conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()
=======
print("🏪 DIM_STORE_MASTER Daily Incremental Load Started")

conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()
print("✅ Connected to Oracle")
>>>>>>> etl-update

# Get the next store_id
cur.execute("SELECT NVL(MAX(store_id),0) FROM dim_store_master")
start_id = cur.fetchone()[0]

print(f"🔢 Starting store_id: {start_id + 1}")

# -------------------------------
# REAL INDIAN RETAIL STORE DATA
# -------------------------------

# Indian Cities with their States
INDIAN_CITIES = {
    "Maharashtra": ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad", "Thane", "Navi Mumbai"],
    "Karnataka": ["Bangalore", "Mysore", "Mangalore", "Hubli", "Belgaum"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli", "Salem", "Tiruppur"],
    "Telangana": ["Hyderabad", "Warangal", "Nizamabad", "Khammam"],
    "Andhra Pradesh": ["Visakhapatnam", "Vijayawada", "Guntur", "Tirupati", "Nellore"],
    "Gujarat": ["Ahmedabad", "Surat", "Vadodara", "Rajkot", "Bhavnagar"],
    "Rajasthan": ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"],
    "West Bengal": ["Kolkata", "Howrah", "Durgapur", "Siliguri", "Asansol"],
    "Uttar Pradesh": ["Lucknow", "Kanpur", "Ghaziabad", "Agra", "Varanasi", "Meerut", "Noida"],
    "Delhi": ["New Delhi", "Delhi"],
    "Madhya Pradesh": ["Indore", "Bhopal", "Jabalpur", "Gwalior", "Ujjain"],
    "Punjab": ["Ludhiana", "Amritsar", "Jalandhar", "Patiala"],
    "Haryana": ["Gurugram", "Faridabad", "Panipat", "Ambala"],
    "Kerala": ["Kochi", "Thiruvananthapuram", "Kozhikode", "Thrissur"],
    "Odisha": ["Bhubaneswar", "Cuttack", "Rourkela", "Puri"]
}

# Real Indian Retail Chains
RETAIL_CHAINS = {
    "Hypermarket": ["Reliance Smart", "DMart", "More Megastore", "Big Bazaar", "Star Bazaar", "HyperCity"],
    "Supermarket": ["Spencer's Retail", "Heritage Fresh", "Nilgiris", "Food Bazaar", "Aditya Birla More"],
    "Convenience Store": ["24Seven", "Twenty Four Seven", "In & Out", "EasyDay", "RoundTheClock"],
    "Kirana Store": [None],  # Independent stores
    "Wholesale": ["Metro Cash & Carry", "Walmart Best Price", "Reliance Wholesale"]
}

# Store Name Patterns for Independent Stores
INDEPENDENT_STORE_PATTERNS = [
    "{area} Supermarket",
    "{area} Groceries",
    "{area} General Store",
    "{area} Daily Needs",
    "New {area} Store",
    "{owner}'s Supermarket",
    "{owner}'s General Store",
    "Sri {owner} Stores",
    "Shri {owner} Retail",
    "{area} Provision Store",
    "{area} Bazaar"
]

# Indian Area/Locality Names
AREA_NAMES = [
    "MG Road", "Main Street", "Station Road", "Market Road", "Gandhi Nagar",
    "Park Street", "Commercial Street", "Brigade Road", "Residency Road",
    "Anna Salai", "Mount Road", "Nehru Place", "Connaught Place",
    "Banjara Hills", "Jubilee Hills", "Koramangala", "Indiranagar",
    "Whitefield", "Electronic City", "Salt Lake", "Sector 17"
]

# Indian Names for Store Owners
OWNER_NAMES = [
    "Sharma", "Kumar", "Patel", "Singh", "Reddy", "Krishnan", "Gupta",
    "Mehta", "Shah", "Jain", "Agarwal", "Verma", "Rao", "Nair"
]

# Street Patterns
STREET_PATTERNS = [
    "Shop No {num}, {landmark}",
    "{num} Main Road",
    "{num} Cross Street",
    "Plot No {num}, {landmark}",
    "{num} {road_name}",
    "Door No {num}, {landmark}"
]

LANDMARKS = [
    "Opp. Metro Station", "Near Bus Stand", "Market Complex", "Shopping Plaza",
    "Commercial Hub", "City Center", "Town Hall Road", "Railway Station Road",
    "Main Bazaar", "New Market", "Old Town", "Lake View", "Park Avenue"
]

ROAD_NAMES = [
    "Main Road", "Cross Road", "Station Road", "Church Road", "Temple Street",
    "Market Road", "Gandhi Road", "Nehru Street", "Patel Nagar"
]

# Class of Trade Distribution
CLASS_OF_TRADE = {
    "Modern Trade - Hypermarket": 15,      # Large format 8000+ sqft
    "Modern Trade - Supermarket": 25,      # Medium format 2000-8000 sqft
    "General Trade - Kirana": 40,          # Small independent stores
    "Convenience Store": 10,                # 24x7 convenience
    "Cash & Carry - Wholesale": 10         # Wholesale outlets
}

def get_random_location():
    """Get random Indian city and state"""
    state = random.choice(list(INDIAN_CITIES.keys()))
    city = random.choice(INDIAN_CITIES[state])
    return city, state

def get_indian_pincode(city):
    """Generate realistic Indian PIN codes based on city"""
    pincode_ranges = {
        "Mumbai": (400001, 400104), "Pune": (411001, 411060),
        "Bangalore": (560001, 560100), "Chennai": (600001, 600100),
        "Hyderabad": (500001, 500100), "Ahmedabad": (380001, 380060),
        "Kolkata": (700001, 700150), "Delhi": (110001, 110096),
        "Jaipur": (302001, 302040), "Lucknow": (226001, 226030)
    }
    
<<<<<<< HEAD
    # Get range for specific cities, default for others
=======
>>>>>>> etl-update
    if city in pincode_ranges:
        start, end = pincode_ranges[city]
        return str(random.randint(start, end))
    else:
        # Generate based on state code
        state_codes = {
            "Maharashtra": "4", "Karnataka": "5", "Tamil Nadu": "6",
            "Telangana": "5", "Gujarat": "3", "West Bengal": "7",
            "Uttar Pradesh": "2", "Delhi": "1", "Rajasthan": "3"
        }
        code = "1"
        for state, prefix in state_codes.items():
            if city in INDIAN_CITIES.get(state, []):
                code = prefix
                break
        return f"{code}{random.randint(10000, 99999)}"

def generate_store_address():
    """Generate realistic Indian store address"""
    num = random.randint(1, 999)
    landmark = random.choice(LANDMARKS)
    road_name = random.choice(ROAD_NAMES)
    
    pattern = random.choice(STREET_PATTERNS)
    address_line_1 = pattern.format(num=num, landmark=landmark, road_name=road_name)
    
<<<<<<< HEAD
    # Address Line 2 (optional additional details)
=======
>>>>>>> etl-update
    address_line_2_options = [
        f"{random.choice(AREA_NAMES)}",
        f"Near {landmark}",
        f"{random.choice(['Ground Floor', 'First Floor', 'Shop Complex'])}",
        None
    ]
    address_line_2 = random.choice(address_line_2_options)
    
    return address_line_1, address_line_2

def get_class_of_trade():
    """Get weighted random class of trade"""
    classes = list(CLASS_OF_TRADE.keys())
    weights = list(CLASS_OF_TRADE.values())
    return random.choices(classes, weights=weights)[0]

def generate_store_data(store_id):
    """Generate complete store data with business logic"""
    
<<<<<<< HEAD
    # Determine class of trade first (drives other attributes)
    class_of_trade = get_class_of_trade()
    
    # Determine if chain or independent
=======
    class_of_trade = get_class_of_trade()
    
>>>>>>> etl-update
    is_chain = 'N'
    chain_name = None
    store_name = None
    
    if "Hypermarket" in class_of_trade:
        is_chain = 'Y'
        chain_name = random.choice(RETAIL_CHAINS["Hypermarket"])
        store_name = chain_name
        
    elif "Supermarket" in class_of_trade:
<<<<<<< HEAD
        # 70% chain, 30% independent
=======
>>>>>>> etl-update
        if random.random() < 0.7:
            is_chain = 'Y'
            chain_name = random.choice(RETAIL_CHAINS["Supermarket"])
            store_name = chain_name
        else:
            is_chain = 'N'
            area = random.choice(AREA_NAMES)
            pattern = random.choice(INDEPENDENT_STORE_PATTERNS)
            store_name = pattern.format(area=area, owner=random.choice(OWNER_NAMES))
            
    elif "Convenience" in class_of_trade:
        is_chain = 'Y'
        chain_name = random.choice(RETAIL_CHAINS["Convenience Store"])
        store_name = chain_name
        
    elif "Wholesale" in class_of_trade:
        is_chain = 'Y'
        chain_name = random.choice(RETAIL_CHAINS["Wholesale"])
        store_name = chain_name
        
    else:  # Kirana - always independent
        is_chain = 'N'
        area = random.choice(AREA_NAMES)
        owner = random.choice(OWNER_NAMES)
        pattern = random.choice(INDEPENDENT_STORE_PATTERNS)
        store_name = pattern.format(area=area, owner=owner)
    
<<<<<<< HEAD
    # Get location
    city, state = get_random_location()
    zip_code = get_indian_pincode(city)
    
    # Get address
    address_line_1, address_line_2 = generate_store_address()
    
    # Add city/area suffix to chain stores for uniqueness
=======
    city, state = get_random_location()
    zip_code = get_indian_pincode(city)
    address_line_1, address_line_2 = generate_store_address()
    
>>>>>>> etl-update
    if is_chain == 'Y':
        store_name = f"{store_name} - {city}"
    
    return (
        store_id,
        store_name[:50],
        address_line_1[:150],
        address_line_2[:100] if address_line_2 else None,
        city[:25],
        zip_code[:10],
        state[:50],
        class_of_trade[:50],
        is_chain,
        chain_name[:50] if chain_name else None
    )

# -------------------------------
<<<<<<< HEAD
# GENERATE STORE DATA
=======
# GENERATE AND INSERT STORE DATA
>>>>>>> etl-update
# -------------------------------

sql = """
INSERT INTO dim_store_master (
    store_id, store_name, store_address_lane_1,
    store_address_lane_2, store_city, store_zip,
    store_state, store_class_of_trade, is_chain, chain_name
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
)
"""

data = []
store_count = 10  # Number of stores to generate daily

print(f"\n🏪 Generating {store_count} new stores...\n")

for i in range(1, store_count + 1):
    store_id = start_id + i
    store_data = generate_store_data(store_id)
    data.append(store_data)
    
<<<<<<< HEAD
    # Print details
    print(f"  → Store {store_id}:")
    print(f"     Name: {store_data[1]}")
    print(f"     Location: {store_data[4]}, {store_data[5]} - {store_data[6]}")
=======
    print(f"  → Store {store_id}:")
    print(f"     Name: {store_data[1]}")
    print(f"     Location: {store_data[4]}, {store_data[6]}")
>>>>>>> etl-update
    print(f"     Type: {store_data[7]}")
    print(f"     Chain: {'Yes' if store_data[8] == 'Y' else 'No'} {('(' + store_data[9] + ')') if store_data[9] else ''}")
    print()

# Insert data
cur.executemany(sql, data)
conn.commit()

print(f"✅ DIM_STORE_MASTER daily increment completed")
print(f"📊 Inserted {len(data)} new stores")
print(f"🔢 Store ID range: {start_id + 1} to {start_id + store_count}")

# Statistics
chain_count = sum(1 for d in data if d[8] == 'Y')
independent_count = len(data) - chain_count

print(f"\n📈 Store Statistics:")
print(f"   Chain Stores: {chain_count}")
print(f"   Independent Stores: {independent_count}")

<<<<<<< HEAD
cur.close()
conn.close()
=======
# Class of Trade breakdown
cot_counts = {}
for d in data:
    cot = d[7]
    cot_counts[cot] = cot_counts.get(cot, 0) + 1

print(f"\n📊 Class of Trade Distribution:")
for cot, count in sorted(cot_counts.items()):
    print(f"   {cot}: {count}")

cur.close()
conn.close()
print("\n🎉 Daily load completed successfully!")
>>>>>>> etl-update

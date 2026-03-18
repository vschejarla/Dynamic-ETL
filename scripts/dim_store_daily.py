import oracledb
import random

# ========================================
# INDIAN RETAIL STORE DATA
# ========================================
INDIAN_CITIES = {
    "Maharashtra":    ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad", "Thane", "Navi Mumbai"],
    "Karnataka":      ["Bangalore", "Mysore", "Mangalore", "Hubli", "Belgaum"],
    "Tamil Nadu":     ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli", "Salem", "Tiruppur"],
    "Telangana":      ["Hyderabad", "Warangal", "Nizamabad", "Khammam"],
    "Andhra Pradesh": ["Visakhapatnam", "Vijayawada", "Guntur", "Tirupati", "Nellore"],
    "Gujarat":        ["Ahmedabad", "Surat", "Vadodara", "Rajkot", "Bhavnagar"],
    "Rajasthan":      ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"],
    "West Bengal":    ["Kolkata", "Howrah", "Durgapur", "Siliguri", "Asansol"],
    "Uttar Pradesh":  ["Lucknow", "Kanpur", "Ghaziabad", "Agra", "Varanasi", "Meerut", "Noida"],
    "Delhi":          ["New Delhi", "Delhi"],
    "Madhya Pradesh": ["Indore", "Bhopal", "Jabalpur", "Gwalior", "Ujjain"],
    "Punjab":         ["Ludhiana", "Amritsar", "Jalandhar", "Patiala"],
    "Haryana":        ["Gurugram", "Faridabad", "Panipat", "Ambala"],
    "Kerala":         ["Kochi", "Thiruvananthapuram", "Kozhikode", "Thrissur"],
    "Odisha":         ["Bhubaneswar", "Cuttack", "Rourkela", "Puri"],
}

RETAIL_CHAINS = {
    "Hypermarket":    ["Reliance Smart", "DMart", "More Megastore", "Big Bazaar", "Star Bazaar", "HyperCity"],
    "Supermarket":    ["Spencer's Retail", "Heritage Fresh", "Nilgiris", "Food Bazaar", "Aditya Birla More"],
    "Convenience":    ["24Seven", "Twenty Four Seven", "In & Out", "EasyDay", "RoundTheClock"],
    "Wholesale":      ["Metro Cash & Carry", "Walmart Best Price", "Reliance Wholesale"],
}

INDEPENDENT_STORE_PATTERNS = [
    "{area} Supermarket", "{area} Groceries", "{area} General Store",
    "{area} Daily Needs",  "New {area} Store",  "{owner}'s Supermarket",
    "{owner}'s General Store", "Sri {owner} Stores", "Shri {owner} Retail",
    "{area} Provision Store", "{area} Bazaar",
]

AREA_NAMES = [
    "MG Road", "Main Street", "Station Road", "Market Road", "Gandhi Nagar",
    "Park Street", "Commercial Street", "Brigade Road", "Residency Road",
    "Anna Salai", "Mount Road", "Nehru Place", "Connaught Place",
    "Banjara Hills", "Jubilee Hills", "Koramangala", "Indiranagar",
    "Whitefield", "Electronic City", "Salt Lake", "Sector 17",
]

OWNER_NAMES = [
    "Sharma", "Kumar", "Patel", "Singh", "Reddy", "Krishnan", "Gupta",
    "Mehta", "Shah", "Jain", "Agarwal", "Verma", "Rao", "Nair",
]

STREET_PATTERNS = [
    "Shop No {num}, {landmark}",
    "{num} Main Road",
    "{num} Cross Street",
    "Plot No {num}, {landmark}",
    "{num} {road_name}",
    "Door No {num}, {landmark}",
]

LANDMARKS = [
    "Opp. Metro Station", "Near Bus Stand", "Market Complex", "Shopping Plaza",
    "Commercial Hub", "City Center", "Town Hall Road", "Railway Station Road",
    "Main Bazaar", "New Market", "Old Town", "Lake View", "Park Avenue",
]

ROAD_NAMES = [
    "Main Road", "Cross Road", "Station Road", "Church Road", "Temple Street",
    "Market Road", "Gandhi Road", "Nehru Street", "Patel Nagar",
]

CLASS_OF_TRADE = {
    "Modern Trade - Hypermarket":  15,
    "Modern Trade - Supermarket":  25,
    "General Trade - Kirana":      40,
    "Convenience Store":           10,
    "Cash & Carry - Wholesale":    10,
}

# FIX 1: complete PIN prefix table for all 15 states
# (original was missing Andhra Pradesh, Madhya Pradesh, Punjab, Haryana, Kerala, Odisha)
STATE_PIN_PREFIX = {
    "Maharashtra":    "4",
    "Karnataka":      "5",
    "Tamil Nadu":     "6",
    "Telangana":      "5",
    "Andhra Pradesh": "5",   # FIX
    "Gujarat":        "3",
    "Rajasthan":      "3",
    "West Bengal":    "7",
    "Uttar Pradesh":  "2",
    "Delhi":          "1",
    "Madhya Pradesh": "4",   # FIX
    "Punjab":         "1",   # FIX
    "Haryana":        "1",   # FIX
    "Kerala":         "6",   # FIX
    "Odisha":         "7",   # FIX
}

# Known city PIN ranges for high-accuracy generation
CITY_PINCODE_RANGES = {
    "Mumbai":    (400001, 400104), "Pune":      (411001, 411060),
    "Bangalore": (560001, 560100), "Chennai":   (600001, 600100),
    "Hyderabad": (500001, 500100), "Ahmedabad": (380001, 380060),
    "Kolkata":   (700001, 700150), "Delhi":     (110001, 110096),
    "Jaipur":    (302001, 302040), "Lucknow":   (226001, 226030),
}

# ========================================
# HELPER FUNCTIONS
# ========================================
def get_random_location():
    state = random.choice(list(INDIAN_CITIES.keys()))
    city  = random.choice(INDIAN_CITIES[state])
    return city, state

def get_indian_pincode(city, state):
    """Return a realistic 6-digit PIN for the given city/state."""
    if city in CITY_PINCODE_RANGES:
        lo, hi = CITY_PINCODE_RANGES[city]
        return str(random.randint(lo, hi))
    prefix = STATE_PIN_PREFIX.get(state, "1")
    return f"{prefix}{random.randint(10000, 99999)}"

def generate_store_address():
    num       = random.randint(1, 999)
    landmark  = random.choice(LANDMARKS)
    road_name = random.choice(ROAD_NAMES)
    addr1     = random.choice(STREET_PATTERNS).format(
        num=num, landmark=landmark, road_name=road_name
    )
    addr2 = random.choice([
        random.choice(AREA_NAMES),
        f"Near {landmark}",
        random.choice(["Ground Floor", "First Floor", "Shop Complex"]),
        None,
    ])
    return addr1, addr2

def get_class_of_trade():
    return random.choices(
        list(CLASS_OF_TRADE.keys()),
        weights=list(CLASS_OF_TRADE.values())
    )[0]

def generate_store_data(store_id):
    cot      = get_class_of_trade()
    is_chain = 'N'
    chain_name = None

    if "Hypermarket" in cot:
        is_chain   = 'Y'
        chain_name = random.choice(RETAIL_CHAINS["Hypermarket"])
        store_name = chain_name
    elif "Supermarket" in cot:
        if random.random() < 0.7:
            is_chain   = 'Y'
            chain_name = random.choice(RETAIL_CHAINS["Supermarket"])
            store_name = chain_name
        else:
            store_name = random.choice(INDEPENDENT_STORE_PATTERNS).format(
                area=random.choice(AREA_NAMES), owner=random.choice(OWNER_NAMES)
            )
    elif "Convenience" in cot:
        is_chain   = 'Y'
        chain_name = random.choice(RETAIL_CHAINS["Convenience"])
        store_name = chain_name
    elif "Wholesale" in cot:
        is_chain   = 'Y'
        chain_name = random.choice(RETAIL_CHAINS["Wholesale"])
        store_name = chain_name
    else:   # Kirana — always independent
        store_name = random.choice(INDEPENDENT_STORE_PATTERNS).format(
            area=random.choice(AREA_NAMES), owner=random.choice(OWNER_NAMES)
        )

    city, state   = get_random_location()
    zip_code      = get_indian_pincode(city, state)   # FIX 1: pass state too
    addr1, addr2  = generate_store_address()

    if is_chain == 'Y':
        store_name = f"{store_name} - {city}"

    return (
        store_id,
        store_name[:50],
        addr1[:150],
        addr2[:100] if addr2 else None,
        city[:25],
        zip_code[:10],
        state[:50],
        cot[:50],
        is_chain,
        chain_name[:50] if chain_name else None,
    )

# ========================================
# CONNECTION
# ========================================
conn = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()

cur.execute("SELECT NVL(MAX(store_id), 0) FROM dim_store_master")
start_id = cur.fetchone()[0]

# ========================================
# GENERATE DATA
# ========================================
INSERT_SQL = """
INSERT INTO dim_store_master (
    store_id, store_name, store_address_lane_1,
    store_address_lane_2, store_city, store_zip,
    store_state, store_class_of_trade, is_chain, chain_name
) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
"""

DAILY_COUNT = 10
data = [generate_store_data(start_id + i) for i in range(1, DAILY_COUNT + 1)]

# FIX 4: wrap insert in try/except with guaranteed connection cleanup
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
# SUMMARY (1 line)
# ========================================
chain_count = sum(1 for d in data if d[8] == 'Y')
cot_counts  = {}
for d in data:
    cot_counts[d[7]] = cot_counts.get(d[7], 0) + 1

print(
    f"Inserted: {len(data)} stores | "
    f"IDs: {start_id + 1}–{start_id + DAILY_COUNT} | "
    f"Chain: {chain_count} | Independent: {len(data) - chain_count} | "
    + " | ".join(f"{k.split(' - ')[-1]}: {v}" for k, v in sorted(cot_counts.items()))
)
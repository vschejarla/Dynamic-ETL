from faker import Faker
import oracledb
import random
from datetime import datetime

fake = Faker('en_IN')

print("📦 DIM_PRODUCT Daily Auto-Increment Started")
print(f"⏰ Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

conn = oracledb.connect(
    user="system",
    password="905966Sh@r4107",
    dsn="host.docker.internal/orcl"
)
cur = conn.cursor()
print("✅ Connected to Oracle Database\n")

# Get the next product_id
cur.execute("SELECT NVL(MAX(product_id),0) FROM dim_product")
start_id = cur.fetchone()[0]

print(f"📊 Current max product_id: {start_id}")
print(f"🔢 New products will start from: {start_id + 1}\n")

# ========================================
# REAL INDIAN RETAIL PRODUCT TEMPLATES
# ========================================
PRODUCT_TEMPLATES = {
    "Grocery": {
        "Rice": {
            "products": ["Basmati Rice", "Sona Masoori Rice", "Brown Rice", "Kolam Rice", "Steamed Rice", "Parboiled Rice"],
            "brands": ["India Gate", "Kohinoor", "Fortune", "Daawat", "Aashirvaad", "Tata Sampann"],
            "flavours": [None],
            "sizes": ["1kg", "5kg", "10kg", "25kg"],
            "sqc": ["PK"],
            "uom": ["KG"],
            "price_range": (50, 800),
            "margin": 15.0  # 15% profit margin
        },
        "Wheat Flour": {
            "products": ["Chakki Atta", "Whole Wheat Flour", "Multigrain Atta", "Organic Atta", "Sharbati Atta"],
            "brands": ["Aashirvaad", "Pillsbury", "Fortune", "Nature Fresh", "Annapurna"],
            "flavours": [None],
            "sizes": ["1kg", "2kg", "5kg", "10kg"],
            "sqc": ["PK"],
            "uom": ["KG"],
            "price_range": (40, 350),
            "margin": 12.0
        },
        "Edible Oil": {
            "products": ["Sunflower Oil", "Refined Oil", "Mustard Oil", "Rice Bran Oil", "Groundnut Oil", "Olive Oil"],
            "brands": ["Fortune", "Sundrop", "Saffola", "Dhara", "Gemini", "Figaro"],
            "flavours": [None],
            "sizes": ["500ml", "1L", "2L", "5L"],
            "sqc": ["BTL"],
            "uom": ["LTR"],
            "price_range": (80, 650),
            "margin": 10.0
        },
        "Pulses & Lentils": {
            "products": ["Toor Dal", "Moong Dal", "Urad Dal", "Chana Dal", "Masoor Dal", "Rajma"],
            "brands": ["Tata Sampann", "Aashirvaad", "Fortune", "MDH", "Catch"],
            "flavours": [None],
            "sizes": ["500g", "1kg", "2kg"],
            "sqc": ["PK"],
            "uom": ["KG"],
            "price_range": (90, 250),
            "margin": 18.0
        },
        "Biscuits & Cookies": {
            "products": ["Cream Biscuits", "Marie Biscuits", "Glucose Biscuits", "Digestive Biscuits", "Butter Cookies"],
            "brands": ["Parle", "Britannia", "Sunfeast", "ITC", "Priya Gold"],
            "flavours": ["Vanilla", "Chocolate", "Butter", "Elaichi", "Orange"],
            "sizes": ["75g", "150g", "300g", "500g", "1kg"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (10, 180),
            "margin": 25.0
        },
        "Spices & Masala": {
            "products": ["Turmeric Powder", "Red Chilli Powder", "Coriander Powder", "Garam Masala", "Kitchen King Masala"],
            "brands": ["MDH", "Everest", "Catch", "Tata Sampann", "Aashirvaad"],
            "flavours": [None],
            "sizes": ["50g", "100g", "200g", "500g"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (15, 120),
            "margin": 30.0
        },
        "Noodles & Pasta": {
            "products": ["Masala Noodles", "Veg Noodles", "Atta Noodles", "Hakka Noodles", "Pasta"],
            "brands": ["Maggi", "Top Ramen", "Yippee", "Sunfeast", "Knorr"],
            "flavours": ["Masala", "Curry", "Tomato", "Chicken", "Vegetable"],
            "sizes": ["70g", "140g", "280g", "560g"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (12, 150),
            "margin": 22.0
        }
    },
    "Beverage": {
        "Carbonated Drinks": {
            "products": ["Cola", "Lemon Soda", "Orange Drink", "Energy Cola", "Diet Cola"],
            "brands": ["Coca Cola", "Pepsi", "Thums Up", "Sprite", "Fanta"],
            "flavours": ["Regular", "Diet", "Zero Sugar"],
            "sizes": ["200ml", "300ml", "500ml", "750ml", "1.25L", "2L"],
            "sqc": ["BTL", "CAN"],
            "uom": ["LTR"],
            "price_range": (20, 90),
            "margin": 20.0
        },
        "Fruit Juices": {
            "products": ["Mango Juice", "Orange Juice", "Apple Juice", "Mixed Fruit Juice", "Pomegranate Juice"],
            "brands": ["Tropicana", "Real", "Paper Boat", "Minute Maid", "B Natural"],
            "flavours": ["Mango", "Orange", "Apple", "Mixed Fruit", "Litchi"],
            "sizes": ["200ml", "500ml", "1L"],
            "sqc": ["BTL", "TETRA"],
            "uom": ["LTR"],
            "price_range": (30, 150),
            "margin": 18.0
        },
        "Tea": {
            "products": ["Black Tea", "Green Tea", "Masala Chai", "Lemon Tea", "Ginger Tea"],
            "brands": ["Tata Tea", "Red Label", "Taj Mahal", "Lipton", "Society"],
            "flavours": ["Regular", "Strong", "Elaichi", "Tulsi", "Lemon"],
            "sizes": ["100g", "250g", "500g", "1kg"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (40, 320),
            "margin": 15.0
        },
        "Coffee": {
            "products": ["Instant Coffee", "Filter Coffee", "Cold Coffee Mix", "Cappuccino", "Coffee Powder"],
            "brands": ["Nescafe", "Bru", "Tata Coffee", "Davidoff", "Sleepy Owl"],
            "flavours": ["Classic", "Strong", "Rich Aroma", "Hazelnut"],
            "sizes": ["50g", "100g", "200g", "500g"],
            "sqc": ["JAR", "PK"],
            "uom": ["G"],
            "price_range": (50, 450),
            "margin": 20.0
        },
        "Energy Drinks": {
            "products": ["Energy Drink", "Sports Drink", "Glucose Drink"],
            "brands": ["Red Bull", "Monster", "Gatorade", "Glucon-D", "Sting"],
            "flavours": ["Regular", "Sugar Free", "Tropical"],
            "sizes": ["250ml", "500ml", "1kg"],
            "sqc": ["BTL", "CAN"],
            "uom": ["LTR"],
            "price_range": (50, 200),
            "margin": 25.0
        }
    },
    "Dairy": {
        "Milk": {
            "products": ["Full Cream Milk", "Toned Milk", "Double Toned Milk", "Skimmed Milk"],
            "brands": ["Amul", "Mother Dairy", "Nestle", "Britannia", "Heritage"],
            "flavours": [None],
            "sizes": ["500ml", "1L", "2L"],
            "sqc": ["TETRA", "POUCH"],
            "uom": ["LTR"],
            "price_range": (25, 120),
            "margin": 8.0
        },
        "Curd & Yogurt": {
            "products": ["Fresh Curd", "Greek Yogurt", "Flavoured Yogurt", "Probiotic Dahi"],
            "brands": ["Amul", "Mother Dairy", "Nestle", "Britannia", "Danone"],
            "flavours": ["Plain", "Strawberry", "Mango", "Blueberry"],
            "sizes": ["200g", "400g", "1kg"],
            "sqc": ["CUP", "TUB"],
            "uom": ["G"],
            "price_range": (15, 85),
            "margin": 12.0
        },
        "Cheese": {
            "products": ["Cheese Slices", "Cheese Cubes", "Cheese Spread", "Mozzarella Cheese", "Processed Cheese"],
            "brands": ["Amul", "Britannia", "Mother Dairy", "Go Cheese", "Kraft"],
            "flavours": ["Plain", "Pepper", "Garlic", "Herbs"],
            "sizes": ["200g", "400g", "500g", "1kg"],
            "sqc": ["PK", "TUB"],
            "uom": ["G"],
            "price_range": (45, 350),
            "margin": 15.0
        },
        "Butter": {
            "products": ["Table Butter", "Cooking Butter", "Salted Butter", "Unsalted Butter"],
            "brands": ["Amul", "Mother Dairy", "Britannia", "Nandini"],
            "flavours": ["Salted", "Unsalted", "Garlic"],
            "sizes": ["100g", "200g", "500g"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (35, 250),
            "margin": 10.0
        },
        "Paneer": {
            "products": ["Fresh Paneer", "Malai Paneer", "Low Fat Paneer"],
            "brands": ["Amul", "Mother Dairy", "Britannia", "Nandini", "Govardhan"],
            "flavours": [None],
            "sizes": ["200g", "500g", "1kg"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (60, 380),
            "margin": 12.0
        }
    },
    "PersonalCare": {
        "Shampoo": {
            "products": ["Anti-Dandruff Shampoo", "Hair Shampoo", "Herbal Shampoo", "Kids Shampoo"],
            "brands": ["Clinic Plus", "Pantene", "Head & Shoulders", "Dove", "Sunsilk"],
            "flavours": [None],
            "sizes": ["180ml", "340ml", "650ml"],
            "sqc": ["BTL"],
            "uom": ["ML"],
            "price_range": (80, 350),
            "margin": 30.0
        },
        "Soap": {
            "products": ["Bathing Soap", "Beauty Soap", "Glycerine Soap", "Herbal Soap"],
            "brands": ["Lux", "Dove", "Lifebuoy", "Pears", "Dettol"],
            "flavours": [None],
            "sizes": ["75g", "100g", "125g", "150g"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (25, 120),
            "margin": 35.0
        },
        "Toothpaste": {
            "products": ["Cavity Protection", "Whitening Toothpaste", "Sensitive Toothpaste", "Herbal Toothpaste"],
            "brands": ["Colgate", "Pepsodent", "Close-Up", "Sensodyne", "Dabur"],
            "flavours": [None],
            "sizes": ["100g", "150g", "200g"],
            "sqc": ["TUB"],
            "uom": ["G"],
            "price_range": (40, 180),
            "margin": 32.0
        },
        "Face Wash": {
            "products": ["Face Wash", "Acne Face Wash", "Brightening Face Wash", "Oil Control"],
            "brands": ["Garnier", "Himalaya", "Neutrogena", "Nivea", "Clean & Clear"],
            "flavours": [None],
            "sizes": ["50ml", "100ml", "150ml"],
            "sqc": ["TUB"],
            "uom": ["ML"],
            "price_range": (50, 250),
            "margin": 28.0
        },
        "Hair Oil": {
            "products": ["Coconut Oil", "Almond Oil", "Hair Oil", "Amla Oil", "Castor Oil"],
            "brands": ["Parachute", "Dabur", "Bajaj", "Navratna", "Indulekha"],
            "flavours": [None],
            "sizes": ["100ml", "200ml", "500ml"],
            "sqc": ["BTL"],
            "uom": ["ML"],
            "price_range": (30, 200),
            "margin": 25.0
        }
    },
    "HomeCare": {
        "Detergent": {
            "products": ["Detergent Powder", "Washing Powder", "Liquid Detergent", "Fabric Wash"],
            "brands": ["Surf Excel", "Ariel", "Tide", "Rin", "Wheel"],
            "flavours": [None],
            "sizes": ["500g", "1kg", "2kg", "5kg"],
            "sqc": ["PK", "BTL"],
            "uom": ["KG"],
            "price_range": (50, 500),
            "margin": 20.0
        },
        "Dishwash": {
            "products": ["Dishwash Bar", "Dishwash Liquid", "Dishwash Gel"],
            "brands": ["Vim", "Pril", "Exo", "Finish"],
            "flavours": ["Lemon", "Regular"],
            "sizes": ["200g", "500ml", "1L"],
            "sqc": ["BTL", "PK"],
            "uom": ["LTR"],
            "price_range": (20, 150),
            "margin": 25.0
        },
        "Floor Cleaner": {
            "products": ["Floor Cleaner", "Disinfectant Floor Cleaner", "Phenyl"],
            "brands": ["Lizol", "Harpic", "Dettol", "Colin"],
            "flavours": ["Jasmine", "Citrus", "Pine", "Lavender"],
            "sizes": ["500ml", "1L", "2L"],
            "sqc": ["BTL"],
            "uom": ["LTR"],
            "price_range": (50, 300),
            "margin": 22.0
        },
        "Toilet Cleaner": {
            "products": ["Toilet Cleaner", "Toilet Bowl Cleaner", "Disinfectant"],
            "brands": ["Harpic", "Domex", "Lizol", "Sanifresh"],
            "flavours": [None],
            "sizes": ["500ml", "1L"],
            "sqc": ["BTL"],
            "uom": ["LTR"],
            "price_range": (60, 200),
            "margin": 20.0
        }
    },
    "BabyCare": {
        "Baby Soap": {
            "products": ["Baby Soap", "Baby Bathing Bar", "Mild Baby Soap"],
            "brands": ["Johnson & Johnson", "Himalaya", "Mee Mee", "Chicco", "Sebamed"],
            "flavours": [None],
            "sizes": ["75g", "100g", "125g"],
            "sqc": ["PK"],
            "uom": ["G"],
            "price_range": (30, 100),
            "margin": 28.0
        },
        "Baby Powder": {
            "products": ["Baby Powder", "Talcum Powder", "Prickly Heat Powder"],
            "brands": ["Johnson & Johnson", "Himalaya", "Sebamed", "Mee Mee"],
            "flavours": [None],
            "sizes": ["100g", "200g", "400g"],
            "sqc": ["BTL"],
            "uom": ["G"],
            "price_range": (50, 250),
            "margin": 25.0
        },
        "Baby Wipes": {
            "products": ["Baby Wipes", "Wet Wipes", "Sensitive Wipes"],
            "brands": ["Pampers", "Himalaya", "Mee Mee", "Huggies", "Johnson's"],
            "flavours": [None],
            "sizes": ["20pcs", "40pcs", "80pcs"],
            "sqc": ["PK"],
            "uom": ["PCS"],
            "price_range": (50, 300),
            "margin": 20.0
        },
        "Diapers": {
            "products": ["Baby Diapers", "Diaper Pants", "New Born Diapers"],
            "brands": ["Pampers", "Huggies", "MamyPoko", "Himalaya", "Mee Mee"],
            "flavours": [None],
            "sizes": ["Small", "Medium", "Large", "XL"],
            "sqc": ["PK"],
            "uom": ["PCS"],
            "price_range": (200, 1500),
            "margin": 15.0
        }
    }
}

# ========================================
# GENERATE 10 NEW PRODUCTS
# ========================================
insert_sql = """
INSERT INTO dim_product (
    product_id, product_name, category, sub_category,
    brand, flavour, product_size, sqc, uom, unit_price
) VALUES (
    :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
)
"""

data = []
PRODUCTS_PER_RUN = 10  # Generate exactly 10 products per run

print(f"{'='*70}")
print(f"NEW PRODUCTS BEING GENERATED:")
print(f"{'='*70}\n")

for i in range(1, PRODUCTS_PER_RUN + 1):
    product_id = start_id + i
    
    # Randomly select category and subcategory
    category = random.choice(list(PRODUCT_TEMPLATES.keys()))
    sub_category = random.choice(list(PRODUCT_TEMPLATES[category].keys()))
    
    template = PRODUCT_TEMPLATES[category][sub_category]
    
    # Build product based on template
    product_name = random.choice(template["products"])
    brand = random.choice(template["brands"])
    flavour = random.choice(template["flavours"])
    product_size = random.choice(template["sizes"])
    sqc = random.choice(template["sqc"])
    uom = random.choice(template["uom"])
    
    # Generate price within realistic range
    min_price, max_price = template["price_range"]
    unit_price = round(random.uniform(min_price, max_price), 2)
    
    # Calculate margin (for business analytics)
    margin = template.get("margin", 15.0)
    
    data.append((
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        flavour,
        product_size,
        sqc,
        uom,
        unit_price
    ))
    
    # Display with business context
    print(f"  📦 Product #{product_id}")
    print(f"     Name: {product_name}")
    print(f"     Brand: {brand}")
    print(f"     Category: {category} > {sub_category}")
    print(f"     Size: {product_size} | Pack: {sqc} | UOM: {uom}")
    print(f"     Price: ₹{unit_price:.2f} | Est. Margin: {margin}%")
    if flavour:
        print(f"     Flavour: {flavour}")
    print()

# ========================================
# INSERT DATA
# ========================================
print(f"{'='*70}")
print(f"INSERTING INTO DATABASE...")
print(f"{'='*70}\n")

try:
    cur.executemany(insert_sql, data)
    conn.commit()
    print(f"✅ Successfully inserted {len(data)} new products")
except Exception as e:
    print(f"❌ Error during insert: {e}")
    conn.rollback()
    cur.close()
    conn.close()
    exit(1)

# ========================================
# SUMMARY STATISTICS
# ========================================
print(f"\n{'='*70}")
print(f"📊 THIS RUN SUMMARY")
print(f"{'='*70}\n")

print(f"   Products inserted: {len(data)}")
print(f"   Product ID range: {start_id + 1} to {start_id + PRODUCTS_PER_RUN}")

# Category breakdown for this run
print(f"\n   Category breakdown (this run):")
cat_counts = {}
for d in data:
    cat = d[2]
    cat_counts[cat] = cat_counts.get(cat, 0) + 1

for cat, count in sorted(cat_counts.items()):
    print(f"      {cat}: {count} product{'s' if count > 1 else ''}")

# Price statistics for this run
prices = [d[9] for d in data]
avg_price = sum(prices) / len(prices)
min_price_run = min(prices)
max_price_run = max(prices)

print(f"\n   Price range (this run):")
print(f"      Average: ₹{avg_price:.2f}")
print(f"      Min: ₹{min_price_run:.2f}")
print(f"      Max: ₹{max_price_run:.2f}")

# Overall table statistics
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT category) as categories,
        COUNT(DISTINCT sub_category) as subcategories,
        COUNT(DISTINCT brand) as brands,
        MIN(unit_price) as min_price,
        MAX(unit_price) as max_price,
        AVG(unit_price) as avg_price
    FROM dim_product
""")

stats = cur.fetchone()

print(f"\n{'='*70}")
print(f"📈 OVERALL TABLE STATISTICS")
print(f"{'='*70}\n")

if stats:
    print(f"   Total products: {stats[0]:,}")
    print(f"   Unique categories: {stats[1]}")
    print(f"   Unique sub-categories: {stats[2]}")
    print(f"   Unique brands: {stats[3]}")
    print(f"\n   Overall price range:")
    print(f"      Min: ₹{stats[4]:.2f}")
    print(f"      Max: ₹{stats[5]:.2f}")
    print(f"      Average: ₹{stats[6]:.2f}")

# Category-wise totals
cur.execute("""
    SELECT category, COUNT(*) as cnt
    FROM dim_product
    GROUP BY category
    ORDER BY cnt DESC
""")

print(f"\n   Products by category:")
for row in cur.fetchall():
    print(f"      {row[0]}: {row[1]:,} products")

cur.close()
conn.close()

print(f"\n{'='*70}")
print(f"🎉 DIM_PRODUCT DAILY INCREMENT COMPLETED")
print(f"{'='*70}")
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n💡 Schedule this script to run daily in Airflow for continuous updates")
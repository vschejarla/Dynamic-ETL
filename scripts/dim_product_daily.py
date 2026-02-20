from faker import Faker
import oracledb
import random
from datetime import datetime

fake = Faker('en_IN')
<<<<<<< HEAD

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
=======
print("📦 DIM_PRODUCT Daily Auto-Increment Started")
print(f"{'='*70}\n")
print(f"🕒 Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*70}\n")

connection = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)

cursor = connection.cursor()
print("✅ Connected to Oracle Database\n")

# ========================================
# CORRECTED INDIAN RETAIL PRODUCT CATALOG
# Category → Subcategory → Brand → Product → Details
# ========================================
PRODUCTS = {
    "Grocery": {
        "Oils": {
            "Fortune": {
                "products": [
                    {"name": "Sunflower Oil", "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (120, 650)},
                    {"name": "Refined Oil", "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (110, 600)},
                    {"name": "Rice Bran Oil", "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (130, 680)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Sundrop": {
                "products": [
                    {"name": "Sunflower Oil", "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (115, 640)},
                    {"name": "Heart Oil", "sizes": ["500ml", "1L", "2L"], "sku_prefix": "OIL", "price_range": (140, 550)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Saffola": {
                "products": [
                    {"name": "Gold Oil", "sizes": ["1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (160, 850)},
                    {"name": "Active Oil", "sizes": ["1L", "2L"], "sku_prefix": "OIL", "price_range": (180, 700)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Dhara": {
                "products": [
                    {"name": "Mustard Oil", "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (100, 580)},
                    {"name": "Groundnut Oil", "sizes": ["500ml", "1L", "2L"], "sku_prefix": "OIL", "price_range": (120, 600)}
                ],
                "flavours": None,
                "uom": "LTR"
            }
        },
        "Rice": {
            "India Gate": {
                "products": [
                    {"name": "Basmati Rice", "sizes": ["1kg", "5kg", "10kg", "25kg"], "sku_prefix": "RIC", "price_range": (120, 1200)},
                    {"name": "Classic Basmati", "sizes": ["1kg", "5kg", "10kg"], "sku_prefix": "RIC", "price_range": (100, 950)},
                    {"name": "Dubar Basmati", "sizes": ["5kg", "10kg"], "sku_prefix": "RIC", "price_range": (500, 1100)}
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Daawat": {
                "products": [
                    {"name": "Rozana Basmati", "sizes": ["1kg", "5kg"], "sku_prefix": "RIC", "price_range": (90, 480)},
                    {"name": "Traditional Basmati", "sizes": ["1kg", "5kg", "10kg"], "sku_prefix": "RIC", "price_range": (110, 1050)}
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Kohinoor": {
                "products": [
                    {"name": "Super Basmati", "sizes": ["1kg", "5kg"], "sku_prefix": "RIC", "price_range": (130, 680)},
                    {"name": "Classic Basmati", "sizes": ["5kg", "10kg"], "sku_prefix": "RIC", "price_range": (450, 980)}
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Fortune": {
                "products": [
                    {"name": "Sona Masoori Rice", "sizes": ["1kg", "5kg", "10kg", "25kg"], "sku_prefix": "RIC", "price_range": (50, 700)},
                    {"name": "Biryani Rice", "sizes": ["1kg", "5kg"], "sku_prefix": "RIC", "price_range": (80, 420)}
                ],
                "flavours": None,
                "uom": "KG"
            }
        },
        "Atta": {
            "Aashirvaad": {
                "products": [
                    {"name": "Chakki Atta", "sizes": ["1kg", "2kg", "5kg", "10kg"], "sku_prefix": "ATT", "price_range": (45, 380)},
                    {"name": "Multigrain Atta", "sizes": ["1kg", "5kg"], "sku_prefix": "ATT", "price_range": (60, 320)},
                    {"name": "Select Atta", "sizes": ["5kg", "10kg"], "sku_prefix": "ATT", "price_range": (240, 520)}
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Pillsbury": {
                "products": [
                    {"name": "Chakki Atta", "sizes": ["1kg", "5kg", "10kg"], "sku_prefix": "ATT", "price_range": (42, 370)},
                    {"name": "Multigrain Atta", "sizes": ["1kg", "5kg"], "sku_prefix": "ATT", "price_range": (55, 310)}
                ],
                "flavours": None,
                "uom": "KG"
            }
        },
        "Pulses": {
            "Tata Sampann": {
                "products": [
                    {"name": "Toor Dal", "sizes": ["500g", "1kg", "2kg"], "sku_prefix": "DAL", "price_range": (90, 280)},
                    {"name": "Moong Dal", "sizes": ["500g", "1kg"], "sku_prefix": "DAL", "price_range": (85, 195)},
                    {"name": "Urad Dal", "sizes": ["500g", "1kg"], "sku_prefix": "DAL", "price_range": (95, 210)}
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Fortune": {
                "products": [
                    {"name": "Chana Dal", "sizes": ["500g", "1kg", "2kg"], "sku_prefix": "DAL", "price_range": (80, 240)},
                    {"name": "Masoor Dal", "sizes": ["500g", "1kg"], "sku_prefix": "DAL", "price_range": (75, 180)}
                ],
                "flavours": None,
                "uom": "KG"
            }
        },
        "Spices": {
            "MDH": {
                "products": [
                    {"name": "Turmeric Powder", "sizes": ["50g", "100g", "200g", "500g"], "sku_prefix": "SPC", "price_range": (15, 120)},
                    {"name": "Red Chilli Powder", "sizes": ["50g", "100g", "200g", "500g"], "sku_prefix": "SPC", "price_range": (20, 130)},
                    {"name": "Garam Masala", "sizes": ["50g", "100g", "200g"], "sku_prefix": "SPC", "price_range": (25, 110)},
                    {"name": "Coriander Powder", "sizes": ["50g", "100g", "200g"], "sku_prefix": "SPC", "price_range": (18, 95)}
                ],
                "flavours": None,
                "uom": "G"
            },
            "Everest": {
                "products": [
                    {"name": "Turmeric Powder", "sizes": ["50g", "100g", "200g", "500g"], "sku_prefix": "SPC", "price_range": (16, 125)},
                    {"name": "Chilli Powder", "sizes": ["50g", "100g", "200g"], "sku_prefix": "SPC", "price_range": (22, 115)},
                    {"name": "Pav Bhaji Masala", "sizes": ["50g", "100g"], "sku_prefix": "SPC", "price_range": (28, 85)}
                ],
                "flavours": None,
                "uom": "G"
            },
            "Catch": {
                "products": [
                    {"name": "Chat Masala", "sizes": ["50g", "100g"], "sku_prefix": "SPC", "price_range": (30, 90)},
                    {"name": "Garam Masala", "sizes": ["50g", "100g", "200g"], "sku_prefix": "SPC", "price_range": (26, 105)}
                ],
                "flavours": None,
                "uom": "G"
            }
        },
        "Biscuits": {
            "Parle": {
                "products": [
                    {"name": "Parle-G", "sizes": ["75g", "150g", "300g", "500g", "1kg"], "sku_prefix": "BIS", "price_range": (10, 180)},
                    {"name": "Monaco", "sizes": ["75g", "150g", "300g"], "sku_prefix": "BIS", "price_range": (12, 85)},
                    {"name": "Hide & Seek", "sizes": ["100g", "200g", "400g"], "sku_prefix": "BIS", "price_range": (20, 140)}
                ],
                "flavours": ["Original", "Chocolate Chip", "Bourbon", "Milano"],
                "flavour_probability": 0.50,
                "uom": "G"
            },
            "Britannia": {
                "products": [
                    {"name": "Good Day", "sizes": ["100g", "200g", "400g", "600g"], "sku_prefix": "BIS", "price_range": (18, 150)},
                    {"name": "Marie Gold", "sizes": ["75g", "150g", "250g"], "sku_prefix": "BIS", "price_range": (12, 70)},
                    {"name": "NutriChoice", "sizes": ["100g", "200g"], "sku_prefix": "BIS", "price_range": (25, 85)}
                ],
                "flavours": ["Butter", "Cashew", "Coconut", "Chocolate", "Ragi"],
                "flavour_probability": 0.55,
                "uom": "G"
            },
            "Sunfeast": {
                "products": [
                    {"name": "Dark Fantasy", "sizes": ["75g", "150g", "300g"], "sku_prefix": "BIS", "price_range": (30, 130)},
                    {"name": "Marie Light", "sizes": ["75g", "150g"], "sku_prefix": "BIS", "price_range": (15, 55)}
                ],
                "flavours": ["Choco Fills", "Vanilla", "Bourbon"],
                "flavour_probability": 0.60,
                "uom": "G"
            }
        },
        "Noodles": {
            "Maggi": {
                "products": [
                    {"name": "Maggi Masala", "sizes": ["70g", "140g", "280g", "560g"], "sku_prefix": "NOD", "price_range": (12, 150)},
                    {"name": "Maggi Atta Noodles", "sizes": ["70g", "280g"], "sku_prefix": "NOD", "price_range": (15, 125)},
                    {"name": "Maggi Cuppa Mania", "sizes": ["70g"], "sku_prefix": "NOD", "price_range": (35, 45)}
                ],
                "flavours": ["Masala", "Atta Masala", "Curry", "Veg Atta", "Chicken"],
                "flavour_probability": 0.90,
                "uom": "G"
            },
            "Top Ramen": {
                "products": [
                    {"name": "Top Ramen Curry", "sizes": ["70g", "280g"], "sku_prefix": "NOD", "price_range": (12, 120)},
                    {"name": "Top Ramen Cup", "sizes": ["70g"], "sku_prefix": "NOD", "price_range": (30, 40)}
                ],
                "flavours": ["Curry", "Masala", "Chicken"],
                "flavour_probability": 0.95,
                "uom": "G"
            },
            "Yippee": {
                "products": [
                    {"name": "Yippee Noodles", "sizes": ["70g", "240g"], "sku_prefix": "NOD", "price_range": (10, 95)}
                ],
                "flavours": ["Magic Masala", "Classic Masala", "Tricolor"],
                "flavour_probability": 0.90,
                "uom": "G"
            }
        },
        "Snacks": {
            "Haldiram's": {
                "products": [
                    {"name": "Aloo Bhujia", "sizes": ["50g", "150g", "200g", "400g", "1kg"], "sku_prefix": "SNK", "price_range": (10, 280)},
                    {"name": "Moong Dal", "sizes": ["50g", "150g", "200g"], "sku_prefix": "SNK", "price_range": (12, 110)},
                    {"name": "Namkeen Mix", "sizes": ["150g", "200g", "400g"], "sku_prefix": "SNK", "price_range": (30, 180)},
                    {"name": "Khatta Meetha", "sizes": ["150g", "200g", "400g"], "sku_prefix": "SNK", "price_range": (35, 190)}
                ],
                "flavours": ["Classic", "Masala", "Pudina", "Chatpata", "Bhelpuri Mix"],
                "flavour_probability": 0.65,
                "uom": "G"
            },
            "Bikaji": {
                "products": [
                    {"name": "Bhujia", "sizes": ["50g", "150g", "200g", "500g"], "sku_prefix": "SNK", "price_range": (10, 180)},
                    {"name": "Sev", "sizes": ["50g", "150g", "200g"], "sku_prefix": "SNK", "price_range": (15, 95)}
                ],
                "flavours": ["Plain", "Masala", "Ratlami"],
                "flavour_probability": 0.60,
                "uom": "G"
            },
            "Lay's": {
                "products": [
                    {"name": "Lay's Chips", "sizes": ["25g", "50g", "90g", "150g"], "sku_prefix": "SNK", "price_range": (10, 90)}
                ],
                "flavours": ["Classic Salted", "Magic Masala", "Cream & Onion", "American Style Cream & Onion", "Spanish Tomato Tango"],
                "flavour_probability": 0.95,
                "uom": "G"
            },
            "Uncle Chipps": {
                "products": [
                    {"name": "Uncle Chipps", "sizes": ["25g", "55g", "120g"], "sku_prefix": "SNK", "price_range": (10, 60)}
                ],
                "flavours": ["Spicy Treat", "Salted"],
                "flavour_probability": 0.85,
                "uom": "G"
            },
            "Kurkure": {
                "products": [
                    {"name": "Kurkure Namkeen", "sizes": ["40g", "90g", "140g"], "sku_prefix": "SNK", "price_range": (10, 70)}
                ],
                "flavours": ["Masala Munch", "Green Chutney", "Chilli Chatka", "Solid Masti"],
                "flavour_probability": 0.90,
                "uom": "G"
            },
            "Bingo": {
                "products": [
                    {"name": "Bingo Chips", "sizes": ["25g", "50g", "100g"], "sku_prefix": "SNK", "price_range": (10, 55)}
                ],
                "flavours": ["Mad Angles", "Tedhe Medhe", "Tangles"],
                "flavour_probability": 0.90,
                "uom": "G"
            }
        }
    },
    "Beverage": {
        "Soft Drink": {
            "Coca Cola": {
                "products": [
                    {"name": "Coca Cola", "sizes": ["200ml", "300ml", "500ml", "750ml", "1L", "1.25L", "2L"], "sku_prefix": "SDR", "price_range": (20, 90)}
                ],
                "flavours": ["Regular", "Diet Coke", "Zero Sugar"],
                "flavour_probability": 0.40,
                "uom": "LTR"
            },
            "Thums Up": {
                "products": [
                    {"name": "Thums Up", "sizes": ["200ml", "300ml", "500ml", "750ml", "1.25L", "2L"], "sku_prefix": "SDR", "price_range": (20, 90)}
                ],
                "flavours": ["Regular", "Charged"],
                "flavour_probability": 0.30,
                "uom": "LTR"
            },
            "Pepsi": {
                "products": [
                    {"name": "Pepsi", "sizes": ["200ml", "300ml", "500ml", "1L", "1.25L", "2L"], "sku_prefix": "SDR", "price_range": (20, 85)}
                ],
                "flavours": ["Regular", "Black", "Diet"],
                "flavour_probability": 0.35,
                "uom": "LTR"
            },
            "Sprite": {
                "products": [
                    {"name": "Sprite", "sizes": ["200ml", "300ml", "500ml", "750ml", "1.25L", "2L"], "sku_prefix": "SDR", "price_range": (20, 85)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Fanta": {
                "products": [
                    {"name": "Fanta", "sizes": ["200ml", "300ml", "500ml", "1.25L", "2L"], "sku_prefix": "SDR", "price_range": (20, 85)}
                ],
                "flavours": ["Orange", "Apple"],
                "flavour_probability": 0.70,
                "uom": "LTR"
            },
            "Mountain Dew": {
                "products": [
                    {"name": "Mountain Dew", "sizes": ["200ml", "500ml", "750ml", "1.25L"], "sku_prefix": "SDR", "price_range": (20, 80)}
                ],
                "flavours": None,
                "uom": "LTR"
            }
        },
        "Juice": {
            "Tropicana": {
                "products": [
                    {"name": "Fruit Juice", "sizes": ["200ml", "500ml", "1L"], "sku_prefix": "JUC", "price_range": (30, 150)}
                ],
                "flavours": ["Mango", "Orange", "Apple", "Mixed Fruit", "Pomegranate"],
                "flavour_probability": 0.95,
                "uom": "LTR"
            },
            "Real": {
                "products": [
                    {"name": "Fruit Juice", "sizes": ["200ml", "1L"], "sku_prefix": "JUC", "price_range": (25, 130)}
                ],
                "flavours": ["Mango", "Orange", "Apple", "Mixed Fruit"],
                "flavour_probability": 0.95,
                "uom": "LTR"
            },
            "Maaza": {
                "products": [
                    {"name": "Mango Drink", "sizes": ["200ml", "600ml", "1.2L"], "sku_prefix": "JUC", "price_range": (20, 100)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Paper Boat": {
                "products": [
                    {"name": "Traditional Drink", "sizes": ["250ml"], "sku_prefix": "JUC", "price_range": (30, 40)}
                ],
                "flavours": ["Aamras", "Jaljeera", "Aam Panna", "Jamun Kala Khatta"],
                "flavour_probability": 0.95,
                "uom": "LTR"
            }
        },
        "Tea": {
            "Tata Tea": {
                "products": [
                    {"name": "Premium Tea", "sizes": ["100g", "250g", "500g", "1kg"], "sku_prefix": "TEA", "price_range": (50, 360)},
                    {"name": "Gold Tea", "sizes": ["100g", "250g", "500g"], "sku_prefix": "TEA", "price_range": (60, 320)},
                    {"name": "Agni Tea", "sizes": ["100g", "250g", "500g"], "sku_prefix": "TEA", "price_range": (55, 310)}
                ],
                "flavours": ["Plain", "Elaichi", "Ginger"],
                "flavour_probability": 0.40,
                "uom": "G"
            },
            "Red Label": {
                "products": [
                    {"name": "Natural Care Tea", "sizes": ["100g", "250g", "500g", "1kg"], "sku_prefix": "TEA", "price_range": (45, 340)}
                ],
                "flavours": None,
                "uom": "G"
            },
            "Taj Mahal": {
                "products": [
                    {"name": "Premium Tea", "sizes": ["100g", "250g", "500g"], "sku_prefix": "TEA", "price_range": (65, 350)}
                ],
                "flavours": None,
                "uom": "G"
            },
            "Lipton": {
                "products": [
                    {"name": "Green Tea", "sizes": ["25 Bags", "100 Bags"], "sku_prefix": "TEA", "price_range": (80, 420)}
                ],
                "flavours": ["Lemon", "Tulsi", "Honey", "Plain", "Mint"],
                "flavour_probability": 0.75,
                "uom": "PCS"
            }
        },
        "Coffee": {
            "Nescafe": {
                "products": [
                    {"name": "Classic Coffee", "sizes": ["50g", "100g", "200g"], "sku_prefix": "COF", "price_range": (80, 450)},
                    {"name": "Gold Coffee", "sizes": ["50g", "100g"], "sku_prefix": "COF", "price_range": (120, 380)}
                ],
                "flavours": ["Classic", "Rich", "Strong"],
                "flavour_probability": 0.50,
                "uom": "G"
            },
            "Bru": {
                "products": [
                    {"name": "Instant Coffee", "sizes": ["50g", "100g", "200g"], "sku_prefix": "COF", "price_range": (70, 400)},
                    {"name": "Gold Roast", "sizes": ["50g", "100g"], "sku_prefix": "COF", "price_range": (100, 330)}
                ],
                "flavours": None,
                "uom": "G"
            }
        },
        "Energy Drink": {
            "Red Bull": {
                "products": [
                    {"name": "Energy Drink", "sizes": ["250ml"], "sku_prefix": "ENR", "price_range": (115, 125)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Sting": {
                "products": [
                    {"name": "Energy Drink", "sizes": ["250ml", "500ml"], "sku_prefix": "ENR", "price_range": (20, 40)}
                ],
                "flavours": ["Berry Blast", "Power Lime"],
                "flavour_probability": 0.60,
                "uom": "LTR"
            },
            "Gatorade": {
                "products": [
                    {"name": "Sports Drink", "sizes": ["500ml", "1L"], "sku_prefix": "ENR", "price_range": (50, 100)}
                ],
                "flavours": ["Orange", "Lemon", "Cool Blue"],
                "flavour_probability": 0.85,
                "uom": "LTR"
            }
>>>>>>> etl-update
        }
    },
    "Dairy": {
        "Milk": {
<<<<<<< HEAD
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
=======
            "Amul": {
                "products": [
                    {"name": "Full Cream Milk", "sizes": ["500ml", "1L"], "sku_prefix": "MLK", "price_range": (30, 68)},
                    {"name": "Toned Milk", "sizes": ["500ml", "1L"], "sku_prefix": "MLK", "price_range": (25, 56)},
                    {"name": "Gold Milk", "sizes": ["500ml", "1L"], "sku_prefix": "MLK", "price_range": (32, 70)}
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Full Cream Milk", "sizes": ["500ml", "1L"], "sku_prefix": "MLK", "price_range": (28, 66)},
                    {"name": "Toned Milk", "sizes": ["500ml", "1L"], "sku_prefix": "MLK", "price_range": (24, 54)}
                ],
                "flavours": None,
                "uom": "LTR"
            }
        },
        "Curd": {
            "Amul": {
                "products": [
                    {"name": "Fresh Curd", "sizes": ["200g", "400g", "1kg"], "sku_prefix": "CRD", "price_range": (20, 85)},
                    {"name": "Masti Dahi", "sizes": ["400g"], "sku_prefix": "CRD", "price_range": (35, 45)}
                ],
                "flavours": ["Plain", "Mango", "Strawberry"],
                "flavour_probability": 0.35,
                "uom": "G"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Dahi", "sizes": ["200g", "400g", "1kg"], "sku_prefix": "CRD", "price_range": (18, 80)}
                ],
                "flavours": None,
                "uom": "G"
            }
        },
        "Cheese": {
            "Amul": {
                "products": [
                    {"name": "Processed Cheese", "sizes": ["200g", "400g", "1kg"], "sku_prefix": "CHZ", "price_range": (80, 380)},
                    {"name": "Pizza Cheese", "sizes": ["200g"], "sku_prefix": "CHZ", "price_range": (120, 140)},
                    {"name": "Cheese Slices", "sizes": ["200g", "400g"], "sku_prefix": "CHZ", "price_range": (100, 220)}
                ],
                "flavours": ["Plain", "Pepper", "Garlic"],
                "flavour_probability": 0.45,
                "uom": "G"
            },
            "Britannia": {
                "products": [
                    {"name": "Cheese Slices", "sizes": ["200g", "400g"], "sku_prefix": "CHZ", "price_range": (95, 210)},
                    {"name": "Cheese Cubes", "sizes": ["200g"], "sku_prefix": "CHZ", "price_range": (110, 125)}
                ],
                "flavours": None,
                "uom": "G"
            }
        },
        "Butter": {
            "Amul": {
                "products": [
                    {"name": "Butter", "sizes": ["100g", "500g"], "sku_prefix": "BUT", "price_range": (45, 270)},
                    {"name": "Lite Butter", "sizes": ["100g"], "sku_prefix": "BUT", "price_range": (50, 55)}
                ],
                "flavours": ["Salted", "Unsalted"],
                "flavour_probability": 0.65,
                "uom": "G"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Table Butter", "sizes": ["100g", "500g"], "sku_prefix": "BUT", "price_range": (42, 260)}
                ],
                "flavours": ["Salted", "Unsalted"],
                "flavour_probability": 0.60,
                "uom": "G"
            }
        },
        "Paneer": {
            "Amul": {
                "products": [
                    {"name": "Fresh Paneer", "sizes": ["200g", "500g", "1kg"], "sku_prefix": "PNR", "price_range": (80, 420)}
                ],
                "flavours": None,
                "uom": "G"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Paneer", "sizes": ["200g", "500g"], "sku_prefix": "PNR", "price_range": (75, 380)}
                ],
                "flavours": None,
                "uom": "G"
            }
        }
    },
    "Personal Care": {
        "Shampoo": {
            "Clinic Plus": {
                "products": [
                    {"name": "Strong & Long Shampoo", "sizes": ["180ml", "340ml", "650ml"], "sku_prefix": "SHP", "price_range": (90, 380)}
                ],
                "flavours": ["Strong & Long", "Natural Shine"],
                "flavour_probability": 0.60,
                "uom": "ML"
            },
            "Dove": {
                "products": [
                    {"name": "Hair Shampoo", "sizes": ["180ml", "340ml", "650ml"], "sku_prefix": "SHP", "price_range": (110, 420)}
                ],
                "flavours": ["Intense Repair", "Daily Shine", "Anti-Dandruff"],
                "flavour_probability": 0.80,
                "uom": "ML"
            },
            "Pantene": {
                "products": [
                    {"name": "Pro-V Shampoo", "sizes": ["180ml", "340ml", "650ml"], "sku_prefix": "SHP", "price_range": (100, 400)}
                ],
                "flavours": ["Silky Smooth", "Total Damage Care", "Hair Fall Control"],
                "flavour_probability": 0.85,
                "uom": "ML"
            },
            "Head & Shoulders": {
                "products": [
                    {"name": "Anti-Dandruff Shampoo", "sizes": ["180ml", "340ml", "650ml"], "sku_prefix": "SHP", "price_range": (120, 450)}
                ],
                "flavours": ["Cool Menthol", "Smooth & Silky", "Clean & Balanced"],
                "flavour_probability": 0.75,
                "uom": "ML"
            }
        },
        "Soap": {
            "Lux": {
                "products": [
                    {"name": "Beauty Soap", "sizes": ["75g", "100g", "125g", "150g"], "sku_prefix": "SOP", "price_range": (25, 135)}
                ],
                "flavours": ["Rose", "Jasmine", "Saffron", "Sandal"],
                "flavour_probability": 0.80,
                "uom": "G"
            },
            "Dove": {
                "products": [
                    {"name": "Beauty Bathing Bar", "sizes": ["75g", "100g", "125g"], "sku_prefix": "SOP", "price_range": (30, 140)}
                ],
                "flavours": ["Original", "Go Fresh", "Deeply Nourishing"],
                "flavour_probability": 0.70,
                "uom": "G"
            },
            "Lifebuoy": {
                "products": [
                    {"name": "Germ Protection Soap", "sizes": ["75g", "100g", "125g"], "sku_prefix": "SOP", "price_range": (22, 120)}
                ],
                "flavours": ["Total", "Nature", "Care"],
                "flavour_probability": 0.65,
                "uom": "G"
            },
            "Dettol": {
                "products": [
                    {"name": "Antiseptic Soap", "sizes": ["75g", "125g"], "sku_prefix": "SOP", "price_range": (35, 90)}
                ],
                "flavours": ["Original", "Skincare", "Cool", "Fresh"],
                "flavour_probability": 0.75,
                "uom": "G"
            }
        },
        "Toothpaste": {
            "Colgate": {
                "products": [
                    {"name": "Dental Cream", "sizes": ["100g", "200g"], "sku_prefix": "TPS", "price_range": (50, 140)},
                    {"name": "Total", "sizes": ["100g", "150g"], "sku_prefix": "TPS", "price_range": (70, 150)},
                    {"name": "MaxFresh", "sizes": ["80g", "150g"], "sku_prefix": "TPS", "price_range": (60, 130)}
                ],
                "flavours": ["Regular", "Mint", "Charcoal", "Sensitive"],
                "flavour_probability": 0.60,
                "uom": "G"
            },
            "Pepsodent": {
                "products": [
                    {"name": "Germi Check", "sizes": ["100g", "150g"], "sku_prefix": "TPS", "price_range": (45, 125)}
                ],
                "flavours": ["Regular", "Clove"],
                "flavour_probability": 0.50,
                "uom": "G"
            },
            "Sensodyne": {
                "products": [
                    {"name": "Sensitive Toothpaste", "sizes": ["75g", "100g"], "sku_prefix": "TPS", "price_range": (120, 180)}
                ],
                "flavours": ["Fresh Mint", "Repair & Protect", "Whitening"],
                "flavour_probability": 0.75,
                "uom": "G"
            }
        },
        "Hair Oil": {
            "Parachute": {
                "products": [
                    {"name": "Coconut Oil", "sizes": ["100ml", "200ml", "500ml", "1L"], "sku_prefix": "OIL", "price_range": (35, 350)}
                ],
                "flavours": ["Pure", "Jasmine", "Cooling"],
                "flavour_probability": 0.55,
                "uom": "ML"
            },
            "Dabur": {
                "products": [
                    {"name": "Amla Hair Oil", "sizes": ["100ml", "200ml", "500ml"], "sku_prefix": "OIL", "price_range": (45, 280)},
                    {"name": "Almond Hair Oil", "sizes": ["100ml", "200ml"], "sku_prefix": "OIL", "price_range": (60, 180)}
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Navratna": {
                "products": [
                    {"name": "Cooling Oil", "sizes": ["100ml", "200ml", "500ml"], "sku_prefix": "OIL", "price_range": (50, 300)}
                ],
                "flavours": None,
                "uom": "ML"
            }
        },
        "Face Wash": {
            "Garnier": {
                "products": [
                    {"name": "Men Face Wash", "sizes": ["50g", "100g"], "sku_prefix": "FSH", "price_range": (70, 160)},
                    {"name": "Bright Complete", "sizes": ["50g", "100g"], "sku_prefix": "FSH", "price_range": (80, 180)}
                ],
                "flavours": ["Charcoal", "Vitamin C", "Neem"],
                "flavour_probability": 0.85,
                "uom": "ML"
            },
            "Himalaya": {
                "products": [
                    {"name": "Purifying Neem Face Wash", "sizes": ["50ml", "100ml", "150ml"], "sku_prefix": "FSH", "price_range": (60, 200)}
                ],
                "flavours": ["Neem", "Turmeric", "Cucumber"],
                "flavour_probability": 0.75,
                "uom": "ML"
            }
        }
    },
    "Home Care": {
        "Detergent": {
            "Surf Excel": {
                "products": [
                    {"name": "Matic Detergent", "sizes": ["500g", "1kg", "2kg", "4kg"], "sku_prefix": "DET", "price_range": (80, 800)},
                    {"name": "Easy Wash", "sizes": ["500g", "1kg", "2kg"], "sku_prefix": "DET", "price_range": (60, 450)}
                ],
                "flavours": ["Matic Top Load", "Matic Front Load", "Regular"],
                "flavour_probability": 0.70,
                "uom": "KG"
            },
            "Ariel": {
                "products": [
                    {"name": "Matic Detergent", "sizes": ["500g", "1kg", "2kg"], "sku_prefix": "DET", "price_range": (90, 500)}
                ],
                "flavours": ["Matic", "Original"],
                "flavour_probability": 0.60,
                "uom": "KG"
            },
            "Tide": {
                "products": [
                    {"name": "Detergent Powder", "sizes": ["500g", "1kg", "2kg", "4kg"], "sku_prefix": "DET", "price_range": (70, 700)}
                ],
                "flavours": ["Plus", "Naturals", "White"],
                "flavour_probability": 0.65,
                "uom": "KG"
            },
            "Wheel": {
                "products": [
                    {"name": "Washing Powder", "sizes": ["500g", "1kg", "2kg", "5kg"], "sku_prefix": "DET", "price_range": (40, 500)}
                ],
                "flavours": None,
                "uom": "KG"
            }
        },
        "Dishwash": {
            "Vim": {
                "products": [
                    {"name": "Dishwash Bar", "sizes": ["200g", "300g", "500g"], "sku_prefix": "DSH", "price_range": (20, 95)},
                    {"name": "Dishwash Gel", "sizes": ["500ml", "1L", "2L"], "sku_prefix": "DSH", "price_range": (80, 350)}
                ],
                "flavours": ["Lemon", "Pudina", "Active Gel"],
                "flavour_probability": 0.70,
                "uom": "LTR"
            },
            "Pril": {
                "products": [
                    {"name": "Dishwash Liquid", "sizes": ["500ml", "1L"], "sku_prefix": "DSH", "price_range": (90, 200)}
                ],
                "flavours": ["Lemon", "Lime"],
                "flavour_probability": 0.60,
                "uom": "LTR"
            }
        },
        "Floor Cleaner": {
            "Lizol": {
                "products": [
                    {"name": "Disinfectant Floor Cleaner", "sizes": ["500ml", "975ml", "2L"], "sku_prefix": "FLR", "price_range": (90, 450)}
                ],
                "flavours": ["Jasmine", "Citrus", "Lavender", "Floral"],
                "flavour_probability": 0.85,
                "uom": "LTR"
            },
            "Dettol": {
                "products": [
                    {"name": "Floor Cleaner", "sizes": ["500ml", "1L"], "sku_prefix": "FLR", "price_range": (100, 250)}
                ],
                "flavours": ["Lemon", "Pine", "Lavender"],
                "flavour_probability": 0.80,
                "uom": "LTR"
            }
        },
        "Toilet Cleaner": {
            "Harpic": {
                "products": [
                    {"name": "Power Plus", "sizes": ["500ml", "1L"], "sku_prefix": "TLT", "price_range": (100, 220)},
                    {"name": "Bathroom Cleaner", "sizes": ["500ml"], "sku_prefix": "TLT", "price_range": (110, 130)}
                ],
                "flavours": ["Original", "Lemon", "Floral"],
                "flavour_probability": 0.60,
                "uom": "LTR"
            },
            "Domex": {
                "products": [
                    {"name": "Toilet Cleaner", "sizes": ["500ml", "1L"], "sku_prefix": "TLT", "price_range": (90, 200)}
                ],
                "flavours": ["Fresh Guard", "Lime"],
                "flavour_probability": 0.55,
                "uom": "LTR"
            }
        }
    },
    "Baby Care": {
        "Baby Soap": {
            "Johnson & Johnson": {
                "products": [
                    {"name": "Baby Soap", "sizes": ["75g", "100g", "125g"], "sku_prefix": "BAB", "price_range": (35, 110)}
                ],
                "flavours": ["Milk", "Honey", "Blossoms"],
                "flavour_probability": 0.60,
                "uom": "G"
            },
            "Himalaya": {
                "products": [
                    {"name": "Baby Bathing Bar", "sizes": ["75g", "125g"], "sku_prefix": "BAB", "price_range": (30, 100)}
                ],
                "flavours": ["Milk & Honey", "Khus & Almond"],
                "flavour_probability": 0.65,
                "uom": "G"
            }
        },
        "Baby Powder": {
            "Johnson & Johnson": {
                "products": [
                    {"name": "Baby Powder", "sizes": ["100g", "200g", "400g"], "sku_prefix": "BAB", "price_range": (60, 280)}
                ],
                "flavours": ["Classic", "Blossoms"],
                "flavour_probability": 0.45,
                "uom": "G"
            },
            "Himalaya": {
                "products": [
                    {"name": "Powder", "sizes": ["100g", "200g", "400g"], "sku_prefix": "BAB", "price_range": (50, 250)}
                ],
                "flavours": None,
                "uom": "G"
            }
        },
        "Baby Wipes": {
            "Pampers": {
                "products": [
                    {"name": "Baby Wipes", "sizes": ["20pcs", "64pcs", "144pcs"], "sku_prefix": "BAB", "price_range": (50, 350)}
                ],
                "flavours": ["Aloe Vera", "Fresh Clean"],
                "flavour_probability": 0.55,
                "uom": "PCS"
            },
            "Himalaya": {
                "products": [
                    {"name": "Gentle Baby Wipes", "sizes": ["20pcs", "72pcs"], "sku_prefix": "BAB", "price_range": (45, 280)}
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "Huggies": {
                "products": [
                    {"name": "Baby Wipes", "sizes": ["20pcs", "80pcs"], "sku_prefix": "BAB", "price_range": (55, 320)}
                ],
                "flavours": ["Cucumber & Aloe", "Fragrance Free"],
                "flavour_probability": 0.50,
                "uom": "PCS"
            }
        },
        "Diapers": {
            "Pampers": {
                "products": [
                    {"name": "Baby Dry Pants", "sizes": ["Small", "Medium", "Large", "XL", "XXL"], "sku_prefix": "BAB", "price_range": (299, 1599)},
                    {"name": "Active Baby", "sizes": ["Small", "Medium", "Large", "XL"], "sku_prefix": "BAB", "price_range": (249, 1399)}
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "Huggies": {
                "products": [
                    {"name": "Wonder Pants", "sizes": ["Small", "Medium", "Large", "XL"], "sku_prefix": "BAB", "price_range": (279, 1499)}
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "MamyPoko": {
                "products": [
                    {"name": "Pants Diapers", "sizes": ["Small", "Medium", "Large", "XL", "XXL"], "sku_prefix": "BAB", "price_range": (249, 1399)}
                ],
                "flavours": None,
                "uom": "PCS"
            }
>>>>>>> etl-update
        }
    }
}

# ========================================
<<<<<<< HEAD
# GENERATE 10 NEW PRODUCTS
=======
# BUSINESS LOGIC STAGES
# ========================================
BUSINESS_STAGES = ["ACTIVE", "LOW_STOCK", "PROMOTIONAL", "SEASONAL", "NEW_LAUNCH", "CLEARANCE"]
STAGE_PROBABILITIES = {
    "ACTIVE": 0.75,          # 75% products are active
    "PROMOTIONAL": 0.10,     # 10% on promotion
    "NEW_LAUNCH": 0.08,      # 8% new launches
    "SEASONAL": 0.04,        # 4% seasonal
    "LOW_STOCK": 0.02,       # 2% being phased out
    "CLEARANCE": 0.01        # 1% clearance
}

def get_business_stage():
    """Assign business stage based on realistic distribution"""
    rand = random.random()
    cumulative = 0
    for stage, prob in STAGE_PROBABILITIES.items():
        cumulative += prob
        if rand <= cumulative:
            return stage
    return "ACTIVE"

# ========================================
# GET NEXT PRODUCT ID AND SKU COUNTER
# ========================================
def get_next_id():
    """Get the next product_id from the database"""
    try:
        cursor.execute("SELECT NVL(MAX(product_id), 0) + 1 FROM dim_product")
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        print(f"❌ Error getting next product_id: {e}")
        return 1

def get_next_sku_counter():
    """Extract the highest SKU counter from existing SKUs"""
    try:
        cursor.execute("SELECT sku FROM dim_product WHERE sku IS NOT NULL")
        skus = cursor.fetchall()
        max_counter = 0
        for (sku,) in skus:
            if sku and '-' in sku:
                parts = sku.split('-')
                if len(parts) >= 4 and parts[-1].isdigit():
                    counter = int(parts[-1])
                    max_counter = max(max_counter, counter)
        return max_counter + 1
    except Exception as e:
        print(f"⚠️  Could not get SKU counter: {e}, starting from 1")
        return 1

# ========================================
# GENERATE INTELLIGENT PRODUCT SKU
# ========================================
def generate_sku(category, subcategory, brand, product_name, sku_prefix, counter):
    """
    Generate intelligent SKU following pattern:
    PREFIX-BRAND_CODE-SUBCATEGORY_CODE-COUNTER
    Example: OIL-FOR-SUN-00123
    """
    brand_code = ''.join([c for c in brand.upper() if c.isalpha()])[:3]
    subcat_code = ''.join([c for c in subcategory.upper() if c.isalpha()])[:3]
    return f"{sku_prefix}-{brand_code}-{subcat_code}-{counter:05d}"

# ========================================
# INTELLIGENT FLAVOUR ASSIGNMENT
# ========================================
def get_flavour(brand_details):
    """Apply real-world retail logic for flavour assignment"""
    if brand_details["flavours"] is None:
        return None
    
    flavour_prob = brand_details.get("flavour_probability", 0.5)
    
    if random.random() < flavour_prob:
        return random.choice(brand_details["flavours"])
    return None

# ========================================
# SQL INSERT STATEMENT
>>>>>>> etl-update
# ========================================
insert_sql = """
INSERT INTO dim_product (
    product_id, product_name, category, sub_category,
<<<<<<< HEAD
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
=======
    brand, flavour, product_size, sku, uom, unit_price, business_stage
) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
"""

# ========================================
# GENERATE 10 NEW PRODUCTS DAILY
# ========================================
DAILY_INCREMENT = 10

# Get starting IDs
next_product_id = get_next_id()
next_sku_counter = get_next_sku_counter()

print(f"⏳ Generating {DAILY_INCREMENT} new products...\n")
print(f"📊 Starting Product ID: {next_product_id}")
print(f"📊 Starting SKU Counter: {next_sku_counter}\n")

data = []
pid = next_product_id
sku_counter = next_sku_counter

# Category selection with weights for daily increments
category_weights = {
    "Grocery": 0.40,       # 40%
    "Beverage": 0.20,      # 20%
    "Dairy": 0.12,         # 12%
    "Personal Care": 0.15, # 15%
    "Home Care": 0.10,     # 10%
    "Baby Care": 0.03      # 3%
}

# Generate 10 products
for i in range(DAILY_INCREMENT):
    # Select category based on weights
    category = random.choices(
        list(category_weights.keys()),
        weights=list(category_weights.values())
    )[0]
    
    subcategories = PRODUCTS[category]
    subcategory = random.choice(list(subcategories.keys()))
    brands = subcategories[subcategory]
    
    # Select brand
    brand_name = random.choice(list(brands.keys()))
    brand_details = brands[brand_name]
    
    # Select product from this brand
    product_info = random.choice(brand_details["products"])
    product_name = product_info["name"]
    size = random.choice(product_info["sizes"])
    min_price, max_price = product_info["price_range"]
    price = round(random.uniform(min_price, max_price), 2)
    
    # Generate SKU
    sku = generate_sku(category, subcategory, brand_name, product_name, 
                     product_info["sku_prefix"], sku_counter)
    sku_counter += 1
    
    # Get flavour
    flavour = get_flavour(brand_details)
    
    # Get business stage
    business_stage = get_business_stage()
    
    # Get UOM
    uom = brand_details["uom"]
    
    data.append((
        pid,
        product_name,
        category,
        subcategory,
        brand_name[:50],
        flavour,
        size,
        sku,
        uom,
        price,
        business_stage
    ))
    
    pid += 1

# ========================================
# INSERT INTO DATABASE
# ========================================
print(f"⏳ Inserting {DAILY_INCREMENT} products into DIM_PRODUCT...\n")

try:
    cursor.executemany(insert_sql, data)
    connection.commit()
    print(f"✅ {len(data)} rows successfully inserted into DIM_PRODUCT\n")
except Exception as e:
    print(f"❌ Error during insert: {e}")
    connection.rollback()
    cursor.close()
    connection.close()
    exit(1)

# ========================================
# DISPLAY INSERTED PRODUCTS
# ========================================
print(f"{'='*70}")
print(f"📈 Daily Auto-Increment Summary:")
print(f"{'='*70}\n")

print(f"📦 Products Added Today: {len(data)}")
print(f"🔢 Product ID Range: {next_product_id} - {pid - 1}")
print(f"🔖 SKU Counter Range: {next_sku_counter} - {sku_counter - 1}\n")

print(f"{'='*70}")
print(f"🆕 Newly Added Products:")
print(f"{'='*70}\n")

for idx, d in enumerate(data, 1):
    product_id, product_name, category, subcategory, brand, flavour, size, sku, uom, price, stage = d
    flavour_str = f" - {flavour}" if flavour else ""
    print(f"{idx:2d}. [{sku}] {brand} {product_name}{flavour_str} ({size})")
    print(f"    Category: {category} → {subcategory} | ₹{price} | {stage}")
    print()

# ========================================
# CATEGORY BREAKDOWN
# ========================================
print(f"{'='*70}")
print(f"📊 Today's Category Distribution:")
print(f"{'='*70}\n")

category_counts = {}
for d in data:
    cat = d[2]
    category_counts[cat] = category_counts.get(cat, 0) + 1

for cat in sorted(category_counts.keys()):
    count = category_counts[cat]
    print(f"   {cat:.<25} {count:>2} products ({count/len(data)*100:.0f}%)")

# ========================================
# BUSINESS STAGE BREAKDOWN
# ========================================
print(f"\n{'='*70}")
print(f"📊 Business Stage Distribution:")
print(f"{'='*70}\n")

stage_counts = {}
for d in data:
    stage = d[10]
    stage_counts[stage] = stage_counts.get(stage, 0) + 1

for stage in ["ACTIVE", "PROMOTIONAL", "NEW_LAUNCH", "SEASONAL", "LOW_STOCK", "CLEARANCE"]:
    count = stage_counts.get(stage, 0)
    if count > 0:
        print(f"   {stage:.<20} {count:>2} products")

# Get total count in database
cursor.execute("SELECT COUNT(*) FROM dim_product")
total_count = cursor.fetchone()[0]

cursor.close()
connection.close()

print(f"\n{'='*70}")
print(f"🎉 DAILY AUTO-INCREMENT COMPLETED SUCCESSFULLY")
print(f"{'='*70}")
print(f"\n📊 Database Statistics:")
print(f"   Total Products in DIM_PRODUCT: {total_count:,}")
print(f"   Products Added Today: {len(data)}")
print(f"   Next Product ID: {pid}")
print(f"   Next SKU Counter: {sku_counter}")
print(f"\n💡 Schedule this script to run daily via cron or task scheduler!")
print(f"   Example cron: 0 1 * * * /usr/bin/python3 /path/to/dim_product_daily.py")
print(f"\n{'='*70}")
>>>>>>> etl-update

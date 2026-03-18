from faker import Faker
import oracledb
import random
from datetime import datetime

fake = Faker('en_IN')

connection = oracledb.connect(
    user="system",
    password="oracle123",
    dsn="host.docker.internal/orcl"
)
cursor = connection.cursor()

# ========================================
# INDIAN RETAIL PRODUCT CATALOG
# Structure: Category → Subcategory → Brand → {products, flavours, uom}
#
# FIXES APPLIED:
#   1. SKU prefix 'OIL' collision: Grocery>Oils kept as 'OIL',
#      Personal Care>Hair Oil changed to 'HOL'
#   2. Baby Care prefixes split: SOB, POW, WIP, DIA instead of all 'BAB'
#   3. Biscuit flavours moved to per-product level to avoid wrong mappings
#      (e.g., Parle-G can no longer get 'Bourbon' or 'Milano')
#   4. Maggi: noodle variants are separate products, not flavours
#      (avoids 'Maggi Masala Masala' redundancy)
# ========================================

PRODUCTS = {
    "Grocery": {
        "Oils": {
            "Fortune": {
                "products": [
                    {"name": "Sunflower Oil",  "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (120, 650)},
                    {"name": "Refined Oil",    "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (110, 600)},
                    {"name": "Rice Bran Oil",  "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (130, 680)},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Sundrop": {
                "products": [
                    {"name": "Sunflower Oil", "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (115, 640)},
                    {"name": "Heart Oil",     "sizes": ["500ml", "1L", "2L"],        "sku_prefix": "OIL", "price_range": (140, 550)},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Saffola": {
                "products": [
                    {"name": "Gold Oil",   "sizes": ["1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (160, 850)},
                    {"name": "Active Oil", "sizes": ["1L", "2L"],       "sku_prefix": "OIL", "price_range": (180, 700)},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Dhara": {
                "products": [
                    {"name": "Mustard Oil",   "sizes": ["500ml", "1L", "2L", "5L"], "sku_prefix": "OIL", "price_range": (100, 580)},
                    {"name": "Groundnut Oil", "sizes": ["500ml", "1L", "2L"],        "sku_prefix": "OIL", "price_range": (120, 600)},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
        "Rice": {
            "India Gate": {
                "products": [
                    {"name": "Basmati Rice",    "sizes": ["1kg", "5kg", "10kg", "25kg"], "sku_prefix": "RIC", "price_range": (120, 1200)},
                    {"name": "Classic Basmati", "sizes": ["1kg", "5kg", "10kg"],         "sku_prefix": "RIC", "price_range": (100, 950)},
                    {"name": "Dubar Basmati",   "sizes": ["5kg", "10kg"],                "sku_prefix": "RIC", "price_range": (500, 1100)},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Daawat": {
                "products": [
                    {"name": "Rozana Basmati",      "sizes": ["1kg", "5kg"],         "sku_prefix": "RIC", "price_range": (90, 480)},
                    {"name": "Traditional Basmati", "sizes": ["1kg", "5kg", "10kg"], "sku_prefix": "RIC", "price_range": (110, 1050)},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Kohinoor": {
                "products": [
                    {"name": "Super Basmati",   "sizes": ["1kg", "5kg"],        "sku_prefix": "RIC", "price_range": (130, 680)},
                    {"name": "Classic Basmati", "sizes": ["5kg", "10kg"],       "sku_prefix": "RIC", "price_range": (450, 980)},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Fortune": {
                "products": [
                    {"name": "Sona Masoori Rice", "sizes": ["1kg", "5kg", "10kg", "25kg"], "sku_prefix": "RIC", "price_range": (50, 700)},
                    {"name": "Biryani Rice",      "sizes": ["1kg", "5kg"],                 "sku_prefix": "RIC", "price_range": (80, 420)},
                ],
                "flavours": None,
                "uom": "KG"
            },
        },
        "Atta": {
            "Aashirvaad": {
                "products": [
                    {"name": "Chakki Atta",    "sizes": ["1kg", "2kg", "5kg", "10kg"], "sku_prefix": "ATT", "price_range": (45, 380)},
                    {"name": "Multigrain Atta","sizes": ["1kg", "5kg"],                "sku_prefix": "ATT", "price_range": (60, 320)},
                    {"name": "Select Atta",    "sizes": ["5kg", "10kg"],               "sku_prefix": "ATT", "price_range": (240, 520)},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Pillsbury": {
                "products": [
                    {"name": "Chakki Atta",    "sizes": ["1kg", "5kg", "10kg"], "sku_prefix": "ATT", "price_range": (42, 370)},
                    {"name": "Multigrain Atta","sizes": ["1kg", "5kg"],         "sku_prefix": "ATT", "price_range": (55, 310)},
                ],
                "flavours": None,
                "uom": "KG"
            },
        },
        "Pulses": {
            "Tata Sampann": {
                "products": [
                    {"name": "Toor Dal",  "sizes": ["500g", "1kg", "2kg"], "sku_prefix": "DAL", "price_range": (90, 280)},
                    {"name": "Moong Dal", "sizes": ["500g", "1kg"],        "sku_prefix": "DAL", "price_range": (85, 195)},
                    {"name": "Urad Dal",  "sizes": ["500g", "1kg"],        "sku_prefix": "DAL", "price_range": (95, 210)},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Fortune": {
                "products": [
                    {"name": "Chana Dal",  "sizes": ["500g", "1kg", "2kg"], "sku_prefix": "DAL", "price_range": (80, 240)},
                    {"name": "Masoor Dal", "sizes": ["500g", "1kg"],        "sku_prefix": "DAL", "price_range": (75, 180)},
                ],
                "flavours": None,
                "uom": "KG"
            },
        },
        "Spices": {
            "MDH": {
                "products": [
                    {"name": "Turmeric Powder",  "sizes": ["50g", "100g", "200g", "500g"], "sku_prefix": "SPC", "price_range": (15, 120)},
                    {"name": "Red Chilli Powder","sizes": ["50g", "100g", "200g", "500g"], "sku_prefix": "SPC", "price_range": (20, 130)},
                    {"name": "Garam Masala",     "sizes": ["50g", "100g", "200g"],         "sku_prefix": "SPC", "price_range": (25, 110)},
                    {"name": "Coriander Powder", "sizes": ["50g", "100g", "200g"],         "sku_prefix": "SPC", "price_range": (18, 95)},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Everest": {
                "products": [
                    {"name": "Turmeric Powder","sizes": ["50g", "100g", "200g", "500g"], "sku_prefix": "SPC", "price_range": (16, 125)},
                    {"name": "Chilli Powder",  "sizes": ["50g", "100g", "200g"],         "sku_prefix": "SPC", "price_range": (22, 115)},
                    {"name": "Pav Bhaji Masala","sizes": ["50g", "100g"],                "sku_prefix": "SPC", "price_range": (28, 85)},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Catch": {
                "products": [
                    {"name": "Chat Masala", "sizes": ["50g", "100g"],         "sku_prefix": "SPC", "price_range": (30, 90)},
                    {"name": "Garam Masala","sizes": ["50g", "100g", "200g"], "sku_prefix": "SPC", "price_range": (26, 105)},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Biscuits": {
            # FIX: Flavours are now defined per-product so wrong variants
            # (e.g., Parle-G getting 'Bourbon') are impossible.
            "Parle": {
                "products": [
                    {"name": "Parle-G",     "sizes": ["75g","150g","300g","500g","1kg"], "sku_prefix": "BIS", "price_range": (10, 180),
                     "flavours": None},
                    {"name": "Monaco",      "sizes": ["75g","150g","300g"],              "sku_prefix": "BIS", "price_range": (12, 85),
                     "flavours": None},
                    {"name": "Hide & Seek", "sizes": ["100g","200g","400g"],             "sku_prefix": "BIS", "price_range": (20, 140),
                     "flavours": ["Chocolate Chip", "Coffee", "Bourbon"], "flavour_probability": 0.70},
                ],
                "flavours": None,  # brand-level fallback (unused when product-level is set)
                "uom": "G"
            },
            "Britannia": {
                "products": [
                    {"name": "Good Day",    "sizes": ["100g","200g","400g","600g"], "sku_prefix": "BIS", "price_range": (18, 150),
                     "flavours": ["Butter", "Cashew", "Coconut"], "flavour_probability": 0.55},
                    {"name": "Marie Gold",  "sizes": ["75g","150g","250g"],         "sku_prefix": "BIS", "price_range": (12, 70),
                     "flavours": None},
                    {"name": "NutriChoice", "sizes": ["100g","200g"],               "sku_prefix": "BIS", "price_range": (25, 85),
                     "flavours": ["Ragi", "5 Grain"], "flavour_probability": 0.60},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Sunfeast": {
                "products": [
                    {"name": "Dark Fantasy","sizes": ["75g","150g","300g"], "sku_prefix": "BIS", "price_range": (30, 130),
                     "flavours": ["Choco Fills", "Vanilla"], "flavour_probability": 0.60},
                    {"name": "Marie Light", "sizes": ["75g","150g"],        "sku_prefix": "BIS", "price_range": (15, 55),
                     "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Noodles": {
            # FIX: Maggi variants are separate products (not flavours of one product)
            # to avoid 'Maggi Masala - Masala' redundancy.
            "Maggi": {
                "products": [
                    {"name": "Masala Noodles",      "sizes": ["70g","140g","280g","560g"], "sku_prefix": "NOD", "price_range": (12, 150), "flavours": None},
                    {"name": "Atta Noodles",        "sizes": ["70g","280g"],               "sku_prefix": "NOD", "price_range": (15, 125), "flavours": None},
                    {"name": "Cuppa Mania",         "sizes": ["70g"],                      "sku_prefix": "NOD", "price_range": (35, 45),
                     "flavours": ["Masala Yo!", "Chilli Chow"], "flavour_probability": 0.90},
                    {"name": "Chicken Noodles",     "sizes": ["70g","280g"],               "sku_prefix": "NOD", "price_range": (15, 130), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Top Ramen": {
                "products": [
                    {"name": "Curry Noodles", "sizes": ["70g","280g"], "sku_prefix": "NOD", "price_range": (12, 120),
                     "flavours": ["Curry", "Masala"], "flavour_probability": 0.85},
                    {"name": "Cup Noodles",   "sizes": ["70g"],        "sku_prefix": "NOD", "price_range": (30, 40),
                     "flavours": ["Chicken", "Masala"], "flavour_probability": 0.95},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Yippee": {
                "products": [
                    {"name": "Magic Masala Noodles", "sizes": ["70g","240g"], "sku_prefix": "NOD", "price_range": (10, 95), "flavours": None},
                    {"name": "Mood Masala Noodles",  "sizes": ["70g"],        "sku_prefix": "NOD", "price_range": (10, 15), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Snacks": {
            "Haldiram's": {
                "products": [
                    {"name": "Aloo Bhujia",  "sizes": ["50g","150g","200g","400g","1kg"], "sku_prefix": "SNK", "price_range": (10, 280), "flavours": None},
                    {"name": "Moong Dal",    "sizes": ["50g","150g","200g"],              "sku_prefix": "SNK", "price_range": (12, 110), "flavours": None},
                    {"name": "Namkeen Mix",  "sizes": ["150g","200g","400g"],             "sku_prefix": "SNK", "price_range": (30, 180), "flavours": None},
                    {"name": "Khatta Meetha","sizes": ["150g","200g","400g"],             "sku_prefix": "SNK", "price_range": (35, 190), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Bikaji": {
                "products": [
                    {"name": "Bhujia", "sizes": ["50g","150g","200g","500g"], "sku_prefix": "SNK", "price_range": (10, 180), "flavours": None},
                    {"name": "Sev",    "sizes": ["50g","150g","200g"],        "sku_prefix": "SNK", "price_range": (15, 95),
                     "flavours": ["Plain", "Ratlami"], "flavour_probability": 0.55},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Lay's": {
                "products": [
                    {"name": "Chips", "sizes": ["25g","50g","90g","150g"], "sku_prefix": "SNK", "price_range": (10, 90),
                     "flavours": ["Classic Salted", "Magic Masala", "Cream & Onion", "Spanish Tomato Tango"],
                     "flavour_probability": 0.95},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Uncle Chipps": {
                "products": [
                    {"name": "Chips", "sizes": ["25g","55g","120g"], "sku_prefix": "SNK", "price_range": (10, 60),
                     "flavours": ["Spicy Treat", "Salted"], "flavour_probability": 0.85},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Kurkure": {
                "products": [
                    {"name": "Namkeen", "sizes": ["40g","90g","140g"], "sku_prefix": "SNK", "price_range": (10, 70),
                     "flavours": ["Masala Munch", "Green Chutney", "Chilli Chatka", "Solid Masti"],
                     "flavour_probability": 0.90},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Bingo": {
                "products": [
                    {"name": "Mad Angles",   "sizes": ["25g","50g","100g"], "sku_prefix": "SNK", "price_range": (10, 55), "flavours": None},
                    {"name": "Tedhe Medhe",  "sizes": ["25g","50g"],        "sku_prefix": "SNK", "price_range": (10, 40), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
    },
    "Beverage": {
        "Soft Drink": {
            "Coca Cola": {
                "products": [
                    {"name": "Coca Cola", "sizes": ["200ml","300ml","500ml","750ml","1L","1.25L","2L"], "sku_prefix": "SDR", "price_range": (20, 90),
                     "flavours": ["Regular", "Diet", "Zero Sugar"], "flavour_probability": 0.40},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Thums Up": {
                "products": [
                    {"name": "Thums Up", "sizes": ["200ml","300ml","500ml","750ml","1.25L","2L"], "sku_prefix": "SDR", "price_range": (20, 90),
                     "flavours": ["Regular", "Charged"], "flavour_probability": 0.30},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Pepsi": {
                "products": [
                    {"name": "Pepsi", "sizes": ["200ml","300ml","500ml","1L","1.25L","2L"], "sku_prefix": "SDR", "price_range": (20, 85),
                     "flavours": ["Regular", "Black", "Diet"], "flavour_probability": 0.35},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Sprite": {
                "products": [
                    {"name": "Sprite", "sizes": ["200ml","300ml","500ml","750ml","1.25L","2L"], "sku_prefix": "SDR", "price_range": (20, 85), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Fanta": {
                "products": [
                    {"name": "Fanta Orange","sizes": ["200ml","300ml","500ml","1.25L","2L"], "sku_prefix": "SDR", "price_range": (20, 85), "flavours": None},
                    {"name": "Fanta Apple", "sizes": ["200ml","500ml"],                     "sku_prefix": "SDR", "price_range": (20, 55), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Mountain Dew": {
                "products": [
                    {"name": "Mountain Dew", "sizes": ["200ml","500ml","750ml","1.25L"], "sku_prefix": "SDR", "price_range": (20, 80), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
        "Juice": {
            "Tropicana": {
                "products": [
                    {"name": "Mango Juice",       "sizes": ["200ml","500ml","1L"], "sku_prefix": "JUC", "price_range": (30, 150), "flavours": None},
                    {"name": "Orange Juice",      "sizes": ["200ml","500ml","1L"], "sku_prefix": "JUC", "price_range": (30, 150), "flavours": None},
                    {"name": "Apple Juice",       "sizes": ["200ml","500ml","1L"], "sku_prefix": "JUC", "price_range": (30, 150), "flavours": None},
                    {"name": "Mixed Fruit Juice", "sizes": ["200ml","1L"],         "sku_prefix": "JUC", "price_range": (30, 130), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Real": {
                "products": [
                    {"name": "Mango Juice",       "sizes": ["200ml","1L"], "sku_prefix": "JUC", "price_range": (25, 130), "flavours": None},
                    {"name": "Orange Juice",      "sizes": ["200ml","1L"], "sku_prefix": "JUC", "price_range": (25, 130), "flavours": None},
                    {"name": "Mixed Fruit Juice", "sizes": ["200ml","1L"], "sku_prefix": "JUC", "price_range": (25, 120), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Maaza": {
                "products": [
                    {"name": "Mango Drink", "sizes": ["200ml","600ml","1.2L"], "sku_prefix": "JUC", "price_range": (20, 100), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Paper Boat": {
                "products": [
                    {"name": "Aamras",            "sizes": ["250ml"], "sku_prefix": "JUC", "price_range": (30, 40), "flavours": None},
                    {"name": "Jaljeera",          "sizes": ["250ml"], "sku_prefix": "JUC", "price_range": (30, 40), "flavours": None},
                    {"name": "Aam Panna",         "sizes": ["250ml"], "sku_prefix": "JUC", "price_range": (30, 40), "flavours": None},
                    {"name": "Jamun Kala Khatta", "sizes": ["250ml"], "sku_prefix": "JUC", "price_range": (30, 40), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
        "Tea": {
            "Tata Tea": {
                "products": [
                    {"name": "Premium Tea","sizes": ["100g","250g","500g","1kg"], "sku_prefix": "TEA", "price_range": (50, 360), "flavours": None},
                    {"name": "Gold Tea",   "sizes": ["100g","250g","500g"],       "sku_prefix": "TEA", "price_range": (60, 320), "flavours": None},
                    {"name": "Elaichi Tea","sizes": ["100g","250g","500g"],       "sku_prefix": "TEA", "price_range": (55, 310), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Red Label": {
                "products": [
                    {"name": "Natural Care Tea", "sizes": ["100g","250g","500g","1kg"], "sku_prefix": "TEA", "price_range": (45, 340), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Taj Mahal": {
                "products": [
                    {"name": "Premium Tea", "sizes": ["100g","250g","500g"], "sku_prefix": "TEA", "price_range": (65, 350), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Lipton": {
                "products": [
                    {"name": "Green Tea Lemon",  "sizes": ["25 Bags","100 Bags"], "sku_prefix": "TEA", "price_range": (80, 420), "flavours": None},
                    {"name": "Green Tea Tulsi",  "sizes": ["25 Bags","100 Bags"], "sku_prefix": "TEA", "price_range": (80, 420), "flavours": None},
                    {"name": "Green Tea Honey",  "sizes": ["25 Bags"],            "sku_prefix": "TEA", "price_range": (90, 105), "flavours": None},
                    {"name": "Green Tea Plain",  "sizes": ["25 Bags","100 Bags"], "sku_prefix": "TEA", "price_range": (80, 420), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
        },
        "Coffee": {
            "Nescafe": {
                "products": [
                    {"name": "Classic Coffee", "sizes": ["50g","100g","200g"], "sku_prefix": "COF", "price_range": (80, 450), "flavours": None},
                    {"name": "Gold Coffee",    "sizes": ["50g","100g"],        "sku_prefix": "COF", "price_range": (120, 380), "flavours": None},
                    {"name": "Sunrise Coffee", "sizes": ["50g","100g","200g"], "sku_prefix": "COF", "price_range": (70, 380), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Bru": {
                "products": [
                    {"name": "Instant Coffee","sizes": ["50g","100g","200g"], "sku_prefix": "COF", "price_range": (70, 400), "flavours": None},
                    {"name": "Gold Roast",    "sizes": ["50g","100g"],        "sku_prefix": "COF", "price_range": (100, 330), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Energy Drink": {
            "Red Bull": {
                "products": [
                    {"name": "Energy Drink", "sizes": ["250ml"], "sku_prefix": "ENR", "price_range": (115, 125), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Sting": {
                "products": [
                    {"name": "Energy Drink", "sizes": ["250ml","500ml"], "sku_prefix": "ENR", "price_range": (20, 40),
                     "flavours": ["Berry Blast", "Power Lime"], "flavour_probability": 0.60},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Gatorade": {
                "products": [
                    {"name": "Sports Drink", "sizes": ["500ml","1L"], "sku_prefix": "ENR", "price_range": (50, 100),
                     "flavours": ["Orange", "Lemon", "Cool Blue"], "flavour_probability": 0.85},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
    },
    "Dairy": {
        "Milk": {
            "Amul": {
                "products": [
                    {"name": "Full Cream Milk", "sizes": ["500ml","1L"], "sku_prefix": "MLK", "price_range": (30, 68), "flavours": None},
                    {"name": "Toned Milk",      "sizes": ["500ml","1L"], "sku_prefix": "MLK", "price_range": (25, 56), "flavours": None},
                    {"name": "Gold Milk",       "sizes": ["500ml","1L"], "sku_prefix": "MLK", "price_range": (32, 70), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Full Cream Milk", "sizes": ["500ml","1L"], "sku_prefix": "MLK", "price_range": (28, 66), "flavours": None},
                    {"name": "Toned Milk",      "sizes": ["500ml","1L"], "sku_prefix": "MLK", "price_range": (24, 54), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
        "Curd": {
            "Amul": {
                "products": [
                    {"name": "Fresh Curd",  "sizes": ["200g","400g","1kg"], "sku_prefix": "CRD", "price_range": (20, 85), "flavours": None},
                    {"name": "Masti Dahi",  "sizes": ["400g"],              "sku_prefix": "CRD", "price_range": (35, 45), "flavours": None},
                    {"name": "Mango Shrikhand","sizes": ["100g","200g"],    "sku_prefix": "CRD", "price_range": (40, 80), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Mishti Doi", "sizes": ["200g","400g","1kg"], "sku_prefix": "CRD", "price_range": (18, 80), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Cheese": {
            "Amul": {
                "products": [
                    {"name": "Processed Cheese","sizes": ["200g","400g","1kg"], "sku_prefix": "CHZ", "price_range": (80, 380), "flavours": None},
                    {"name": "Pizza Mozzarella", "sizes": ["200g"],             "sku_prefix": "CHZ", "price_range": (120, 140), "flavours": None},
                    {"name": "Cheese Slices",    "sizes": ["200g","400g"],      "sku_prefix": "CHZ", "price_range": (100, 220), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Britannia": {
                "products": [
                    {"name": "Cheese Slices","sizes": ["200g","400g"], "sku_prefix": "CHZ", "price_range": (95, 210), "flavours": None},
                    {"name": "Cheese Cubes", "sizes": ["200g"],        "sku_prefix": "CHZ", "price_range": (110, 125), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Butter": {
            "Amul": {
                "products": [
                    {"name": "Salted Butter",   "sizes": ["100g","500g"], "sku_prefix": "BUT", "price_range": (45, 270), "flavours": None},
                    {"name": "Unsalted Butter",  "sizes": ["100g","500g"], "sku_prefix": "BUT", "price_range": (48, 275), "flavours": None},
                    {"name": "Lite Butter",      "sizes": ["100g"],        "sku_prefix": "BUT", "price_range": (50, 55),  "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Salted Butter",   "sizes": ["100g","500g"], "sku_prefix": "BUT", "price_range": (42, 260), "flavours": None},
                    {"name": "Unsalted Butter",  "sizes": ["100g","500g"], "sku_prefix": "BUT", "price_range": (44, 265), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Paneer": {
            "Amul": {
                "products": [
                    {"name": "Fresh Paneer", "sizes": ["200g","500g","1kg"], "sku_prefix": "PNR", "price_range": (80, 420), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Mother Dairy": {
                "products": [
                    {"name": "Paneer", "sizes": ["200g","500g"], "sku_prefix": "PNR", "price_range": (75, 380), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
    },
    "Personal Care": {
        "Shampoo": {
            "Clinic Plus": {
                "products": [
                    {"name": "Strong & Long Shampoo","sizes": ["180ml","340ml","650ml"], "sku_prefix": "SHP", "price_range": (90, 380), "flavours": None},
                    {"name": "Natural Shine Shampoo","sizes": ["180ml","340ml"],         "sku_prefix": "SHP", "price_range": (90, 250), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Dove": {
                "products": [
                    {"name": "Intense Repair Shampoo", "sizes": ["180ml","340ml","650ml"], "sku_prefix": "SHP", "price_range": (110, 420), "flavours": None},
                    {"name": "Daily Shine Shampoo",    "sizes": ["180ml","340ml","650ml"], "sku_prefix": "SHP", "price_range": (110, 420), "flavours": None},
                    {"name": "Anti-Dandruff Shampoo",  "sizes": ["180ml","340ml"],         "sku_prefix": "SHP", "price_range": (120, 380), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Pantene": {
                "products": [
                    {"name": "Silky Smooth Shampoo",     "sizes": ["180ml","340ml","650ml"], "sku_prefix": "SHP", "price_range": (100, 400), "flavours": None},
                    {"name": "Hair Fall Control Shampoo","sizes": ["180ml","340ml","650ml"], "sku_prefix": "SHP", "price_range": (100, 400), "flavours": None},
                    {"name": "Total Damage Care Shampoo","sizes": ["180ml","340ml"],         "sku_prefix": "SHP", "price_range": (110, 360), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Head & Shoulders": {
                "products": [
                    {"name": "Cool Menthol Shampoo",     "sizes": ["180ml","340ml","650ml"], "sku_prefix": "SHP", "price_range": (120, 450), "flavours": None},
                    {"name": "Smooth & Silky Shampoo",   "sizes": ["180ml","340ml"],         "sku_prefix": "SHP", "price_range": (120, 380), "flavours": None},
                    {"name": "Clean & Balanced Shampoo", "sizes": ["180ml","340ml"],         "sku_prefix": "SHP", "price_range": (120, 380), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
        },
        "Soap": {
            "Lux": {
                "products": [
                    {"name": "Rose Beauty Soap",    "sizes": ["75g","100g","125g","150g"], "sku_prefix": "SOP", "price_range": (25, 135), "flavours": None},
                    {"name": "Jasmine Beauty Soap", "sizes": ["75g","100g","125g"],        "sku_prefix": "SOP", "price_range": (25, 100), "flavours": None},
                    {"name": "Saffron Beauty Soap", "sizes": ["100g","125g"],              "sku_prefix": "SOP", "price_range": (30, 80),  "flavours": None},
                    {"name": "Sandal Beauty Soap",  "sizes": ["100g","125g"],              "sku_prefix": "SOP", "price_range": (30, 80),  "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Dove": {
                "products": [
                    {"name": "Original Bathing Bar",          "sizes": ["75g","100g","125g"], "sku_prefix": "SOP", "price_range": (30, 140), "flavours": None},
                    {"name": "Go Fresh Bathing Bar",          "sizes": ["75g","100g"],        "sku_prefix": "SOP", "price_range": (32, 120), "flavours": None},
                    {"name": "Deeply Nourishing Bathing Bar", "sizes": ["75g","100g","125g"], "sku_prefix": "SOP", "price_range": (30, 140), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Lifebuoy": {
                "products": [
                    {"name": "Total Germ Protection Soap",  "sizes": ["75g","100g","125g"], "sku_prefix": "SOP", "price_range": (22, 120), "flavours": None},
                    {"name": "Nature Germ Protection Soap", "sizes": ["75g","100g"],        "sku_prefix": "SOP", "price_range": (22, 95),  "flavours": None},
                    {"name": "Care Germ Protection Soap",   "sizes": ["75g","100g"],        "sku_prefix": "SOP", "price_range": (22, 95),  "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Dettol": {
                "products": [
                    {"name": "Original Antiseptic Soap",  "sizes": ["75g","125g"], "sku_prefix": "SOP", "price_range": (35, 90), "flavours": None},
                    {"name": "Skincare Antiseptic Soap",  "sizes": ["75g","125g"], "sku_prefix": "SOP", "price_range": (35, 90), "flavours": None},
                    {"name": "Cool Antiseptic Soap",      "sizes": ["75g","125g"], "sku_prefix": "SOP", "price_range": (35, 90), "flavours": None},
                    {"name": "Fresh Antiseptic Soap",     "sizes": ["75g","125g"], "sku_prefix": "SOP", "price_range": (35, 90), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Toothpaste": {
            "Colgate": {
                "products": [
                    {"name": "Dental Cream",          "sizes": ["100g","200g"],   "sku_prefix": "TPS", "price_range": (50, 140), "flavours": None},
                    {"name": "Total Advanced",        "sizes": ["100g","150g"],   "sku_prefix": "TPS", "price_range": (70, 150), "flavours": None},
                    {"name": "MaxFresh Mint",         "sizes": ["80g","150g"],    "sku_prefix": "TPS", "price_range": (60, 130), "flavours": None},
                    {"name": "Charcoal Clean",        "sizes": ["100g"],          "sku_prefix": "TPS", "price_range": (100, 130), "flavours": None},
                    {"name": "Sensitive Relief",      "sizes": ["75g","100g"],    "sku_prefix": "TPS", "price_range": (110, 160), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Pepsodent": {
                "products": [
                    {"name": "Germi Check",     "sizes": ["100g","150g"], "sku_prefix": "TPS", "price_range": (45, 125), "flavours": None},
                    {"name": "Clove & Salt",    "sizes": ["100g","150g"], "sku_prefix": "TPS", "price_range": (50, 130), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Sensodyne": {
                "products": [
                    {"name": "Fresh Mint Sensitive",      "sizes": ["75g","100g"], "sku_prefix": "TPS", "price_range": (120, 180), "flavours": None},
                    {"name": "Repair & Protect Sensitive","sizes": ["75g","100g"], "sku_prefix": "TPS", "price_range": (130, 190), "flavours": None},
                    {"name": "Whitening Sensitive",       "sizes": ["75g"],        "sku_prefix": "TPS", "price_range": (130, 150), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Hair Oil": {
            # FIX: sku_prefix changed from 'OIL' to 'HOL' to avoid collision with Grocery>Oils
            "Parachute": {
                "products": [
                    {"name": "Coconut Oil",         "sizes": ["100ml","200ml","500ml","1L"], "sku_prefix": "HOL", "price_range": (35, 350), "flavours": None},
                    {"name": "Jasmine Coconut Oil", "sizes": ["100ml","200ml","500ml"],      "sku_prefix": "HOL", "price_range": (40, 320), "flavours": None},
                    {"name": "Cooling Coconut Oil", "sizes": ["100ml","200ml"],              "sku_prefix": "HOL", "price_range": (50, 180), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Dabur": {
                "products": [
                    {"name": "Amla Hair Oil",   "sizes": ["100ml","200ml","500ml"], "sku_prefix": "HOL", "price_range": (45, 280), "flavours": None},
                    {"name": "Almond Hair Oil", "sizes": ["100ml","200ml"],         "sku_prefix": "HOL", "price_range": (60, 180), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Navratna": {
                "products": [
                    {"name": "Cooling Hair Oil", "sizes": ["100ml","200ml","500ml"], "sku_prefix": "HOL", "price_range": (50, 300), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
        },
        "Face Wash": {
            "Garnier": {
                "products": [
                    {"name": "Men Charcoal Face Wash",  "sizes": ["50g","100g"], "sku_prefix": "FSH", "price_range": (70, 160), "flavours": None},
                    {"name": "Bright Complete Vitamin C","sizes": ["50g","100g"], "sku_prefix": "FSH", "price_range": (80, 180), "flavours": None},
                    {"name": "Neem Face Wash",           "sizes": ["50g","100g"], "sku_prefix": "FSH", "price_range": (70, 155), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
            "Himalaya": {
                "products": [
                    {"name": "Purifying Neem Face Wash",    "sizes": ["50ml","100ml","150ml"], "sku_prefix": "FSH", "price_range": (60, 200), "flavours": None},
                    {"name": "Turmeric Glow Face Wash",     "sizes": ["50ml","100ml"],         "sku_prefix": "FSH", "price_range": (65, 175), "flavours": None},
                    {"name": "Refreshing Cucumber Face Wash","sizes": ["50ml","100ml"],        "sku_prefix": "FSH", "price_range": (60, 170), "flavours": None},
                ],
                "flavours": None,
                "uom": "ML"
            },
        },
    },
    "Home Care": {
        "Detergent": {
            "Surf Excel": {
                "products": [
                    {"name": "Matic Top Load Detergent",   "sizes": ["500g","1kg","2kg","4kg"], "sku_prefix": "DET", "price_range": (80, 800), "flavours": None},
                    {"name": "Matic Front Load Detergent", "sizes": ["500g","1kg","2kg"],       "sku_prefix": "DET", "price_range": (85, 500), "flavours": None},
                    {"name": "Easy Wash Detergent",        "sizes": ["500g","1kg","2kg"],       "sku_prefix": "DET", "price_range": (60, 450), "flavours": None},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Ariel": {
                "products": [
                    {"name": "Matic Top Load Detergent",   "sizes": ["500g","1kg","2kg"], "sku_prefix": "DET", "price_range": (90, 500), "flavours": None},
                    {"name": "Matic Front Load Detergent", "sizes": ["500g","1kg","2kg"], "sku_prefix": "DET", "price_range": (95, 510), "flavours": None},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Tide": {
                "products": [
                    {"name": "Tide Plus Detergent",     "sizes": ["500g","1kg","2kg","4kg"], "sku_prefix": "DET", "price_range": (70, 700), "flavours": None},
                    {"name": "Tide Naturals Detergent", "sizes": ["500g","1kg"],             "sku_prefix": "DET", "price_range": (65, 300), "flavours": None},
                ],
                "flavours": None,
                "uom": "KG"
            },
            "Wheel": {
                "products": [
                    {"name": "Washing Powder", "sizes": ["500g","1kg","2kg","5kg"], "sku_prefix": "DET", "price_range": (40, 500), "flavours": None},
                ],
                "flavours": None,
                "uom": "KG"
            },
        },
        "Dishwash": {
            "Vim": {
                "products": [
                    {"name": "Lemon Dishwash Bar",  "sizes": ["200g","300g","500g"],  "sku_prefix": "DSH", "price_range": (20, 95),  "flavours": None},
                    {"name": "Pudina Dishwash Bar",  "sizes": ["200g","500g"],         "sku_prefix": "DSH", "price_range": (22, 90),  "flavours": None},
                    {"name": "Active Gel Dishwash",  "sizes": ["500ml","1L","2L"],     "sku_prefix": "DSH", "price_range": (80, 350), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Pril": {
                "products": [
                    {"name": "Lemon Dishwash Liquid", "sizes": ["500ml","1L"], "sku_prefix": "DSH", "price_range": (90, 200), "flavours": None},
                    {"name": "Lime Dishwash Liquid",  "sizes": ["500ml","1L"], "sku_prefix": "DSH", "price_range": (90, 200), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
        "Floor Cleaner": {
            "Lizol": {
                "products": [
                    {"name": "Jasmine Floor Cleaner", "sizes": ["500ml","975ml","2L"], "sku_prefix": "FLR", "price_range": (90, 450), "flavours": None},
                    {"name": "Citrus Floor Cleaner",  "sizes": ["500ml","975ml","2L"], "sku_prefix": "FLR", "price_range": (90, 450), "flavours": None},
                    {"name": "Lavender Floor Cleaner","sizes": ["500ml","975ml"],       "sku_prefix": "FLR", "price_range": (90, 320), "flavours": None},
                    {"name": "Floral Floor Cleaner",  "sizes": ["500ml","975ml"],       "sku_prefix": "FLR", "price_range": (90, 320), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Dettol": {
                "products": [
                    {"name": "Lemon Floor Cleaner",   "sizes": ["500ml","1L"], "sku_prefix": "FLR", "price_range": (100, 250), "flavours": None},
                    {"name": "Pine Floor Cleaner",    "sizes": ["500ml","1L"], "sku_prefix": "FLR", "price_range": (100, 250), "flavours": None},
                    {"name": "Lavender Floor Cleaner","sizes": ["500ml","1L"], "sku_prefix": "FLR", "price_range": (100, 250), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
        "Toilet Cleaner": {
            "Harpic": {
                "products": [
                    {"name": "Power Plus Original",  "sizes": ["500ml","1L"], "sku_prefix": "TLT", "price_range": (100, 220), "flavours": None},
                    {"name": "Power Plus Lemon",     "sizes": ["500ml","1L"], "sku_prefix": "TLT", "price_range": (100, 220), "flavours": None},
                    {"name": "Power Plus Floral",    "sizes": ["500ml"],      "sku_prefix": "TLT", "price_range": (110, 130), "flavours": None},
                    {"name": "Bathroom Cleaner",     "sizes": ["500ml"],      "sku_prefix": "TLT", "price_range": (110, 130), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
            "Domex": {
                "products": [
                    {"name": "Fresh Guard Toilet Cleaner","sizes": ["500ml","1L"], "sku_prefix": "TLT", "price_range": (90, 200), "flavours": None},
                    {"name": "Lime Toilet Cleaner",       "sizes": ["500ml","1L"], "sku_prefix": "TLT", "price_range": (90, 200), "flavours": None},
                ],
                "flavours": None,
                "uom": "LTR"
            },
        },
    },
    "Baby Care": {
        "Baby Soap": {
            "Johnson & Johnson": {
                "products": [
                    {"name": "Milk Baby Soap",     "sizes": ["75g","100g","125g"], "sku_prefix": "SOB", "price_range": (35, 110), "flavours": None},
                    {"name": "Honey Baby Soap",    "sizes": ["75g","100g"],        "sku_prefix": "SOB", "price_range": (35, 90),  "flavours": None},
                    {"name": "Blossoms Baby Soap", "sizes": ["75g","100g"],        "sku_prefix": "SOB", "price_range": (35, 90),  "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Himalaya": {
                "products": [
                    {"name": "Milk & Honey Baby Bathing Bar",  "sizes": ["75g","125g"], "sku_prefix": "SOB", "price_range": (30, 100), "flavours": None},
                    {"name": "Khus & Almond Baby Bathing Bar", "sizes": ["75g","125g"], "sku_prefix": "SOB", "price_range": (30, 100), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Baby Powder": {
            "Johnson & Johnson": {
                "products": [
                    {"name": "Classic Baby Powder",   "sizes": ["100g","200g","400g"], "sku_prefix": "POW", "price_range": (60, 280), "flavours": None},
                    {"name": "Blossoms Baby Powder",  "sizes": ["100g","200g"],        "sku_prefix": "POW", "price_range": (65, 200), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
            "Himalaya": {
                "products": [
                    {"name": "Baby Powder", "sizes": ["100g","200g","400g"], "sku_prefix": "POW", "price_range": (50, 250), "flavours": None},
                ],
                "flavours": None,
                "uom": "G"
            },
        },
        "Baby Wipes": {
            "Pampers": {
                "products": [
                    {"name": "Aloe Vera Baby Wipes",    "sizes": ["20pcs","64pcs","144pcs"], "sku_prefix": "WIP", "price_range": (50, 350), "flavours": None},
                    {"name": "Fresh Clean Baby Wipes",  "sizes": ["20pcs","64pcs"],          "sku_prefix": "WIP", "price_range": (50, 280), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "Himalaya": {
                "products": [
                    {"name": "Gentle Baby Wipes", "sizes": ["20pcs","72pcs"], "sku_prefix": "WIP", "price_range": (45, 280), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "Huggies": {
                "products": [
                    {"name": "Cucumber & Aloe Baby Wipes", "sizes": ["20pcs","80pcs"], "sku_prefix": "WIP", "price_range": (55, 320), "flavours": None},
                    {"name": "Fragrance Free Baby Wipes",  "sizes": ["20pcs","80pcs"], "sku_prefix": "WIP", "price_range": (55, 320), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
        },
        "Diapers": {
            "Pampers": {
                "products": [
                    {"name": "Baby Dry Pants", "sizes": ["Small","Medium","Large","XL","XXL"], "sku_prefix": "DIA", "price_range": (299, 1599), "flavours": None},
                    {"name": "Active Baby",    "sizes": ["Small","Medium","Large","XL"],       "sku_prefix": "DIA", "price_range": (249, 1399), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "Huggies": {
                "products": [
                    {"name": "Wonder Pants", "sizes": ["Small","Medium","Large","XL"], "sku_prefix": "DIA", "price_range": (279, 1499), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
            "MamyPoko": {
                "products": [
                    {"name": "Pants Diapers", "sizes": ["Small","Medium","Large","XL","XXL"], "sku_prefix": "DIA", "price_range": (249, 1399), "flavours": None},
                ],
                "flavours": None,
                "uom": "PCS"
            },
        },
    },
}

# ========================================
# BUSINESS STAGES  (sum = 1.0)
# ========================================
STAGE_PROBABILITIES = {
    "ACTIVE":      0.75,   # normal selling products
    "PROMOTIONAL": 0.10,   # on discount/offer
    "NEW_LAUNCH":  0.08,   # recently introduced
    "SEASONAL":    0.04,   # season-dependent
    "LOW_STOCK":   0.02,   # inventory running low
    "CLEARANCE":   0.01,   # end-of-life stock
}

def get_business_stage():
    rand = random.random()
    cumulative = 0.0
    for stage, prob in STAGE_PROBABILITIES.items():
        cumulative += prob
        if rand <= cumulative:
            return stage
    return "ACTIVE"   # unreachable safety fallback


# ========================================
# DB HELPERS
# ========================================
def get_next_id():
    cursor.execute("SELECT NVL(MAX(product_id), 0) + 1 FROM dim_product")
    return cursor.fetchone()[0]


def get_next_sku_counter():
    cursor.execute("SELECT sku FROM dim_product WHERE sku IS NOT NULL")
    skus = cursor.fetchall()
    max_counter = 0
    for (sku,) in skus:
        if sku and '-' in sku:
            parts = sku.split('-')
            if len(parts) >= 4 and parts[-1].isdigit():
                max_counter = max(max_counter, int(parts[-1]))
    return max_counter + 1


# ========================================
# SKU GENERATOR
# Pattern: PREFIX-BRAND_CODE(3)-SUBCAT_CODE(3)-COUNTER(5)
# e.g.  OIL-FOR-OIL-00123
# ========================================
def generate_sku(subcategory, brand, sku_prefix, counter):
    brand_code = ''.join(c for c in brand.upper() if c.isalpha())[:3]
    subcat_code = ''.join(c for c in subcategory.upper() if c.isalpha())[:3]
    return f"{sku_prefix}-{brand_code}-{subcat_code}-{counter:05d}"


# ========================================
# FLAVOUR RESOLVER
# Checks product-level flavours first, falls back to brand-level
# ========================================
def get_flavour(product_info, brand_details):
    flavours = product_info.get("flavours", brand_details.get("flavours"))
    if not flavours:
        return None
    prob = product_info.get("flavour_probability",
           brand_details.get("flavour_probability", 0.5))
    return random.choice(flavours) if random.random() < prob else None


# ========================================
# INSERT SQL  (positional binds for oracledb)
# ========================================
INSERT_SQL = """
INSERT INTO dim_product (
    product_id, product_name, category, sub_category,
    brand, flavour, product_size, sku, uom, unit_price, business_stage
) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
"""

# ========================================
# DAILY GENERATION
# ========================================
DAILY_INCREMENT = 10

CATEGORY_WEIGHTS = {
    "Grocery":       0.40,
    "Beverage":      0.20,
    "Dairy":         0.12,
    "Personal Care": 0.15,
    "Home Care":     0.10,
    "Baby Care":     0.03,
}

next_product_id  = get_next_id()
next_sku_counter = get_next_sku_counter()

data        = []
pid         = next_product_id
sku_counter = next_sku_counter

for _ in range(DAILY_INCREMENT):
    category    = random.choices(list(CATEGORY_WEIGHTS), weights=list(CATEGORY_WEIGHTS.values()))[0]
    subcategory = random.choice(list(PRODUCTS[category]))
    brand_name  = random.choice(list(PRODUCTS[category][subcategory]))
    brand_details  = PRODUCTS[category][subcategory][brand_name]
    product_info   = random.choice(brand_details["products"])

    size  = random.choice(product_info["sizes"])
    price = round(random.uniform(*product_info["price_range"]), 2)
    sku   = generate_sku(subcategory, brand_name, product_info["sku_prefix"], sku_counter)
    sku_counter += 1

    data.append((
        pid,
        product_info["name"],
        category,
        subcategory,
        brand_name[:50],
        get_flavour(product_info, brand_details),
        size,
        sku,
        brand_details["uom"],
        price,
        get_business_stage(),
    ))
    pid += 1

# ========================================
# DB INSERT
# ========================================
try:
    cursor.executemany(INSERT_SQL, data)
    connection.commit()
except Exception as e:
    connection.rollback()
    cursor.close()
    connection.close()
    raise RuntimeError(f"Insert failed: {e}") from e

# ========================================
# SUMMARY
# ========================================
cursor.execute("SELECT COUNT(*) FROM dim_product")
total_count = cursor.fetchone()[0]
cursor.close()
connection.close()

print(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
      f"Added: {len(data)} | "
      f"ID range: {next_product_id}-{pid-1} | "
      f"Total in DB: {total_count:,}")

for i, d in enumerate(data, 1):
    pid_, name, cat, subcat, brand, flavour, size, sku, uom, price, stage = d
    variant = f" [{flavour}]" if flavour else ""
    print(f"  {i:2d}. {sku} | {brand} {name}{variant} {size} | {cat}>{subcat} | ₹{price} | {stage}")
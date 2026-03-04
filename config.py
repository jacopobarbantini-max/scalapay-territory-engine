"""
config.py — Scalapay Territory Engine
Central configuration: tiers, scoring weights, business constants.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API KEYS ────────────────────────────────────────────────
HUBSPOT_TOKEN = os.getenv("HUBSPOT_API_KEY", "")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# ── COUNTRY CONFIGURATIONS ──────────────────────────────────
COUNTRIES = {
    "IT": {"label": "Italy",    "flag": "\U0001f1ee\U0001f1f9", "currency": "EUR"},
    "FR": {"label": "France",   "flag": "\U0001f1eb\U0001f1f7", "currency": "EUR"},
    "ES": {"label": "Spain",    "flag": "\U0001f1ea\U0001f1f8", "currency": "EUR"},
    "PT": {"label": "Portugal", "flag": "\U0001f1f5\U0001f1f9", "currency": "EUR"},
}

IB_COUNTRIES = ["ES", "PT"]

# ── TIER MAP  (Similarweb industry → Gold / Silver / Bronze) ───────────
#    Maps Similarweb "Industry" string to a priority tier.
#    GOLD   = highest-priority verticals for BNPL
#    SILVER = solid fit, moderate priority
#    BRONZE = lower fit but still addressable
#    EXCLUDE = blocked verticals (gambling, adult, etc.)
TIER_MAP = {
    # ── GOLD — core BNPL verticals ──────────────────────────
    "Apparel/Clothing":                     "GOLD",
    "Apparel/Footwear":                     "GOLD",
    "Apparel/Jewelry":                      "GOLD",
    "Apparel/Apparel Accessories":          "GOLD",
    "Beauty & Personal Care/Make-Up & Cosmetics": "GOLD",
    "Beauty & Personal Care/Skin Care":     "GOLD",
    "Beauty & Personal Care/Perfumes & Fragrances": "GOLD",
    "Beauty & Personal Care/Hair Care":     "GOLD",
    "Beauty & Personal Care/Anti-Aging":    "GOLD",
    "Beauty & Personal Care/Nail Care":     "GOLD",
    "Beauty & Personal Care/Shaving & Grooming": "GOLD",
    "Beauty & Personal Care/Oral Care":     "GOLD",
    "Beauty & Personal Care/Tanning & Sun Care": "GOLD",
    "Beauty & Personal Care/Hygiene & Toiletries": "GOLD",
    "Computers & Consumer Electronics/Consumer Electronics": "GOLD",
    "Computers & Consumer Electronics/Computer Hardware": "GOLD",
    "Computers & Consumer Electronics/Portable Media Devices": "GOLD",
    "Computers & Consumer Electronics/Home Audio & Video": "GOLD",
    "Computers & Consumer Electronics/Computer Accessories": "GOLD",
    "Computers & Consumer Electronics/Consumer Electronic Accessories": "GOLD",
    "Home & Garden/Home Furniture":         "GOLD",
    "Home & Garden/Home Decor & Interior Decorating": "GOLD",
    "Home & Garden/Home Appliances":        "GOLD",
    "Home & Garden/Kitchen & Dining":       "GOLD",
    "Home & Garden/Bedding & Linens":       "GOLD",
    "Home & Garden/Lights & Lighting":      "GOLD",
    "Home & Garden/Bathroom":               "GOLD",
    "Retailers & General Merchandise/Shopping Portals & Search Engines": "GOLD",
    "Retailers & General Merchandise/Marketplace": "GOLD",
    # ── SILVER — strong secondary verticals ─────────────────
    "Home & Garden/Home Improvement & Maintenance": "SILVER",
    "Home & Garden/Yard, Garden & Patio":   "SILVER",
    "Home & Garden/Home Safety & Security": "SILVER",
    "Home & Garden/Home Heating & Cooling": "SILVER",
    "Home & Garden/Home Laundry":           "SILVER",
    "Home & Garden/Water Filters":          "SILVER",
    "Home & Garden/Residential Cleaning":   "SILVER",
    "Home & Garden/Home & Garden Media & Publications": "SILVER",
    "Sports & Fitness/Sporting Goods":      "SILVER",
    "Sports & Fitness/Sports & Fitness Apparel": "SILVER",
    "Sports & Fitness/Fitness":             "SILVER",
    "Sports & Fitness/Sports Fan Gear & Apparel": "SILVER",
    "Sports & Fitness/Boating & Water Recreation": "SILVER",
    "Sports & Fitness/Sports Instruction & Coaching": "SILVER",
    "Health/Pharmacy":                      "SILVER",
    "Health/Nutrition & Dieting":           "SILVER",
    "Health/Health Care Services":          "SILVER",
    "Health/Biotech & Pharmaceutical":      "SILVER",
    "Health/Medical Devices, Equipment & Supplies": "SILVER",
    "Health/Health Conditions & Concerns":  "SILVER",
    "Hobbies & Leisure/Pets & Animals":     "SILVER",
    "Hobbies & Leisure/Toys & Games":       "SILVER",
    "Hobbies & Leisure/Arts & Crafts":      "SILVER",
    "Hobbies & Leisure/Gardening":          "SILVER",
    "Hobbies & Leisure/Cooking":            "SILVER",
    "Hobbies & Leisure/Photo & Video":      "SILVER",
    "Hobbies & Leisure/Camping & Outdoor Recreation": "SILVER",
    "Family & Community/Baby, Parenting & Family": "SILVER",
    "Occasions & Gifts/Gifts":             "SILVER",
    "Occasions & Gifts/Flower Arrangements": "SILVER",
    "Occasions & Gifts/Special Occasions":  "SILVER",
    "Occasions & Gifts/Parties & Party Supplies": "SILVER",
    "Occasions & Gifts/Cards & Greetings":  "SILVER",
    "Occasions & Gifts/Holidays & Seasonal Events": "SILVER",
    "Beauty & Personal Care/Spa & Medical Spa": "SILVER",
    "Beauty & Personal Care/Fashion & Style": "SILVER",
    "Beauty & Personal Care/Body Art":      "SILVER",
    "Computers & Consumer Electronics/Car Audio & Video": "SILVER",
    "Computers & Consumer Electronics/GPS & Navigation": "SILVER",
    "Computers & Consumer Electronics/Software": "SILVER",
    "Computers & Consumer Electronics/Computer Repair": "SILVER",
    "Computers & Consumer Electronics/Consumer Electronic Service": "SILVER",
    # ── BRONZE — lower fit but addressable ──────────────────
    "Travel & Tourism/Accommodations":      "BRONZE",
    "Travel & Tourism/Travel Booking Services": "BRONZE",
    "Travel & Tourism/Vacation Packages":   "BRONZE",
    "Travel & Tourism/Specialty Travel":    "BRONZE",
    "Travel & Tourism/Tourist Attractions & Destinations": "BRONZE",
    "Travel & Tourism/Luggage & Travel Accessories": "BRONZE",
    "Travel & Tourism/Luggage Services":    "BRONZE",
    "Travel & Tourism/Transportation & Excursions": "BRONZE",
    "Travel & Tourism/Travel Documents":    "BRONZE",
    "Travel & Tourism/Travel Media & Publications": "BRONZE",
    "Food & Groceries/Food":                "BRONZE",
    "Food & Groceries/Online Grocery Shopping & Grocery Delivery": "BRONZE",
    "Food & Groceries/Beverages":           "BRONZE",
    "Food & Groceries/Household Supplies":  "BRONZE",
    "Food & Groceries/Tobacco":             "BRONZE",
    "Vehicles/Motor Vehicles":              "BRONZE",
    "Vehicles/Vehicle Parts & Accessories": "BRONZE",
    "Vehicles/Vehicle Dealers":             "BRONZE",
    "Vehicles/Vehicle Repair & Maintenance": "BRONZE",
    "Vehicles/Boats & Watercraft":          "BRONZE",
    "Vehicles/Vehicle Specs, Reviews & Comparisons": "BRONZE",
    "Vehicles/Vehicle Auctions":            "BRONZE",
    "Vehicles/Personal Airplanes & Aircraft": "BRONZE",
    "Vehicles/Vehicle History Reports":     "BRONZE",
    "Vehicles/Vehicle Towing":              "BRONZE",
    "Vehicles/Vehicle Window Tinting":      "BRONZE",
    "Hobbies & Leisure/Antiques & Collectibles": "BRONZE",
    "Hobbies & Leisure/Wine & Beer Collecting & Brewing": "BRONZE",
    "Hobbies & Leisure/Scale Models & Model Building": "BRONZE",
    "Hobbies & Leisure/Roleplaying Games":  "BRONZE",
    "Hobbies & Leisure/Astronomy":          "BRONZE",
    "Hobbies & Leisure/Birding":            "BRONZE",
    "Hobbies & Leisure/Prizes & Competitions": "BRONZE",
    "Retailers & General Merchandise/Auctions": "BRONZE",
    "Retailers & General Merchandise/Classifieds": "BRONZE",
    "Retailers & General Merchandise/Coupons & Rebates": "BRONZE",
    "Retailers & General Merchandise/Wholesalers & Liquidators": "BRONZE",
    "News, Media & Publications/Books & Literature": "BRONZE",
    "News, Media & Publications/Magazines & Magazine Subscriptions": "BRONZE",
    "News, Media & Publications/Publishing": "BRONZE",
    "Dining & Nightlife/Restaurants":       "BRONZE",
    "Dining & Nightlife/Dining & Nightlife Reviews, Guides & Listings": "BRONZE",
    "Dining & Nightlife/Nightclubs, Bars & Music Clubs": "BRONZE",
    "Real Estate/Real Estate Listings":     "BRONZE",
    "Business & Industrial/Retail Trade":   "BRONZE",
    "Business & Industrial/Office":         "BRONZE",
    "Business & Industrial/Food Service Industry": "BRONZE",
    # ── NOT_FIT — not BNPL-relevant, skip but don't exclude ─
    "Arts & Entertainment/Movies & Films":  "NOT_FIT",
    "Arts & Entertainment/Music & Audio":   "NOT_FIT",
    "Arts & Entertainment/TV & Video":      "NOT_FIT",
    "Arts & Entertainment/Entertainment Industry": "NOT_FIT",
    "Arts & Entertainment/Entertainment Media Retailers": "NOT_FIT",
    "Arts & Entertainment/Events, Shows & Cultural Attractions": "NOT_FIT",
    "Arts & Entertainment/Event Entertainment": "NOT_FIT",
    "Arts & Entertainment/Sports Entertainment": "NOT_FIT",
    "Arts & Entertainment/Visual Art & Design": "NOT_FIT",
    "Arts & Entertainment/Film & TV Industry": "NOT_FIT",
    "Arts & Entertainment/Fun & Trivia":    "NOT_FIT",
    "Arts & Entertainment/Humor & Jokes":   "NOT_FIT",
    "Arts & Entertainment/Offbeat":         "NOT_FIT",
    "Arts & Entertainment/Comics & Graphic Novels": "NOT_FIT",
    "Arts & Entertainment/Cartoons":        "NOT_FIT",
    "Sports & Fitness/Sports":              "NOT_FIT",
    "Sports & Fitness/Sports News & Media": "NOT_FIT",
    "Business & Industrial/Aerospace & Defense": "NOT_FIT",
    "Business & Industrial/Agriculture":    "NOT_FIT",
    "Business & Industrial/Building Construction & Maintenance": "NOT_FIT",
    "Business & Industrial/Business Management": "NOT_FIT",
    "Business & Industrial/Chemical Industry": "NOT_FIT",
    "Business & Industrial/Commercial & Industrial Printing": "NOT_FIT",
    "Business & Industrial/Design & Engineering": "NOT_FIT",
    "Business & Industrial/Energy Industry": "NOT_FIT",
    "Business & Industrial/Industrial Goods & Manufacturing": "NOT_FIT",
    "Business & Industrial/Janitorial Products & Services": "NOT_FIT",
    "Business & Industrial/Manufacturing":  "NOT_FIT",
    "Business & Industrial/Scientific Equipment & Services": "NOT_FIT",
    "Business & Industrial/Security Equipment & Services": "NOT_FIT",
    "Business & Industrial/Shipping & Packing": "NOT_FIT",
    "Internet & Telecom/Calling Cards":     "NOT_FIT",
    "Internet & Telecom/Email & Messaging": "NOT_FIT",
    "Internet & Telecom/Internet Service Plans": "NOT_FIT",
    "Internet & Telecom/Internet Services": "NOT_FIT",
    "Internet & Telecom/Network Security":  "NOT_FIT",
    "Internet & Telecom/Online Services":   "NOT_FIT",
    "Internet & Telecom/Social Networks & Online Communities": "NOT_FIT",
    "Internet & Telecom/Telephony":         "NOT_FIT",
    "Jobs & Education/Schooling":           "NOT_FIT",
    "Jobs & Education/Vocational Training": "NOT_FIT",
    "Law & Government/Military":            "NOT_FIT",
    "News, Media & Publications/Celebrities & Entertainment News": "NOT_FIT",
    "News, Media & Publications/Local News, Media & Publications": "NOT_FIT",
    "News, Media & Publications/Men's Interests Media & Publications": "NOT_FIT",
    "News, Media & Publications/Newspapers": "NOT_FIT",
    "News, Media & Publications/Online Media": "NOT_FIT",
    "News, Media & Publications/Political News & Media": "NOT_FIT",
    "News, Media & Publications/Reference Materials & Resources": "NOT_FIT",
    "News, Media & Publications/Weather":   "NOT_FIT",
    "Finance/Credit & Lending":             "NOT_FIT",
    "Finance/Investing":                    "NOT_FIT",
    "Family & Community/Community Service & Social Organizations": "NOT_FIT",
    "Family & Community/Faith & Belief":    "NOT_FIT",
    "Family & Community/Romance & Relationships": "NOT_FIT",
    # ── EXCLUDE — blocked verticals ─────────────────────────
    "Adult":                                "EXCLUDE",
    "Gambling":                             "EXCLUDE",
    "Gambling/Casinos":                     "EXCLUDE",
    "Gambling/Sports Betting":              "EXCLUDE",
    "Gambling/Poker":                       "EXCLUDE",
    "Gambling/Lottery":                     "EXCLUDE",
}

# Numeric score per tier (max 25 pts in composite)


# --- TIER MAP BY COUNTRY (from Running sheet) ---
# Maps Scalapay internal category -> tier, per country
TIER_BY_COUNTRY = {
    "IB": {
        "Jewelry & Watches": "BRONZE", "Electronics": "BRONZE",
        "Pharma": "GOLD", "Dental": "GOLD", "Petcare": "GOLD",
        "B2B": "SILVER", "Cosmetics & Beauty": "GOLD",
        "Apparel & Fashion": "GOLD", "Shoes & Accessories": "GOLD",
        "Wellness": "GOLD", "Food & Beverage": "GOLD",
        "Glasses & Eyewear": "SILVER", "Other": "SILVER",
        "Medical": "GOLD", "Sport": "GOLD",
        "Generalist Marketplace": "GOLD", "General Retail": "GOLD",
        "Professional Services": "GOLD", "Education": "GOLD",
        "Baby & Toddler": "SILVER", "Home & Garden": "SILVER",
        "Auto & Moto": "BRONZE", "Luxury Goods": "SILVER",
        "Auto Repair Shops": "SILVER", "Household Appliance": "SILVER",
        "Travel": "SILVER", "Learning & Classes": "GOLD",
        "Hobbies & Games": "SILVER", "Veterinarians": "SILVER",
        "Entertainment & Sports": "SILVER", "Household Appliances": "SILVER",
        "B2B Goods & Trade Materials": "SILVER",
        "Electronics & Household appliance": "BRONZE",
    },
    "FR": {
        "Jewelry & Watches": "BRONZE", "Electronics": "BRONZE",
        "Pharma": "GOLD", "Dental": "GOLD", "Petcare": "GOLD",
        "B2B": "SILVER", "Cosmetics & Beauty": "GOLD",
        "Apparel & Fashion": "GOLD", "Shoes & Accessories": "GOLD",
        "Wellness": "GOLD", "Food & Beverage": "GOLD",
        "Glasses & Eyewear": "SILVER", "Other": "SILVER",
        "Medical": "GOLD", "Sport": "GOLD",
        "Generalist Marketplace": "GOLD", "General Retail": "GOLD",
        "Professional Services": "GOLD", "Education": "GOLD",
        "Baby & Toddler": "SILVER", "Home & Garden": "SILVER",
        "Auto & Moto": "BRONZE", "Luxury Goods": "SILVER",
        "Auto Repair Shops": "BRONZE", "Household Appliance": "SILVER",
        "Travel": "SILVER", "Learning & Classes": "GOLD",
        "Hobbies & Games": "SILVER", "Veterinarians": "SILVER",
        "Entertainment & Sports": "SILVER", "Household Appliances": "SILVER",
        "B2B Goods & Trade Materials": "SILVER",
        "Electronics & Household appliance": "BRONZE",
    },
}

# Category mapping: Similarweb industry -> Scalapay internal category
SW_TO_SCALAPAY_CATEGORY = {
    "Apparel/Clothing": "Apparel & Fashion",
    "Apparel/Footwear": "Shoes & Accessories",
    "Apparel/Jewelry": "Jewelry & Watches",
    "Apparel/Apparel Accessories": "Apparel & Fashion",
    "Beauty & Personal Care/Make-Up & Cosmetics": "Cosmetics & Beauty",
    "Beauty & Personal Care/Skin Care": "Cosmetics & Beauty",
    "Beauty & Personal Care/Perfumes & Fragrances": "Cosmetics & Beauty",
    "Beauty & Personal Care/Hair Care": "Cosmetics & Beauty",
    "Beauty & Personal Care/Anti-Aging": "Cosmetics & Beauty",
    "Beauty & Personal Care/Nail Care": "Cosmetics & Beauty",
    "Beauty & Personal Care/Shaving & Grooming": "Cosmetics & Beauty",
    "Beauty & Personal Care/Oral Care": "Dental",
    "Beauty & Personal Care/Tanning & Sun Care": "Cosmetics & Beauty",
    "Beauty & Personal Care/Hygiene & Toiletries": "Cosmetics & Beauty",
    "Beauty & Personal Care/Spa & Medical Spa": "Wellness",
    "Beauty & Personal Care/Fashion & Style": "Apparel & Fashion",
    "Computers & Consumer Electronics/Consumer Electronics": "Electronics",
    "Computers & Consumer Electronics/Computer Hardware": "Electronics",
    "Computers & Consumer Electronics/Portable Media Devices": "Electronics",
    "Computers & Consumer Electronics/Home Audio & Video": "Electronics",
    "Computers & Consumer Electronics/Computer Accessories": "Electronics",
    "Computers & Consumer Electronics/Consumer Electronic Accessories": "Electronics",
    "Computers & Consumer Electronics/Software": "Electronics",
    "Home & Garden/Home Furniture": "Home & Garden",
    "Home & Garden/Home Decor & Interior Decorating": "Home & Garden",
    "Home & Garden/Home Appliances": "Household Appliance",
    "Home & Garden/Kitchen & Dining": "Home & Garden",
    "Home & Garden/Bedding & Linens": "Home & Garden",
    "Home & Garden/Lights & Lighting": "Home & Garden",
    "Home & Garden/Bathroom": "Home & Garden",
    "Home & Garden/Home Improvement & Maintenance": "Home & Garden",
    "Home & Garden/Yard, Garden & Patio": "Home & Garden",
    "Retailers & General Merchandise/Shopping Portals & Search Engines": "Generalist Marketplace",
    "Retailers & General Merchandise/Marketplace": "Generalist Marketplace",
    "Retailers & General Merchandise/Auctions": "General Retail",
    "Sports & Fitness/Sporting Goods": "Sport",
    "Sports & Fitness/Sports & Fitness Apparel": "Sport",
    "Sports & Fitness/Fitness": "Sport",
    "Health/Pharmacy": "Pharma",
    "Health/Nutrition & Dieting": "Wellness",
    "Health/Health Care Services": "Medical",
    "Health/Biotech & Pharmaceutical": "Pharma",
    "Health/Medical Devices, Equipment & Supplies": "Medical",
    "Hobbies & Leisure/Pets & Animals": "Petcare",
    "Hobbies & Leisure/Toys & Games": "Hobbies & Games",
    "Hobbies & Leisure/Arts & Crafts": "Hobbies & Games",
    "Family & Community/Baby, Parenting & Family": "Baby & Toddler",
    "Occasions & Gifts/Gifts": "Other",
    "Travel & Tourism/Accommodations": "Travel",
    "Travel & Tourism/Travel Booking Services": "Travel",
    "Travel & Tourism/Vacation Packages": "Travel",
    "Travel & Tourism/Specialty Travel": "Travel",
    "Travel & Tourism/Luggage & Travel Accessories": "Travel",
    "Food & Groceries/Food": "Food & Beverage",
    "Food & Groceries/Online Grocery Shopping & Grocery Delivery": "Food & Beverage",
    "Food & Groceries/Beverages": "Food & Beverage",
    "Vehicles/Motor Vehicles": "Auto & Moto",
    "Vehicles/Vehicle Parts & Accessories": "Auto & Moto",
    "Vehicles/Vehicle Dealers": "Auto & Moto",
    "Vehicles/Vehicle Repair & Maintenance": "Auto Repair Shops",
    "Business & Industrial/Retail Trade": "B2B",
    "Business & Industrial/Office": "B2B",
    "Dining & Nightlife/Restaurants": "Food & Beverage",
    "Arts & Entertainment/Events, Shows & Cultural Attractions": "Entertainment & Sports",
    "Hobbies & Leisure/Camping & Outdoor Recreation": "Sport",
    "News, Media & Publications/Books & Literature": "Other",
}

TIER_SCORE = {
    "GOLD":    25,
    "SILVER":  18,
    "BRONZE":  10,
    "NOT_FIT":  3,
    "UNKNOWN":  8,
    "EXCLUDE":  0,
}

# ── REVENUE TIERS (Similarweb bucket → account segment) ────
REVENUE_TIERS = {
    "> 1B":         "Enterprise",
    "500M - 1B":    "Enterprise",
    "200M - 500M":  "Enterprise",
    "100M - 200M":  "Large",
    "75M - 100M":   "Large",
    "50M - 75M":    "Large",
    "50M - 100M":   "Large",
    "25M - 50M":    "Mid-Market",
    "15M - 25M":    "Mid-Market",
    "10M - 15M":    "Mid-Market",
    "10M - 25M":    "Mid-Market",
    "5M - 10M":     "SMB",
    "2M - 5M":      "SMB",
    "1M - 2M":      "SMB",
    "1M - 5M":      "SMB",
    "0 - 1M":       "Micro",
    "< 1M":         "Micro",
}

ACCOUNT_SIZE_SCORE = {
    "Enterprise": 20,
    "Large":      15,
    "Mid-Market": 10,
    "SMB":         5,
    "Micro":       2,
    "Unknown":     3,
}

# ── SCORING WEIGHTS (sidebar sliders default values) ────────
SCORING_WEIGHTS = {
    "tier":              0.25,
    "penetration_ttv":   0.20,
    "traffic_growth":    0.20,
    "lead_warmth":       0.15,
    "whitespace":        0.20,  # absorbs old competitor weight — now purely competitor-based
}

# ── BUSINESS ASSUMPTIONS ────────────────────────────────────
AVG_ORDER_VALUE_EUR = 120.0          # average basket for BNPL-eligible purchase
TRAFFIC_TO_TRANSACTION_RATE = 0.025  # 2.5% of visits → transactions
SCALAPAY_SHARE_OF_CHECKOUT = 0.08    # 8% share of checkout (target)
DEFAULT_PENETRATION_PCT = 5.0        # fallback BNPL penetration

# BNPL penetration % by macro-category (industry research estimates)
# --- REAL SCALAPAY PENETRATION RATES (from Scalapay Metrics sheet) ---
# Penetration = % of checkout addressable by BNPL, by country
BNPL_PENETRATION_BY_COUNTRY = {
    "ES": {
        "Home & Garden": 5.4, "Apparel & Fashion": 9.3, "Health & Wellness": 5.7,
        "Learning & Classes": 14.8, "Hobbies & Games": 5.7, "Food & Beverage": 4.6,
        "Other": 5.6, "Travel": 5.3, "Luxury Goods": 7.9, "Sport": 6.8,
        "Cosmetics & Beauty": 8.1, "Shoes & Accessories": 9.3, "Petcare": 5.7,
        "Electronics & Household appliance": 6.5, "Jewelry & Watches": 7.3,
        "Baby & Toddler": 6.1, "B2B": 6.4, "Auto & Moto": 6.7,
        "Generalist Marketplace": 3.0,
    },
    "FR": {
        "Home & Garden": 5.6, "Apparel & Fashion": 9.2, "Health & Wellness": 5.7,
        "Learning & Classes": 16.5, "Hobbies & Games": 5.6, "Food & Beverage": 4.5,
        "Other": 5.1, "Travel": 5.0, "Luxury Goods": 6.9, "Sport": 6.3,
        "Cosmetics & Beauty": 8.9, "Shoes & Accessories": 9.3, "Petcare": 4.9,
        "Electronics & Household appliance": 8.7, "Jewelry & Watches": 9.1,
        "Baby & Toddler": 5.7, "B2B": 3.4, "Auto & Moto": 7.6,
        "Generalist Marketplace": 3.7,
    },
    "IB": {  # Iberia uses ES rates (PT similar)
        "Home & Garden": 5.4, "Apparel & Fashion": 9.3, "Health & Wellness": 5.7,
        "Learning & Classes": 14.8, "Hobbies & Games": 5.7, "Food & Beverage": 4.6,
        "Other": 5.6, "Travel": 5.3, "Luxury Goods": 7.9, "Sport": 6.8,
        "Cosmetics & Beauty": 8.1, "Shoes & Accessories": 9.3, "Petcare": 5.7,
        "Electronics & Household appliance": 6.5, "Jewelry & Watches": 7.3,
        "Baby & Toddler": 6.1, "B2B": 6.4, "Auto & Moto": 6.7,
        "Generalist Marketplace": 3.0,
    },
}

# Fallback: global averages (used if country not found)
BNPL_PENETRATION_PCT = {
    "Home & Garden": 5.6, "Apparel & Fashion": 9.6, "Health & Wellness": 5.6,
    "Learning & Classes": 13.6, "Hobbies & Games": 6.9, "Food & Beverage": 4.5,
    "Other": 5.4, "Travel": 4.5, "Luxury Goods": 7.0, "Sport": 7.0,
    "Cosmetics & Beauty": 9.0, "Shoes & Accessories": 9.2, "Petcare": 5.6,
    "Electronics & Household appliance": 8.8, "Jewelry & Watches": 9.2,
    "Baby & Toddler": 6.5, "B2B": 5.0, "Auto & Moto": 8.0,
    "Generalist Marketplace": 4.2,
}

# ── WHITESPACE CATEGORIES ──────────────────────────────────
# Industries where BNPL is under-penetrated → big greenfield opportunity
WHITESPACE_CATEGORIES = {
    "Travel & Tourism/Accommodations",
    "Travel & Tourism/Travel Booking Services",
    "Travel & Tourism/Vacation Packages",
    "Travel & Tourism/Specialty Travel",
    "Vehicles/Motor Vehicles",
    "Vehicles/Vehicle Parts & Accessories",
    "Health/Pharmacy",
    "Health/Nutrition & Dieting",
    "Health/Health Care Services",
    "Hobbies & Leisure/Pets & Animals",
    "Food & Groceries/Online Grocery Shopping & Grocery Delivery",
    "Food & Groceries/Food",
    "Dining & Nightlife/Restaurants",
    "Real Estate/Real Estate Listings",
    "Business & Industrial/Office",
}

# ── COMPETITOR & PSP DETECTION (enrichment.py) ──────────────
KNOWN_COMPETITORS = [
    "klarna",
    "afterpay",
    "clearpay",
    "alma",
    "oney",
    "cofidis",
    "pledg",
    "aplazame",
    "pagantis",
    "sequra",
    "cetelem",
    "paypal credit",
    "pay later",
    "paylater",
    "zip",
    "laybuy",
    "tabby",
    "tamara",
    "postepay",
    "soisy",
    "pagolight",
]

# PayPal BNPL identifiers (for whitespace logic: PayPal-only BNPL = still whitespace)
PAYPAL_BNPL_TAGS = {"Paypal Credit", "Pay Later", "Paylater"}

KNOWN_PSPS = [
    "stripe",
    "adyen",
    "checkout.com",
    "braintree",
    "paypal",
    "worldpay",
    "ingenico",
    "mollie",
    "shopify payments",
    "redsys",
    "bizum",
    "satispay",
    "nexi",
    "sella",
]

# ── WARMTH SCORES (lead_warmth → pts, max 10) ──────────────
WARMTH_SCORES = {
    "Existing Client":       2,
    "Active Pipeline":      10,
    "In CRM, No Deal":      7,
    "Net New":               5,
    "Lost 6m+ ago":          4,
    "Recently Lost":         3,
}

# ── HUBSPOT CONSTANTS ───────────────────────────────────────
HS_CROSS_COUNTRY_PROPERTY = os.getenv(
    "HUBSPOT_CROSS_COUNTRY_PROPERTY", "cross_country_flag"
)

# ── HubSpot Pipeline Mapping (from live API) ────────────────
HS_PIPELINES = {
    "77766861":   "Sales",
    "75805933":   "Inbound",
    "127897798":  "Partner",
    "1347411134": "Partnership",
    "208540610":  "Account Mgmt",
    "258360802":  "Churn",
}

# Stage IDs that mean "Won" across all pipelines
HS_WON_STAGE_IDS = {
    "184232171",   # Sales - Won
    "181259992",   # Inbound - Won
    "256106457",   # Partner - Live & Engaged
    "1834011870",  # Partnership - Won
    "363460851",   # Account Mgmt - Won
}

# Stage IDs that mean "Closed Lost" across all pipelines
HS_LOST_STAGE_IDS = {
    "184232172",   # Sales - Closed lost
    "181259993",   # Inbound - Closed lost
    "256106459",   # Partner - Not interested
    "1834011871",  # Partnership - Closed Lost
    "363460852",   # Account Mgmt - Closed lost
    "428007616",   # Churn - Churn
}

# Stage IDs that are actively in pipeline (not closed)
HS_ACTIVE_STAGE_IDS = {
    # Sales
    "184232166",   # SQL
    "184232167",   # Discovery meetings
    "2705937612",  # NBM Pending Review
    "184232168",   # Business Meeting
    "184232169",   # Negotiation
    "184220904",   # Onboarding Initiated
    "184220905",   # Onboarding Completed
    # Inbound
    "181259987",   # Inbound Created
    "181259988",   # Proposal sent
    "181259989",   # Registered
    "720800761",   # KYC Pending Approval
    "181259990",   # Onboarding Completed
    "2019815647",  # Integration Completed
    # Partner
    "256106455",   # Target
    "256106456",   # In discussion
    # Partnership
    "2586233042",  # Inbound Created
    "1834011865",  # Proposal sent
    "1834011866",  # KYC Pending Approval
    "2019816637",  # Onboarding Completed
    "2021900503",  # Integration Completed
    # Account Mgmt
    "363460848",   # Discovery meetings
    "363460849",   # Business Meeting
    "363460850",   # Validation & Negotiation
    "362959344",   # Final proposal to EB
    "362959345",   # Contract signed
    # Churn
    "428007611",   # Churn Risk
    "428007613",   # Negotiation
}

# Human-readable stage labels by ID
HS_STAGE_LABELS = {
    # Sales
    "184232166": "SQL", "184232167": "Discovery meetings",
    "2705937612": "NBM Pending Review", "184232168": "Business Meeting",
    "184232169": "Negotiation", "184220904": "Onboarding Initiated",
    "184220905": "Onboarding Completed", "184232171": "Won",
    "184232172": "Closed lost",
    # Inbound
    "181259987": "Inbound Created", "181259988": "Proposal sent",
    "181259989": "Registered", "720800761": "KYC Pending Approval",
    "181259990": "Onboarding Completed", "2019815647": "Integration Completed",
    "181259992": "Won", "181259993": "Closed lost",
    # Partner
    "256106455": "Target", "256106456": "In discussion",
    "256106457": "Live & Engaged", "256106458": "Live & Sleeping",
    "256106459": "Not interested",
    # Partnership
    "2586233042": "Inbound Created", "1834011865": "Proposal sent",
    "1834011866": "KYC Pending Approval", "2019816637": "Onboarding Completed",
    "2021900503": "Integration Completed", "1834011870": "Won",
    "1834011871": "Closed Lost",
    # Account Mgmt
    "363460847": "Target", "363460848": "Discovery meetings",
    "363460849": "Business Meeting", "363460850": "Validation & Negotiation",
    "362959344": "Final proposal to EB", "362959345": "Contract signed",
    "363460851": "Won", "363460852": "Closed lost", "363503572": "Terminated",
    # Churn
    "428007611": "Churn Risk", "428007613": "Negotiation",
    "428007616": "Churn", "428007617": "Retained",
}

# Legacy compatibility
HS_DEAL_STAGES_WON = {"closedwon", "won"}
HS_DEAL_STAGES_WARM = {"appointmentscheduled", "qualifiedtobuy",
    "presentationscheduled", "decisionmakerboughtin", "contractsent"}
HS_DEAL_STAGES_LOST = {"closedlost", "lost"}
CLOSED_LOST_REACTIVATION_MONTHS = 6

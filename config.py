from typing import Dict, List

_SCALAPAY_TIERS = {
    "Jewelry & Watches": ("BRONZE","BRONZE","BRONZE"),
    "Electronics": ("BRONZE","BRONZE","BRONZE"),
    "Pharma": ("GOLD","GOLD","GOLD"),
    "Dental": ("GOLD","GOLD","GOLD"),
    "Petcare": ("GOLD","GOLD","GOLD"),
    "B2B": ("GOLD","SILVER","SILVER"),
    "Cosmetics & Beauty": ("GOLD","GOLD","GOLD"),
    "Apparel & Fashion": ("GOLD","GOLD","GOLD"),
    "Shoes & Accessories": ("GOLD","GOLD","GOLD"),
    "Wellness": ("GOLD","GOLD","GOLD"),
    "Food & Beverage": ("GOLD","GOLD","GOLD"),
    "Glasses & Eyewear": ("GOLD","SILVER","SILVER"),
    "Other": ("GOLD","SILVER","SILVER"),
    "Medical": ("GOLD","GOLD","GOLD"),
    "Sport": ("GOLD","GOLD","GOLD"),
    "Generalist Marketplace": ("GOLD","GOLD","GOLD"),
    "General Retail": ("GOLD","GOLD","GOLD"),
    "Professional Services": ("GOLD","GOLD","GOLD"),
    "Education": ("GOLD","GOLD","GOLD"),
    "Baby & Toddler": ("SILVER","SILVER","SILVER"),
    "Home & Garden": ("SILVER","SILVER","SILVER"),
    "Auto & Moto": ("SILVER","BRONZE","BRONZE"),
    "Luxury Goods": ("SILVER","SILVER","SILVER"),
    "Household Appliance": ("SILVER","SILVER","SILVER"),
    # Travel split by sub-category (risk-based: net default rate)
    # IT rules: base SILVER, GOLD exceptions, BRONZE >2% NL
    # FR from FR data, IB from ES data
    #                                        IT        FR        IB(ES+PT)
    "Travel - OTA":                       ("BRONZE", "BRONZE", "BRONZE"),
    "Travel - Hotel & Accommodation":     ("SILVER", "BRONZE", "BRONZE"),
    "Travel - Tour Operator":             ("SILVER", "SILVER", "SILVER"),
    "Travel - Local & Urban Transport":   ("SILVER", "GOLD",   "SILVER"),
    "Travel - Adventure & Group Travel":  ("GOLD",   "SILVER", "GOLD"),
    "Travel - Theme Parks":               ("GOLD",   "SILVER", "GOLD"),
    "Travel - Cruise":                    ("BRONZE", "SILVER", "GOLD"),
    "Travel - Car Rental":                ("BRONZE", "SILVER", "GOLD"),
    "Travel - Entertainment & Experiences": ("SILVER","GOLD",  "GOLD"),
    "Travel - Ticketing & Events":        ("GOLD",   "GOLD",   "GOLD"),
    "Travel - Ferry & Maritime":          ("SILVER", "SILVER", "GOLD"),
    "Travel - Wellness & Spa":            ("GOLD",   "GOLD",   "GOLD"),
    "Travel - Ski & Mountain":            ("GOLD",   "SILVER", "GOLD"),
    "Travel - Other":                     ("SILVER", "SILVER", "SILVER"),
    "Hobbies & Games": ("SILVER","SILVER","SILVER"),
    "Veterinarians": ("SILVER","SILVER","SILVER"),
    "Entertainment & Sports": ("SILVER","SILVER","SILVER"),
}
_REGION_INDEX = {"IT": 0, "FR": 1, "ES": 2, "PT": 2, "IB": 2}

_SW_TO_SCALAPAY = {
    "Apparel/Clothing": "Apparel & Fashion",
    "Apparel/Footwear": "Shoes & Accessories",
    "Apparel/Jewelry": "Jewelry & Watches",
    "Beauty & Personal Care/Make-Up & Cosmetics": "Cosmetics & Beauty",
    "Beauty & Personal Care/Skin Care": "Cosmetics & Beauty",
    "Beauty & Personal Care/Perfumes & Fragrances": "Cosmetics & Beauty",
    "Beauty & Personal Care/Oral Care": "Dental",
    "Beauty & Personal Care/Spa & Medical Spa": "Wellness",
    "Health/Pharmacy": "Pharma",
    "Health/Biotech & Pharmaceutical": "Pharma",
    "Health/Nutrition & Dieting": "Wellness",
    "Health/Health Care Services": "Medical",
    "Health/Medical Devices, Equipment & Supplies": "Medical",
    "Sports & Fitness/Sporting Goods": "Sport",
    "Sports & Fitness/Fitness": "Sport",
    "Food & Groceries/Food": "Food & Beverage",
    "Food & Groceries/Beverages": "Food & Beverage",
    "Food & Groceries/Online Grocery Shopping & Grocery Delivery": "Food & Beverage",
    "Dining & Nightlife/Restaurants": "Food & Beverage",
    "Hobbies & Leisure/Pets & Animals": "Petcare",
    "Home & Garden/Home Furniture": "Home & Garden",
    "Home & Garden/Home Appliances": "Household Appliance",
    "Travel & Tourism/Accommodations": "Travel - Hotel & Accommodation",
    "Travel & Tourism/Travel Booking Services": "Travel - OTA",
    "Travel & Tourism/Vacation Packages": "Travel - Tour Operator",
    "Travel & Tourism/Specialty Travel": "Travel - Adventure & Group Travel",
    "Travel & Tourism/Tourist Attractions & Destinations": "Travel - Entertainment & Experiences",
    "Travel & Tourism/Transportation & Excursions": "Travel - Local & Urban Transport",
    "Travel & Tourism/Luggage & Travel Accessories": "Other",
    "Travel & Tourism/Travel Documents": "Other",
    "Travel & Tourism/Luggage Services": "Other",
    "Travel & Tourism/Travel Media & Publications": "Other",
    "Computers & Consumer Electronics/Consumer Electronics": "Electronics",
    "Computers & Consumer Electronics/Software": "Electronics",
    "Hobbies & Leisure/Toys & Games": "Hobbies & Games",
    "Hobbies & Leisure/Cooking": "Food & Beverage",
    "Family & Community/Baby, Parenting & Family": "Baby & Toddler",
    "Vehicles/Motor Vehicles": "Auto & Moto",
    "Vehicles/Vehicle Parts & Accessories": "Auto & Moto",
    "Retailers & General Merchandise/Marketplace": "Generalist Marketplace",
    "Retailers & General Merchandise/Shopping Portals & Search Engines": "General Retail",
    "Retailers & General Merchandise/Coupons & Rebates": "General Retail",
    "Business & Industrial/Building Construction & Maintenance": "B2B",
    "Business & Industrial/Industrial Goods & Manufacturing": "B2B",
    "Business & Industrial/Shipping & Packing": "B2B",
    "Jobs & Education/Schooling": "Education",
    "Arts & Entertainment/Entertainment Media Retailers": "Entertainment & Sports",
    "Occasions & Gifts/Gifts": "General Retail",
}

_EXCLUDE = ["Gambling","Adult","Finance/Investing","Real Estate","Food & Groceries/Tobacco"]

def get_tier(industry, country):
    if not industry or not isinstance(industry, str): return "UNKNOWN"
    industry = industry.strip()
    for exc in _EXCLUDE:
        if industry.startswith(exc): return "EXCLUDE"
    sc = _SW_TO_SCALAPAY.get(industry)
    if not sc:
        macro = industry.split("/")[0].strip()
        for k, v in _SW_TO_SCALAPAY.items():
            if k.startswith(macro + "/"): sc = v; break
    if not sc: return "UNKNOWN"
    t = _SCALAPAY_TIERS.get(sc)
    if not t: return "UNKNOWN"
    return t[_REGION_INDEX.get(str(country).upper(), 2)]

def get_scalapay_category(industry):
    if not industry or not isinstance(industry, str): return "Other"
    c = _SW_TO_SCALAPAY.get(industry.strip())
    if c: return c
    macro = industry.split("/")[0].strip()
    for k, v in _SW_TO_SCALAPAY.items():
        if k.startswith(macro + "/"): return v
    return "Other"

TIER_SCORE = {"GOLD": 25, "SILVER": 18, "BRONZE": 10, "UNKNOWN": 8, "EXCLUDE": 0}
# TTV-based account segmentation (Scalapay internal thresholds)
# Based on Est TTV Annual = Traffic × 2.5% × €85 AOV × 8% Scalapay share × 12
TTV_SEGMENT_THRESHOLDS = [
    (5_000_000, "Strategic"),      # > €5M TTV/year
    (500_000,   "Enterprise"),     # €500K – €5M TTV/year
    (0,         "Executive"),      # < €500K TTV/year
]
ACCOUNT_SIZE_SCORE = {"Strategic": 20, "Enterprise": 15, "Executive": 8, "Unknown": 4}

# Legacy: Similarweb Annual Revenue bucket mapping (kept for reference)
REVENUE_TIERS = {"> 1B": "Strategic", "500M - 1B": "Strategic", "200M - 500M": "Enterprise", "100M - 200M": "Enterprise", "75M - 100M": "Enterprise", "50M - 75M": "Enterprise", "25M - 50M": "Enterprise", "15M - 25M": "Enterprise", "10M - 15M": "Executive", "5M - 10M": "Executive", "2M - 5M": "Executive", "1M - 2M": "Executive", "0 - 1M": "Executive"}
# BNPL Penetration by Scalapay category AND country (from internal data)
# Used for BOTH scoring and TTV estimation
# Format: { category: { country: pct } } — PT uses ES values (Iberia)
BNPL_PENETRATION_BY_COUNTRY = {
    "Home & Garden":          {"IT": 5.2, "FR": 5.6, "ES": 5.4, "PT": 5.4},
    "Apparel & Fashion":      {"IT": 8.3, "FR": 9.2, "ES": 9.3, "PT": 9.3},
    "Wellness":               {"IT": 4.9, "FR": 5.7, "ES": 5.7, "PT": 5.7},
    "Medical":                {"IT": 4.9, "FR": 5.7, "ES": 5.7, "PT": 5.7},
    "Pharma":                 {"IT": 4.9, "FR": 5.7, "ES": 5.7, "PT": 5.7},
    "Dental":                 {"IT": 4.9, "FR": 5.7, "ES": 5.7, "PT": 5.7},
    "Learning & Classes":     {"IT":10.6, "FR": 5.6, "ES":14.8, "PT": 14.8},
    "Education":              {"IT":10.6, "FR": 5.6, "ES":14.8, "PT": 14.8},
    "Hobbies & Games":        {"IT": 7.7, "FR": 4.5, "ES": 5.7, "PT": 5.7},
    "Food & Beverage":        {"IT": 3.8, "FR": 5.1, "ES": 4.6, "PT": 4.6},
    "Other":                  {"IT": 5.3, "FR": 5.0, "ES": 5.6, "PT": 5.6},
    "Travel - OTA":                   {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Hotel & Accommodation": {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Tour Operator":         {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Local & Urban Transport": {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Adventure & Group Travel": {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Theme Parks":           {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Cruise":                {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Car Rental":            {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Entertainment & Experiences": {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Ticketing & Events":    {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Ferry & Maritime":      {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Wellness & Spa":        {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Travel - Other":                 {"IT": 3.6, "FR": 6.9, "ES": 5.3, "PT": 5.3},
    "Luxury Goods":           {"IT": 7.5, "FR": 6.3, "ES": 7.9, "PT": 7.9},
    "Sport":                  {"IT": 6.7, "FR": 8.9, "ES": 6.8, "PT": 6.8},
    "Cosmetics & Beauty":     {"IT": 8.4, "FR": 9.3, "ES": 8.1, "PT": 8.1},
    "Shoes & Accessories":    {"IT": 8.2, "FR": 4.9, "ES": 9.3, "PT": 9.3},
    "Petcare":                {"IT": 5.0, "FR": 8.7, "ES": 5.7, "PT": 5.7},
    "Veterinarians":          {"IT": 5.0, "FR": 8.7, "ES": 5.7, "PT": 5.7},
    "Electronics":            {"IT": 8.9, "FR": 9.1, "ES": 7.3, "PT": 7.3},
    "Electronics & Household appliance": {"IT": 8.9, "FR": 9.1, "ES": 7.3, "PT": 7.3},
    "Household Appliance":    {"IT": 8.9, "FR": 9.1, "ES": 7.3, "PT": 7.3},
    "Jewelry & Watches":      {"IT": 7.9, "FR": 5.7, "ES": 6.1, "PT": 6.1},
    "Glasses & Eyewear":      {"IT": 7.9, "FR": 5.7, "ES": 6.1, "PT": 6.1},
    "B2B":                    {"IT": 6.6, "FR": 5.1, "ES": 6.4, "PT": 6.4},
    "B2B Goods & Trade Materials": {"IT": 6.6, "FR": 5.1, "ES": 6.4, "PT": 6.4},
    "Auto & Moto":            {"IT": 5.1, "FR": 5.7, "ES": 4.7, "PT": 4.7},
    "Auto Repair Shops":      {"IT": 5.1, "FR": 5.7, "ES": 4.7, "PT": 4.7},
    "Baby & Toddler":         {"IT": 9.0, "FR": 5.1, "ES": 6.7, "PT": 6.7},
    "Generalist Marketplace": {"IT": 4.8, "FR": 5.4, "ES": 3.0, "PT": 3.0},
    "General Retail":         {"IT": 5.3, "FR": 5.0, "ES": 5.6, "PT": 5.6},
    "Professional Services":  {"IT": 5.3, "FR": 5.0, "ES": 5.6, "PT": 5.6},
    "Entertainment & Sports": {"IT": 6.7, "FR": 8.9, "ES": 6.8, "PT": 6.8},
}
DEFAULT_PENETRATION_PCT = 5.0

def get_penetration(scalapay_category: str, country: str) -> float:
    """Get BNPL penetration % for a category + country."""
    cat_data = BNPL_PENETRATION_BY_COUNTRY.get(scalapay_category)
    if not cat_data:
        return DEFAULT_PENETRATION_PCT
    return cat_data.get(country.upper(), cat_data.get("ES", DEFAULT_PENETRATION_PCT))

# Flat version for scoring (uses global average)
BNPL_PENETRATION_PCT = {cat: sum(v.values())/len(v) for cat, v in BNPL_PENETRATION_BY_COUNTRY.items()}

# AOV by Scalapay category (EUR, from internal data)
CATEGORY_AOV: Dict[str, float] = {
    "Jewelry & Watches": 136, "Electronics": 510, "Pharma": 64,
    "Dental": 1000, "Petcare": 87, "B2B": 100,
    "Cosmetics & Beauty": 120, "Apparel & Fashion": 99,
    "Shoes & Accessories": 119, "Wellness": 117, "Food & Beverage": 108,
    "Glasses & Eyewear": 192, "Other": 184, "Medical": 393,
    "Sport": 152, "Generalist Marketplace": 48, "General Retail": 94,
    "Professional Services": 515, "Education": 320,
    "Baby & Toddler": 151, "Home & Garden": 284,
    "Auto & Moto": 250, "Luxury Goods": 277, "Auto Repair Shops": 234,
    "Household Appliance": 339,
    "Travel - OTA": 640, "Travel - Hotel & Accommodation": 913,
    "Travel - Tour Operator": 1054, "Travel - Local & Urban Transport": 119,
    "Travel - Theme Parks": 172, "Travel - Cruise": 2577,
    "Travel - Car Rental": 183, "Travel - Entertainment & Experiences": 649,
    "Travel - Ticketing & Events": 120, "Travel - Adventure & Group Travel": 1231,
    "Travel - Ferry & Maritime": 323, "Travel - Wellness & Spa": 298,
    "Travel - Other": 599,
    "Hobbies & Games": 182,
    "Veterinarians": 571, "Entertainment & Sports": 99,
    "B2B Goods & Trade Materials": 283,
    "Electronics & Household appliance": 489, "Learning & Classes": 320,
}
DEFAULT_AOV_EUR = 120.0
SCORING_WEIGHTS = {"tier": 0.20, "account_size": 0.20, "penetration_ttv": 0.15, "traffic_growth": 0.15, "lead_warmth": 0.10, "competitor": 0.10, "whitespace": 0.10}
WARMTH_SCORES = {"Net New": 15, "Lost >6 months": 10, "Stale Deal": 8, "In HubSpot (unknown)": 6, "Lost <6 months": 3, "Warm (active)": 1, "Existing Won": 0, "Cold/Lost": 7, "Warm": 2}
CLOSED_LOST_REACTIVATION_MONTHS = 6
KNOWN_COMPETITORS = ["klarna","oney","heylight","paypal","alma","clearpay","afterpay","cofidis","pledg","floa","pagantis","sequra","aplazame"]
KNOWN_PSPS = ["stripe","adyen","checkout.com","worldpay","braintree","mollie","redsys","ingenico"]
# TTV ESTIMATION
# New formula: Visits × ecom_conversion × AOV(category) × BNPL_penetration(category, country) × 12
# Penetration replaces the old flat Scalapay share (8%)
TRAFFIC_TO_TRANSACTION_RATE = 0.025  # 2.5% of visits → transactions
WHITESPACE_CATEGORIES = ["Pharma","Dental","Medical","Wellness","Petcare","Veterinarians","Education","Food & Beverage","Glasses & Eyewear"]
HS_DEAL_STAGES_WON = ["closedwon","deal won","won"]
HS_DEAL_STAGES_LOST = ["closedlost","closed lost","lost"]
HS_DEAL_STAGES_WARM = ["discovery","negotiation","proposal","qualification"]
HS_CROSS_COUNTRY_PROPERTY = "cross_country_flag"

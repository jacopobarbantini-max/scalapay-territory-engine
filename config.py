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

# Subcategory AOV refinement — HubSpot subcategories with distinct AOV from parent
# Used as fallback when no HubSpot per-merchant AOV is available
SUBCATEGORY_AOV: Dict[str, float] = {
    # Apparel & Fashion
    "Fast Fashion": 65,
    "Mid Fashion": 120,
    "Premium Fashion": 220,
    "Underwear & Beachwear": 55,
    # Electronics & Household appliance
    "Smartphone": 700,
    "Pc & Electronics": 500,
    "Household Appliance": 350,
    "Sou": 200,  # Security & Automation
    # Home & Garden
    "Furniture": 450,
    "Mattresses & Bedding": 550,
    "Decoration & Lighting": 150,
    "Home Supplies": 100,
    "Garden": 180,
    "Bricolage": 120,
    # Cosmetics & Beauty
    "Body & Skincare": 80,
    "Make-up": 55,
    "Perfumery": 95,
    "Hair": 70,
    "Nails": 40,
    "Epilation": 200,
    # Shoes & Accessories
    "Shoes & Sneakers": 130,
    "Glasses & Eyewear": 192,
    "Bags, Luggage & Leather Goods": 180,
    "Accessories & Specialty Goods": 80,
    # Travel
    "Hotel & Resort": 913,
    "Tour Operator": 1054,
    "Entertainment & Experiences": 649,
    "Travel - Agency": 640,
    "Transportation": 119,
    # Hobbies & Games
    "Books": 35,
    "Music": 30,
    "Toys": 55,
    "Video Game": 60,
    # Luxury Goods
    "Luxury Clothes": 600,
    "Luxury Goods - Jewelry": 450,
    "Luxury Goods - Shoes & Sneakers": 500,
    "Luxury Goods - Watches": 800,
    "Bags & Luggage": 400,
    # Food
    "Food": 45,
    "Beverage": 35,
    # Baby
    "Kidswear": 80,
    "Baby Gear": 200,
    "Baby Miscellaneous": 50,
    # Sport
    "Sportswear & Equipment": 160,
    "Training Equipment": 250,
    # Health
    "Pharma": 64,
    "Medical": 393,
    "Supplements": 55,
    "Wellness": 117,
    # B2B
    "B2B - Goods": 150,
    "B2B - Services": 500,
}

def get_aov(category: str, subcategory: str = "", country: str = "",
            hs_aov_tot: float = 0, hs_aov_country: float = 0,
            hs_aov_benchmark: float = 0) -> tuple:
    """
    AOV cascade — returns (aov, source) tuple.
    Priority:
      1. HubSpot real per-merchant per-country AOV
      2. HubSpot real per-merchant total AOV
      3. HubSpot scalapay__aov benchmark
      4. Config subcategory AOV
      5. Config category AOV
    """
    if hs_aov_country and hs_aov_country > 5:
        return hs_aov_country, "HS-country"
    if hs_aov_tot and hs_aov_tot > 5:
        return hs_aov_tot, "HS-merchant"
    if hs_aov_benchmark and hs_aov_benchmark > 5:
        return hs_aov_benchmark, "HS-benchmark"
    if subcategory and subcategory in SUBCATEGORY_AOV:
        return SUBCATEGORY_AOV[subcategory], "subcat"
    if category and category in CATEGORY_AOV:
        return CATEGORY_AOV[category], "category"
    return DEFAULT_AOV_EUR, "default"
SCORING_WEIGHTS = {"tier": 0.20, "account_size": 0.20, "penetration_ttv": 0.15, "traffic_growth": 0.15, "lead_warmth": 0.10, "competitor": 0.10, "whitespace": 0.10}
WARMTH_SCORES = {"Net New": 15, "Lost >6 months": 10, "Stale Deal": 8, "In HubSpot (unknown)": 6, "Lost <6 months": 3, "Warm (active)": 1, "Existing Won": 0, "Cold/Lost": 7, "Warm": 2}
CLOSED_LOST_REACTIVATION_MONTHS = 6
KNOWN_COMPETITORS = ["klarna","oney","heylight","paypal","alma","clearpay","afterpay","cofidis","pledg","floa","pagantis","sequra","aplazame"]
KNOWN_PSPS = ["stripe","adyen","checkout.com","worldpay","braintree","mollie","redsys","ingenico"]
# TTV ESTIMATION
# New formula: Visits × ecom_conversion × AOV(category) × BNPL_penetration(category, country) × 12
# Penetration replaces the old flat Scalapay share (8%)
TRAFFIC_TO_TRANSACTION_RATE = 0.025  # Legacy fallback
WHITESPACE_CATEGORIES = ["Pharma","Dental","Medical","Wellness","Petcare","Veterinarians","Education","Food & Beverage","Glasses & Eyewear"]

# ═══════════════════════════════════════════════════════
# v5: CONVERSION RATE PER CATEGORY — derived from REAL SW data
# Source: 9,249 leads, Monthly Transactions (SW) / Monthly Traffic (SW), median per category
# Used ONLY as fallback when SW Monthly Transactions are unavailable
CATEGORY_CR: Dict[str, float] = {
    "Apparel & Fashion": 0.0060,  # n=918
    "Auto & Moto": 0.0057,  # n=445
    "B2B": 0.0058,  # n=1146
    "Baby & Toddler": 0.0066,  # n=155
    "Cosmetics & Beauty": 0.0120,  # n=468
    "Dental": 0.0059,  # n=9
    "Education": 0.0096,  # n=3
    "Electronics": 0.0051,  # n=629
    "Entertainment & Sports": 0.0055,  # n=536
    "Food & Beverage": 0.0086,  # n=651
    "General Retail": 0.0106,  # n=142
    "Generalist Marketplace": 0.0074,  # n=114
    "Hobbies & Games": 0.0065,  # n=177
    "Home & Garden": 0.0057,  # n=897
    "Household Appliance": 0.0058,  # n=147
    "Jewelry & Watches": 0.0056,  # n=266
    "Medical": 0.0125,  # n=90
    "Other": 0.0088,  # n=272
    "Petcare": 0.0107,  # n=439
    "Pharma": 0.0117,  # n=564
    "Shoes & Accessories": 0.0061,  # n=254
    "Sport": 0.0060,  # n=683
    "Travel - Adventure & Group Travel": 0.0062,  # n=13
    "Travel - Entertainment & Experiences": 0.0058,  # n=129
    "Travel - Hotel & Accommodation": 0.0074,  # n=4
    "Travel - Local & Urban Transport": 0.0082,  # n=28
    "Travel - OTA": 0.0052,  # n=30
    "Travel - Tour Operator": 0.0116,  # n=3
    "Wellness": 0.0094,  # n=37
}
DEFAULT_CR = 0.0062  # overall median

# ═══════════════════════════════════════════════════════
# v5: AOV VIABILITY — BNPL makes no sense below €20
# Multiplier on TTV: low AOV → low BNPL viability
# ═══════════════════════════════════════════════════════
def get_aov_viability(aov: float) -> float:
    """
    BNPL viability by AOV — calibrated on actual HubSpot TTV data (6,567 merchants).
    Shein (€90) = €323M TTV, Temu (€44) = €227M TTV → low AOV works at volume.
    Pay in X with Deutsche Bank makes >€500 fully viable.
    """
    if aov < 20:   return 0.15   # Delivery food — minimal BNPL value
    if aov < 40:   return 0.60   # Temu €44 = €227M TTV proves this works
    if aov < 70:   return 0.85   # Eco Bio €65 = €1.3M, Amica Farmacia €47 = €950K
    if aov < 100:  return 0.95   # Shein €90 = €323M, Douglas €83 = €13M
    if aov < 300:  return 1.00   # Sweet spot — ideal BNPL range
    if aov < 700:  return 1.00   # Pay in X makes this fully viable
    if aov < 1500: return 0.95   # Pay in X with Deutsche Bank
    return 0.90                   # Cruise/Tour — works with financing (Si Vola €2053 = €17.6M)

# ═══════════════════════════════════════════════════════
# v5: COMPETITION ADJUSTMENT — TAM to SAM
# How much TTV can Scalapay realistically capture given competition
# ═══════════════════════════════════════════════════════

# Regional market leaders (higher share = lower SAM for us)
_REGIONAL_LEADERS = {
    "FR": {"alma": 0.65, "klarna": 0.65, "oney": 0.75},     # Alma strong in FR but merchant adds Scalapay
    "ES": {"sequra": 0.65, "klarna": 0.65},                   # Sequra strong in ES but not lock-in
    "PT": {"sequra": 0.70, "klarna": 0.65},
    "IT": {"klarna": 0.65},                                    # Klarna in IT
}
_MINOR_PLAYERS = {"paypal", "cofidis", "pledg", "floa", "heylight", "pagantis", "aplazame"}
_DIRECT_COMPETITORS_SET = {"klarna", "alma", "sequra", "oney", "clearpay", "afterpay"}

def get_sam_factor(competitors_list: str, country: str, has_paypal: bool = False) -> float:
    """
    Competition adjustment: TAM × factor = realistic Scalapay TTV.
    Calibrated less punitively: merchants ADD BNPL providers, they don't replace.
    Graindemalice has both Alma + PayPal → room for Scalapay too.
    
    Returns 0.0-1.0:
      1.00 = no competition
      0.65-0.70 = one strong direct competitor (merchant still likely to add Scalapay)
      0.25 = 3+ saturated
    """
    comps = []
    if isinstance(competitors_list, str) and competitors_list.strip():
        comps = [c.strip().lower() for c in competitors_list.split(",") if c.strip()]
    n = len(comps)
    co = country.upper() if country else "FR"
    leaders = _REGIONAL_LEADERS.get(co, {})

    if n == 0:
        return 0.95 if has_paypal else 1.00
    elif n == 1:
        comp = comps[0]
        if comp in leaders:
            return leaders[comp]
        elif comp in _DIRECT_COMPETITORS_SET:
            return 0.65  # Direct but merchant diversifies
        else:
            return 0.85  # Minor player — easy to coexist
    elif n == 2:
        has_leader = any(c in leaders for c in comps)
        has_direct = any(c in _DIRECT_COMPETITORS_SET for c in comps)
        if has_leader:
            return 0.40  # Leader + another
        elif has_direct:
            return 0.45  # Two direct competitors
        else:
            return 0.55  # Two minor players
    else:
        return 0.25  # Saturated but never zero

# ═══════════════════════════════════════════════════════
# v5: BNPL WIDGET TEXT PATTERNS (context-aware matching)
# Used in enrichment Layer 4 (product pages) to detect BNPL
# without false positives on generic words
# ═══════════════════════════════════════════════════════
BNPL_WIDGET_PATTERNS = {
    "alma": [
        "avec alma", "paga con alma", "pay with alma", "powered by alma",
        "alma - pay", "alma pay", "almapay",
    ],
    "oney": [
        "avec oney", "paga con oney", "pay with oney", "powered by oney",
        "oney bank", "oney pay",
    ],
    "sequra": [
        "con sequra", "with sequra", "powered by sequra",
        "paga con sequra", "fracciona con sequra",
    ],
    "floa": [
        "avec floa", "powered by floa", "floa pay", "floa bank",
    ],
}

# Generic BNPL installment phrases → then check nearby brand
BNPL_INSTALLMENT_PHRASES = [
    "payez en 2x", "payez en 3x", "payez en 4x",
    "paga in 2 rate", "paga in 3 rate", "paga in 4 rate",
    "paga en 2 cuotas", "paga en 3 cuotas", "paga en 4 cuotas",
    "pay in 2", "pay in 3", "pay in 4",
    "3 fois sans frais", "4 fois sans frais",
    "3x sans frais", "4x sans frais",
    "sin intereses", "sans frais", "senza interessi",
    "split payment", "buy now pay later",
]

# ═══════════════════════════════════════════════════════
# v5: NON-E-COMMERCE WATCHLIST
# Platforms with high transaction volume but not in SW e-commerce data
# ═══════════════════════════════════════════════════════
NON_ECOMM_WATCHLIST = [
    {"domain": "whop.com", "name": "Whop", "category": "Digital Marketplace", "est_monthly_txns": 500000},
    {"domain": "store.playstation.com", "name": "PlayStation Store", "category": "Digital Games", "est_monthly_txns": 2000000},
    {"domain": "store.steampowered.com", "name": "Steam", "category": "Digital Games", "est_monthly_txns": 5000000},
    {"domain": "eventbrite.com", "name": "Eventbrite", "category": "Ticketing", "est_monthly_txns": 300000},
    {"domain": "ticketmaster.com", "name": "Ticketmaster", "category": "Ticketing", "est_monthly_txns": 1000000},
    {"domain": "udemy.com", "name": "Udemy", "category": "Digital Education", "est_monthly_txns": 500000},
    {"domain": "skillshare.com", "name": "Skillshare", "category": "Digital Education", "est_monthly_txns": 200000},
    {"domain": "patreon.com", "name": "Patreon", "category": "Creator Economy", "est_monthly_txns": 300000},
    {"domain": "gumroad.com", "name": "Gumroad", "category": "Creator Economy", "est_monthly_txns": 100000},
    {"domain": "etsy.com", "name": "Etsy", "category": "Marketplace", "est_monthly_txns": 3000000},
]

# HubSpot properties for cross-country traffic data
HS_COUNTRY_VISITS = {
    "FR": "fr___annual_visits",
    "ES": "es___annual_visits",
    "IT": "it___annual_visits",
}
HS_COUNTRY_MONTHLY_VISITS = {
    "FR": "fr___monthly_visits",
    "ES": "es___monthly_visits",
    "IT": "it___monthly_visits",
}
HS_DEAL_STAGES_WON = ["closedwon","deal won","won","onboarding completed","integration completed"]
HS_DEAL_STAGES_LOST = ["closedlost","closed lost","lost","not interested","churn","terminated"]
HS_DEAL_STAGES_WARM = ["discovery","negotiation","proposal","qualification","sql",
    "discovery meetings","business meeting","proposal sent",
    "validation & negotiation","final proposal to eb","in discussion",
    "inbound created","target","nbm pending review",
    "kyc pending approval","onboarding initiated","contract signed"]

# Stage-aware stale deal thresholds (days without contact, holiday-adjusted)
# Early stage: if no contact in 30d → dead
# Mid stage: if no contact in 45d → check with AE
# Late stage: if no contact in 60d → possibly waiting (legal, KYC)
HS_STALE_THRESHOLDS = {
    "early":  30,  # SQL, Inbound Created, Target, Discovery meetings
    "mid":    45,  # Business Meeting, Proposal sent, Negotiation, In discussion
    "late":   60,  # KYC Pending, Onboarding, Contract signed, Final proposal
}
HS_EARLY_STAGES = {"sql","inbound created","target","discovery meetings","discovery","qualification"}
HS_LATE_STAGES = {"kyc pending approval","onboarding initiated",
    "contract signed","final proposal to eb","validation & negotiation"}
HS_CROSS_COUNTRY_PROPERTY = "cross_country_flag"

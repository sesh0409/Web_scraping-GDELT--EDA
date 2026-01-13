!pip install requests pandas tenacity newspaper3k lxml trafilatura newspaper3k
!pip install lxml[html_clean] || pip install lxml_html_clean

import os
import re
import json
import time
import random
import hashlib
import tempfile
from datetime import datetime
from time import perf_counter
from urllib.parse import urlparse

import requests
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# =====================================================
# LIB AVAILABILITY
# =====================================================
TRAFILATURA_AVAILABLE = True
NEWSPAPER_AVAILABLE = True
LANGDETECT_AVAILABLE = True

try:
    import trafilatura
except Exception:
    TRAFILATURA_AVAILABLE = False

try:
    from newspaper import Article
except Exception:
    NEWSPAPER_AVAILABLE = False

try:
    from langdetect import detect
except Exception:
    LANGDETECT_AVAILABLE = False


# =====================================================
# CONFIG
# =====================================================
START_DATE_STR = "2020-01-01"
END_DATE_STR   = "2025-12-01"

START_DT = "20200101000000"
END_DT   = "20251201235959"

# ---- IMPORTANT: reduce these initially to avoid throttling ----
MAXRECORDS_DOC = 250               # 250 is safer
MAX_PAGES_PER_TICKER = 10          # was 20; increase after stable

MAX_URLS_PER_TICKER = 600
MAX_WORKERS_SCRAPE = 8             # reduce concurrency slightly
REQUEST_TIMEOUT = 45               # give gateway more time
BASE_SLEEP = 3.0                   # was 1.5; slower avoids throttle

MIN_BODY_CHARS = 450
DROP_IF_LANG_NOT_EN = True
GDELT_LANGUAGE_FILTER = "english"

CACHE_DIR = os.path.join(os.environ.get("LOCALAPPDATA", os.getcwd()), "StockNewsCache")
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_PATH = os.path.join(CACHE_DIR, "_url_text_cache_fast_v5.csv")
OUT_PATH = r"C:\Users\Dell\OneDrive\Documents\IIMV -MBA\IIMV_Capstone_Project\StockNews_GDELT_SCRAPED_20200101_20251201_V5.csv"
RUNTIME_SUMMARY_PATH = os.path.join(CACHE_DIR, f"_ticker_runtime_{START_DATE_STR}_{END_DATE_STR}.csv")

# Debug folder to save HTML error pages from GDELT
DEBUG_DIR = os.path.join(CACHE_DIR, "gdelt_debug")
os.makedirs(DEBUG_DIR, exist_ok=True)


# =====================================================
# GDELT ENDPOINT
# =====================================================
GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"


# =====================================================
# INPUT: TICKERS (exactly as you provided)
# =====================================================
TICKERS = [
    "MSFT","AAPL","GOOGL","AMZN","META","NVDA","ORCL","IBM","ADBE","CRM",
    "INTC","AMD","QCOM","CSCO","AVGO","TXN","NOW","SNOW","NFLX","PYPL",
    "INTU","AMAT","MU","LRCX","KLAC","ON","MSI","DELL","HPQ","HPE",
    "PANW","CRWD","ZS","OKTA","UBER","VOLVY","MBGYY","RACE","F","LYFT",
    "TESLA","HSBC","C","SCBFY","JPM","BAC","WFC","GS","MS",
    "BNPQY","DB","UBS","BCS","SAN","HDB","ICICIBANK","SBIN","MUFG","SMFG",
    "INFY","TCS","WIPRO",
    "SAP","ASML","NOK","ERIC","STM","LOGI","ADYEN",
    "SONY","NTTYY","TSM","BABA","JD","BIDU","TCEHY","SE","GRAB","TM","HMC",
    "RELIANCE","LT","ITC","HINDUNILVR","BHARTIARTL",
    "AXISBANK","KOTAKBANK","INDUSINDBK","BAJFINANCE","BAJAJFINSV",
    "MARUTI","TATAMOTORS","TATASTEEL","JSWSTEEL","EICHERMOT",
    "HEROMOTOCO","ULTRACEMCO","GRASIM","SHREECEM",
    "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP",
    "NTPC","POWERGRID","ONGC","IOC","BPCL",
    "COALINDIA","ADANIENT","ADANIPORTS",
    "ASIANPAINT","NESTLEIND","BRITANNIA","TATACONSUM",
    "HCLTECH","TECHM","COST","WMT","TSCO",
    "UPL","HAVELLS","GODREJCP","PIDILITIND",
    "DMART","ICICIPRULI","SBILIFE"
]


# =====================================================
# COMPANY META
# IMPORTANT: paste your FULL COMPANY_META dict here (exactly as you have it)
# =====================================================
COMPANY_META = {
    "MSFT": ("Microsoft Corporation", "United States", "North America", "Enterprise Software & Cloud"),
    "AAPL": ("Apple Inc.", "United States", "North America", "Consumer Technology"),
    "GOOGL": ("Alphabet Inc.", "United States", "North America", "Internet & Digital Advertising"),
    "AMZN": ("Amazon.com, Inc.", "United States", "North America", "E-commerce & Cloud Computing"),
    "META": ("Meta Platforms, Inc.", "United States", "North America", "Social Media & Digital Platforms"),
    "NVDA": ("NVIDIA Corporation", "United States", "North America", "Semiconductors & AI Hardware"),
    "ORCL": ("Oracle Corporation", "United States", "North America", "Enterprise Software & Databases"),
    "IBM": ("International Business Machines Corporation", "United States", "North America", "IT Services & Enterprise Solutions"),
    "ADBE": ("Adobe Inc.", "United States", "North America", "Creative & Digital Media Software"),
    "CRM": ("Salesforce, Inc.", "United States", "North America", "Cloud-based CRM Software"),
    "INTC": ("Intel Corporation", "United States", "North America", "Semiconductors"),
    "AMD": ("Advanced Micro Devices, Inc.", "United States", "North America", "Semiconductors"),
    "QCOM": ("Qualcomm Incorporated", "United States", "North America", "Wireless Semiconductors"),
    "CSCO": ("Cisco Systems, Inc.", "United States", "North America", "Networking & Infrastructure"),
    "AVGO": ("Broadcom Inc.", "United States", "North America", "Semiconductors & Infrastructure Software"),
    "TXN": ("Texas Instruments Incorporated", "United States", "North America", "Analog Semiconductors"),
    "NOW": ("ServiceNow, Inc.", "United States", "North America", "Enterprise Workflow Software"),
    "SNOW": ("Snowflake Inc.", "United States", "North America", "Cloud Data Platforms"),
    "NFLX": ("Netflix, Inc.", "United States", "North America", "Streaming & Digital Entertainment"),
    "PYPL": ("PayPal Holdings, Inc.", "United States", "North America", "Digital Payments & FinTech"),
    "INTU": ("Intuit Inc.", "United States", "North America", "Financial & Tax Software"),
    "AMAT": ("Applied Materials, Inc.", "United States", "North America", "Semiconductor Equipment"),
    "MU":   ("Micron Technology, Inc.", "United States", "North America", "Memory Semiconductors"),
    "LRCX": ("Lam Research Corporation", "United States", "North America", "Semiconductor Equipment"),
    "KLAC": ("KLA Corporation", "United States", "North America", "Semiconductor Process Control"),
    "ON":   ("ON Semiconductor Corporation", "United States", "North America", "Power & Automotive Semiconductors"),
    "MSI": ("Motorola Solutions, Inc.", "United States", "North America", "Enterprise Communications"),
    "DELL": ("Dell Technologies Inc.", "United States", "North America", "Enterprise Hardware & Services"),
    "HPQ": ("HP Inc.", "United States", "North America", "Computer Hardware"),
    "HPE": ("Hewlett Packard Enterprise", "United States", "North America", "Enterprise Infrastructure"),
    "PANW": ("Palo Alto Networks, Inc.", "United States", "North America", "Cybersecurity"),
    "CRWD": ("CrowdStrike Holdings, Inc.", "United States", "North America", "Cybersecurity"),
    "ZS": ("Zscaler, Inc.", "United States", "North America", "Cloud Security"),
    "OKTA": ("Okta, Inc.", "United States", "North America", "Identity & Access Management"),
    "UBER": ("Uber Technologies, Inc.", "United States", "North America", "Digital Mobility Platform"),

    "VOLVY": ("Volvo AB", "Sweden", "EMEA", "Automotive & Mobility Technology"),
    "MBGYY": ("Mercedes-Benz Group AG", "Germany", "EMEA", "Automotive & Mobility Technology"),
    "RACE": ("Ferrari N.V.", "Italy", "EMEA", "Luxury Automotive & Performance Technology"),
    "F": ("Ford Motor Company", "United States", "North America", "Automotive & Mobility Technology"),
    "LYFT": ("Lyft, Inc.", "United States", "North America", "Digital Mobility Platform"),
    "TESLA": ("Tesla, Inc.", "United States", "North America", "Electric Vehicles & Clean Energy"),

    "HSBC": ("HSBC Holdings plc", "United Kingdom", "EMEA", "Global Banking & Financial Services"),
    "C": ("Citigroup Inc.", "United States", "North America", "Global Banking & Financial Services"),
    "SCBFY": ("Standard Chartered PLC", "United Kingdom", "EMEA", "International Banking & Financial Services"),

    "JPM": ("JPMorgan Chase & Co.", "United States", "North America", "Investment Banking & Financial Services"),
    "BAC": ("Bank of America Corporation", "United States", "North America", "Retail & Investment Banking"),
    "WFC": ("Wells Fargo & Company", "United States", "North America", "Retail & Commercial Banking"),
    "GS": ("The Goldman Sachs Group, Inc.", "United States", "North America", "Investment Banking & Asset Management"),
    "MS": ("Morgan Stanley", "United States", "North America", "Investment Banking & Wealth Management"),

    "BNPQY": ("BNP Paribas SA", "France", "EMEA", "Universal Banking & Financial Services"),
    "DB": ("Deutsche Bank AG", "Germany", "EMEA", "Investment Banking & Financial Services"),
    "UBS": ("UBS Group AG", "Switzerland", "EMEA", "Wealth Management & Investment Banking"),
    "BCS": ("Barclays PLC", "United Kingdom", "EMEA", "Retail & Investment Banking"),
    "SAN": ("Banco Santander, S.A.", "Spain", "EMEA", "Retail & Commercial Banking"),

    "HDB": ("HDFC Bank Limited (ADR)", "India", "APAC", "Retail & Commercial Banking"),
    "ICICIBANK": ("ICICI Bank Limited (ADR)", "India", "APAC", "Retail & Corporate Banking"),
    "SBIN": ("State Bank of India (ADR)", "India", "APAC", "Public Sector Banking"),
    "MUFG": ("Mitsubishi UFJ Financial Group, Inc.", "Japan", "APAC", "Global Banking & Financial Services"),
    "SMFG": ("Sumitomo Mitsui Financial Group, Inc.", "Japan", "APAC", "Corporate & Retail Banking"),

    "INFY": ("Infosys Limited", "India", "APAC", "IT Services & Consulting"),
    "TCS": ("Tata Consultancy Services Limited", "India", "APAC", "IT Services & Consulting"),
    "WIPRO": ("Wipro Limited", "India", "APAC", "IT Services & Consulting"),

    "SAP": ("SAP SE", "Germany", "EMEA", "Enterprise Application Software"),
    "ASML": ("ASML Holding N.V.", "Netherlands", "EMEA", "Semiconductor Equipment"),
    "NOK": ("Nokia Corporation", "Finland", "EMEA", "Telecom Equipment & Networks"),
    "ERIC": ("Telefonaktiebolaget LM Ericsson", "Sweden", "EMEA", "Telecom Infrastructure"),
    "STM": ("STMicroelectronics N.V.", "Italy", "EMEA", "Semiconductors"),
    "LOGI": ("Logitech International S.A.", "Switzerland", "EMEA", "Consumer & Enterprise Hardware"),
    "ADYEN": ("Adyen N.V.", "Netherlands", "EMEA", "Payments & FinTech"),

    "SONY": ("Sony Group Corporation", "Japan", "APAC", "Electronics & Digital Entertainment"),
    "NTTYY": ("NTT Data Corporation", "Japan", "APAC", "IT Services & Systems Integration"),
    "TSM": ("Taiwan Semiconductor Manufacturing Company Limited", "Taiwan", "APAC", "Semiconductor Foundry"),
    "BABA": ("Alibaba Group Holding Limited", "China", "APAC", "E-commerce & Cloud Computing"),
    "JD": ("JD.com, Inc.", "China", "APAC", "Technology-driven Retail"),
    "BIDU": ("Baidu, Inc.", "China", "APAC", "AI & Internet Services"),
    "TCEHY": ("Tencent Holdings Limited", "Hong Kong", "APAC", "Internet Platforms & Gaming"),
    "SE": ("Sea Limited", "Singapore", "APAC", "E-commerce & Digital Entertainment"),
    "GRAB": ("Grab Holdings Ltd.", "Singapore", "APAC", "Super App & Digital Mobility"),
    "TM": ("Toyota Motor Corporation", "Japan", "APAC", "Automotive Technology"),
    "HMC": ("Honda Motor Co., Ltd.", "Japan", "APAC", "Automotive Technology"),

    # Conglomerates & Industrials
    "RELIANCE": ("Reliance Industries Limited", "India", "APAC", "Energy, Telecom & Digital Services"),
    "LT": ("Larsen & Toubro Limited", "India", "APAC", "Engineering & Infrastructure"),
    "ADANIENT": ("Adani Enterprises Limited", "India", "APAC", "Infrastructure & Energy"),
    "ADANIPORTS": ("Adani Ports and SEZ Limited", "India", "APAC", "Ports & Logistics"),

    # FMCG & Consumer
    "ITC": ("ITC Limited", "India", "APAC", "FMCG & Consumer Goods"),
    "HINDUNILVR": ("Hindustan Unilever Limited", "India", "APAC", "FMCG"),
    "BHARTIARTL": ("Bharti Airtel Limited","India","APAC","Telecommunications & Digital Services"),
    "NESTLEIND": ("Nestlé India Limited", "India", "APAC", "Food & Beverages"),
    "BRITANNIA": ("Britannia Industries Limited", "India", "APAC", "Packaged Foods"),
    "TATACONSUM": ("Tata Consumer Products Limited", "India", "APAC", "Consumer Products"),
    "GODREJCP": ("Godrej Consumer Products Limited", "India", "APAC", "Personal & Home Care"),
    "DMART": ("Avenue Supermarts Limited", "India", "APAC", "Retail & Supermarkets"),

    # Banking & Financial Services
    "AXISBANK": ("Axis Bank Limited", "India", "APAC", "Private Sector Banking"),
    "KOTAKBANK": ("Kotak Mahindra Bank Limited", "India", "APAC", "Private Banking & Financial Services"),
    "INDUSINDBK": ("IndusInd Bank Limited", "India", "APAC", "Commercial Banking"),
    "BAJFINANCE": ("Bajaj Finance Limited", "India", "APAC", "NBFC & Consumer Finance"),
    "BAJAJFINSV": ("Bajaj Finserv Limited", "India", "APAC", "Financial Services Holding"),
    "SBILIFE": ("SBI Life Insurance Company Limited", "India", "APAC", "Life Insurance"),
    "ICICIPRULI": ("ICICI Prudential Life Insurance Company Limited", "India", "APAC", "Life Insurance"),

    # IT & Digital Services
    "HCLTECH": ("HCL Technologies Limited", "India", "APAC", "IT Services & Digital Transformation"),
    "TECHM": ("Tech Mahindra Limited", "India", "APAC", "IT Services & Telecom Solutions"),
    "COST": ("Costco Wholesale Corporation","United States","North America","Warehouse Retail & Membership-based Wholesale"),
    "WMT": ("Walmart Inc.","United States","North America","Mass Merchandising & Omnichannel Retail"),
    "TSCO": ("Tesco PLC (ADR)","United Kingdom","EMEA","Grocery Retail & Supermarkets"),
 
    # Automobiles
    "MARUTI": ("Maruti Suzuki India Limited", "India", "APAC", "Passenger Vehicles"),
    "TATAMOTORS": ("Tata Motors Limited", "India", "APAC", "Automotive & EVs"),
    "EICHERMOT": ("Eicher Motors Limited", "India", "APAC", "Premium Motorcycles"),
    "HEROMOTOCO": ("Hero MotoCorp Limited", "India", "APAC", "Two-Wheeler Manufacturing"),
    

    # Metals, Cement & Materials
    "TATASTEEL": ("Tata Steel Limited", "India", "APAC", "Steel Manufacturing"),
    "JSWSTEEL": ("JSW Steel Limited", "India", "APAC", "Steel Manufacturing"),
    "ULTRACEMCO": ("UltraTech Cement Limited", "India", "APAC", "Cement & Building Materials"),
    "GRASIM": ("Grasim Industries Limited", "India", "APAC", "Diversified Manufacturing"),
    "SHREECEM": ("Shree Cement Limited", "India", "APAC", "Cement Manufacturing"),
    "PIDILITIND": ("Pidilite Industries Limited", "India", "APAC", "Adhesives & Specialty Chemicals"),

    # Pharma & Healthcare
    "SUNPHARMA": ("Sun Pharmaceutical Industries Limited", "India", "APAC", "Pharmaceuticals"),
    "DRREDDY": ("Dr. Reddy’s Laboratories Limited", "India", "APAC", "Pharmaceuticals"),
    "CIPLA": ("Cipla Limited", "India", "APAC", "Respiratory & Specialty Pharma"),
    "DIVISLAB": ("Divi’s Laboratories Limited", "India", "APAC", "Pharma APIs"),
    "APOLLOHOSP": ("Apollo Hospitals Enterprise Limited", "India", "APAC", "Healthcare Services"),

    # Energy & Utilities
    "NTPC": ("NTPC Limited", "India", "APAC", "Power Generation"),
    "POWERGRID": ("Power Grid Corporation of India Limited", "India", "APAC", "Power Transmission"),
    "ONGC": ("Oil and Natural Gas Corporation Limited", "India", "APAC", "Oil & Gas Exploration"),
    "IOC": ("Indian Oil Corporation Limited", "India", "APAC", "Oil Refining & Marketing"),
    "BPCL": ("Bharat Petroleum Corporation Limited", "India", "APAC", "Oil Refining & Marketing"),
    "COALINDIA": ("Coal India Limited", "India", "APAC", "Coal Mining"),

    # Consumer Durables & Chemicals
    "ASIANPAINT": ("Asian Paints Limited", "India", "APAC", "Decorative Paints"),
    "HAVELLS": ("Havells India Limited", "India", "APAC", "Electrical Equipment"),
    "UPL": ("UPL Limited", "India", "APAC", "Agrochemicals"),
}


# =====================================================
# SESSION
# =====================================================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
})


# =====================================================
# Helpers: safety + cleaning + cache
# =====================================================
class NonJSONResponse(Exception):
    pass

def cooldown(mult=1.0):
    time.sleep((BASE_SLEEP * mult) + random.uniform(0.4, 1.2))

def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_text_field(s: str) -> str:
    s = normalize_whitespace(str(s or ""))
    s = re.sub(r"\.{3,}", "...", s)
    s = re.sub(r"[-–—]{2,}", "-", s)
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)
    s = re.sub(r"([,.;:!?])\s+", r"\1 ", s)
    return s.strip()

def clean_url(u: str) -> str:
    u = (u or "").strip()
    u = re.sub(r"#.*$", "", u)
    return u

def url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest()

def atomic_write_csv(df: pd.DataFrame, path: str):
    folder = os.path.dirname(path) or "."
    os.makedirs(folder, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="tmp_", suffix=".csv", dir=folder)
    os.close(fd)
    try:
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

def save_cache(cache: dict, path: str, max_rows: int = 150000):
    if not cache:
        return
    items = list(cache.items())[:max_rows]
    df = pd.DataFrame(items, columns=["url_hash", "body_text"])
    atomic_write_csv(df, path)

def load_cache_csv(path: str, max_rows: int = 250000) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
        if "url_hash" not in df.columns or "body_text" not in df.columns:
            return {}
        df = df.head(max_rows)
        return {str(h): str(t) for h, t in zip(df["url_hash"], df["body_text"])}
    except Exception:
        return {}

def is_probably_english(text: str) -> bool:
    if not text:
        return False
    if LANGDETECT_AVAILABLE:
        try:
            return detect(text[:1200]) == "en"
        except Exception:
            return False
    sample = text[:1500]
    ascii_chars = sum(1 for c in sample if ord(c) < 128)
    return (ascii_chars / max(1, len(sample))) > 0.9

def save_debug_html(ticker: str, r: requests.Response):
    """Save the HTML page from GDELT to inspect why it isn't JSON."""
    try:
        stamp = int(time.time())
        fn = os.path.join(DEBUG_DIR, f"gdelt_{ticker}_{stamp}_{r.status_code}.html")
        with open(fn, "w", encoding="utf-8", errors="ignore") as f:
            f.write(r.text or "")
        print(f"[DEBUG] Saved HTML response to: {fn}")
    except Exception:
        pass

def safe_json_response(ticker: str, r: requests.Response) -> dict:
    txt = (r.text or "").strip()
    if not txt:
        raise NonJSONResponse("Empty response body")

    ctype = (r.headers.get("Content-Type", "") or "").lower()

    # If HTML, save it for diagnosis
    if "html" in ctype or txt.startswith("<"):
        save_debug_html(ticker, r)
        raise NonJSONResponse("HTML response (throttle/gateway/proxy)")

    try:
        data = r.json()
    except Exception:
        try:
            data = json.loads(txt)
        except Exception:
            raise NonJSONResponse("Failed to parse JSON")

    if not isinstance(data, dict):
        raise NonJSONResponse("JSON is not a dict")
    return data


# =====================================================
# Query Builder
# =====================================================
def build_query(ticker: str, company_name: str) -> str:
    finance = "(stock OR shares OR earnings OR revenue OR profit OR guidance OR outlook OR market OR investors OR dividend OR acquisition OR merger OR results)"
    return f'("{company_name}" OR {ticker}) AND {finance}'


# =====================================================
# GDELT DOC Fetch (slower + stronger retry)
# =====================================================
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=5, max=180),
    retry=retry_if_exception_type((requests.RequestException, NonJSONResponse)),
    reraise=True
)
def gdelt_doc_fetch(ticker: str, query: str, startrecord: int, maxrecords: int) -> dict:
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "startdatetime": START_DT,
        "enddatetime": END_DT,
        "sort": "datedesc",
        "maxrecords": maxrecords,
        "startrecord": startrecord,
        "formatdatetime": "true",
        "lang": GDELT_LANGUAGE_FILTER,
    }

    cooldown(1.0)  # IMPORTANT: slow down every call

    r = session.get(GDELT_DOC, params=params, timeout=REQUEST_TIMEOUT)

    # If server says throttle, treat it as retryable
    if r.status_code in (429, 500, 502, 503, 504):
        save_debug_html(ticker, r)
        raise requests.HTTPError(f"GDELT temporary status {r.status_code}", response=r)

    r.raise_for_status()
    return safe_json_response(ticker, r)


# =====================================================
# Scrape body text
# =====================================================
@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    retry=retry_if_exception_type((requests.RequestException,)),
    reraise=True
)
def fetch_url_html(url: str) -> str:
    headers = {
        "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text or ""

def extract_body_text(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if not host or url.lower().endswith(".pdf"):
            return ""
    except Exception:
        pass

    if TRAFILATURA_AVAILABLE:
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                text = trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
                text = normalize_whitespace(text)
                if len(text) >= MIN_BODY_CHARS:
                    return text
        except Exception:
            pass

    if NEWSPAPER_AVAILABLE:
        try:
            art = Article(url, language="en")
            art.download()
            art.parse()
            text = normalize_whitespace(art.text or "")
            if len(text) >= MIN_BODY_CHARS:
                return text
        except Exception:
            pass

    if TRAFILATURA_AVAILABLE:
        try:
            html = fetch_url_html(url)
            text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""
            text = normalize_whitespace(text)
            if len(text) >= MIN_BODY_CHARS:
                return text
        except Exception:
            pass

    return ""

def scrape_one_url(url: str, cache: dict) -> tuple[str, str]:
    key = url_key(url)
    if key in cache:
        return key, cache[key]

    body = extract_body_text(url)

    if DROP_IF_LANG_NOT_EN and body:
        if not is_probably_english(body):
            body = ""

    if body:
        cache[key] = body

    return key, body


# =====================================================
# SMOKE TEST (VERY IMPORTANT)
# =====================================================
def gdelt_smoke_test():
    """One simple request to verify we get JSON from GDELT at all."""
    print("\n[SMOKE TEST] Testing GDELT JSON response...")
    test_query = '"Microsoft" AND stock'
    try:
        data = gdelt_doc_fetch("SMOKETEST", test_query, startrecord=1, maxrecords=5)
        arts = data.get("articles", [])
        print(f"[SMOKE TEST] OK ✅  Articles returned: {len(arts)}")
        if arts:
            print("[SMOKE TEST] Example URL:", arts[0].get("url", "")[:120])
        return True
    except Exception as e:
        print("[SMOKE TEST] FAILED ❌", e)
        print(f"[SMOKE TEST] Check debug HTML folder: {DEBUG_DIR}")
        return False


# =====================================================
# MAIN PIPELINE
# =====================================================
def build_gdelt_dataset_with_body_parallel(tickers: list[str]) -> pd.DataFrame:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    url_cache = load_cache_csv(CACHE_PATH)
    rows = []
    ticker_stats = []

    for t in tickers:
        if t not in COMPANY_META:
            print(f"[SKIP] Missing meta for {t}")
            continue

        company_name, country, region, sector = COMPANY_META[t]
        query = build_query(t, company_name)

        print(f"\n=== {t} | {company_name} ===")

        t0 = perf_counter()
        start_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        all_articles = []
        startrecord = 1
        pages = 0

        while pages < MAX_PAGES_PER_TICKER:
            try:
                data = gdelt_doc_fetch(t, query=query, startrecord=startrecord, maxrecords=MAXRECORDS_DOC)
                arts = data.get("articles", [])
                if not arts:
                    break

                all_articles.extend(arts)

                pages += 1
                startrecord += MAXRECORDS_DOC

                # extra cooldown between pages to avoid throttle
                cooldown(1.7)

                if len(all_articles) >= MAX_URLS_PER_TICKER:
                    break

            except Exception as e:
                print(f"[WARN] GDELT DOC failed for {t} (startrecord={startrecord}): {e}")
                break

        if not all_articles:
            elapsed = perf_counter() - t0
            ticker_stats.append({
                "ticker": t,
                "company_name": company_name,
                "start_ts_local": start_local,
                "elapsed_seconds": round(elapsed, 2),
                "elapsed_minutes": round(elapsed / 60, 2),
                "urls_found": 0,
                "urls_scraped_ok": 0,
                "rows_added": 0,
                "pages_attempted": pages,
            })
            print(f"[TIME] {t} finished in {elapsed/60:.2f} min | urls=0")
            continue

        df = pd.DataFrame(all_articles)

        # Datetime parsing
        if "seendate" in df.columns:
            dt = pd.to_datetime(df["seendate"], errors="coerce", utc=True)
            if dt.isna().mean() > 0.5:
                dt = pd.to_datetime(df["seendate"].astype(str), errors="coerce", utc=True)
            df["datetime"] = dt
        else:
            df["datetime"] = pd.NaT

        df = df.dropna(subset=["datetime"])
        df["date"] = df["datetime"].dt.date

        for col in ["title", "url", "description", "excerpt"]:
            if col not in df.columns:
                df[col] = ""

        # ---------- Cleaning (headlines/snippets/urls) ----------
        df["headline"] = df["title"].fillna("").astype(str).map(clean_text_field)
        desc = df["description"].fillna("").astype(str).map(clean_text_field)
        excp = df["excerpt"].fillna("").astype(str).map(clean_text_field)
        df["snippet"] = desc.where(desc.str.len() > 0, excp)

        df["url"] = df["url"].fillna("").astype(str).map(clean_url)
        df = df[df["url"].str.startswith("http")]
        df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)

        urls = df["url"].tolist()
        urls_found = len(urls)

        # ---------- Scrape bodies ----------
        url_to_hash = {u: url_key(u) for u in urls}
        url_hashes = [url_to_hash[u] for u in urls]
        to_scrape = [u for u in urls if url_to_hash[u] not in url_cache]

        if to_scrape:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_SCRAPE) as ex:
                futures = [ex.submit(scrape_one_url, u, url_cache) for u in to_scrape]
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception:
                        pass

        body_texts = [url_cache.get(h, "") for h in url_hashes]
        df["body_text"] = pd.Series(body_texts).astype(str).map(clean_text_field)

        df["body_len"] = df["body_text"].fillna("").astype(str).str.len()
        df = df[df["body_len"] >= MIN_BODY_CHARS].reset_index(drop=True)

        urls_scraped_ok = int(df["url"].nunique())

        # ---------- Add meta ----------
        df["source"] = "GDELT_DOC+SCRAPED"
        df["ticker"] = t
        df["Company_Name"] = company_name
        df["Country"] = country
        df["Region"] = region
        df["Sector"] = sector

        keep = [
            "source","ticker","date","datetime","headline","snippet","url",
            "body_text","body_len",
            "Company_Name","Country","Region","Sector"
        ]
        df_out = df[keep].copy()
        rows_added = len(df_out)

        if rows_added:
            rows.append(df_out)

        # Save cache
        try:
            save_cache(url_cache, CACHE_PATH)
        except Exception as e:
            print("[WARN] Cache save failed:", e)

        elapsed = perf_counter() - t0
        ticker_stats.append({
            "ticker": t,
            "company_name": company_name,
            "start_ts_local": start_local,
            "elapsed_seconds": round(elapsed, 2),
            "elapsed_minutes": round(elapsed / 60, 2),
            "urls_found": urls_found,
            "urls_scraped_ok": urls_scraped_ok,
            "rows_added": rows_added,
            "pages_attempted": pages,
        })
        print(f"[TIME] {t} finished in {elapsed/60:.2f} min | urls_found={urls_found} | ok={urls_scraped_ok} | rows={rows_added} | pages={pages}")

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=[
        "source","ticker","date","datetime","headline","snippet","url",
        "body_text","body_len",
        "Company_Name","Country","Region","Sector"
    ])

    # Global clean + dedupe
    if len(out):
        out["url"] = out["url"].astype(str).str.strip()
        out["headline"] = out["headline"].astype(str).map(clean_text_field)
        out["snippet"] = out["snippet"].astype(str).map(clean_text_field)
        out["body_text"] = out["body_text"].astype(str).map(clean_text_field)
        out = out.drop_duplicates(subset=["ticker", "url"], keep="first")
        out["body_len"] = out["body_text"].fillna("").astype(str).str.len()

    stats_df = pd.DataFrame(ticker_stats)
    if len(out) and len(stats_df):
        out = out.merge(
            stats_df[["ticker","elapsed_seconds","elapsed_minutes","urls_found","urls_scraped_ok","pages_attempted"]],
            on="ticker", how="left"
        )

    try:
        pd.DataFrame(ticker_stats).to_csv(RUNTIME_SUMMARY_PATH, index=False)
        print("✅ Runtime summary saved:", RUNTIME_SUMMARY_PATH)
    except Exception as e:
        print("[WARN] Could not save runtime summary:", e)

    return out.sort_values(["ticker","datetime"]).reset_index(drop=True)


# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    print("Cache dir:", CACHE_DIR)
    print("Cache path:", CACHE_PATH)
    print("Scrapers: trafilatura=", TRAFILATURA_AVAILABLE, "| newspaper3k=", NEWSPAPER_AVAILABLE, "| langdetect=", LANGDETECT_AVAILABLE)

    # ---- smoke test first ----
    if not gdelt_smoke_test():
        print("\n❌ GDELT is not returning JSON in your environment.")
        print("Open the latest HTML file saved under:", DEBUG_DIR)
        print("It will usually say: rate limit / blocked / proxy login / gateway error.")
    else:
        news_df = build_gdelt_dataset_with_body_parallel(TICKERS)

        print("\n✅ DONE")
        print("Rows:", len(news_df))
        print("Unique tickers:", news_df["ticker"].nunique() if len(news_df) else 0)

        try:
            atomic_write_csv(news_df, OUT_PATH)
            print("✅ Saved:", OUT_PATH)
        except PermissionError as pe:
            print("[WARN] PermissionError saving final CSV (maybe open in Excel/OneDrive lock):", pe)
            fallback_out = os.path.join(CACHE_DIR, f"StockNews_GDELT_SCRAPED_fallback_{int(time.time())}.csv")
            atomic_write_csv(news_df, fallback_out)
            print("✅ Saved fallback:", fallback_out)


df = pd.read_csv(r"C:\Users\Dell\OneDrive\Documents\IIMV -MBA\IIMV_Capstone_Project\StockNews_GDELT_SCRAPED_20200101_20251201_V5.csv", 
                 encoding='latin1')
df.head(10)

df = df[df["source"] == "GDELT_DOC+SCRAPED"].reset_index(drop=True)

before = len(df)

df = df.drop_duplicates().reset_index(drop=True)
df["body_len"] = df["body_len"].fillna(df["body_text"].astype(str).str.len())
df["body_len"].isna().any()

after = len(df)
duplicates_removed = before - after

print(f"Duplicates removed: {duplicates_removed}")

cols_to_fill = [
    "Company_Name",
    "Country",
    "Region",
    "Sector",
    "elapsed_seconds",
    "elapsed_minutes",
    "urls_found",
    "urls_scraped_ok",
    "pages_attempted"
]
def clean_df(df):
    df = df.copy()

    df[cols_to_fill] = df[cols_to_fill].replace(r"^\s*$", pd.NA, regex=True).ffill()

    df["body_len"] = df["body_len"].fillna(df["body_text"].astype(str).str.len())

    df = df.drop_duplicates().reset_index(drop=True)
    return df

df = clean_df(df)

df[cols_to_fill].isna().any()

df.to_excel(
    r"C:\Users\Dell\OneDrive\Documents\IIMV -MBA\IIMV_Capstone_Project\StockNews_GDELT_SCRAPED_20200101_20251201_V8.xlsx",
    index=False
)


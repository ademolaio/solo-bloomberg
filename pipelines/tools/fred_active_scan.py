import os
import requests
from datetime import date, timedelta

FRED_KEY = os.environ.get("FRED_API_KEY")
BASE = "https://api.stlouisfed.org/fred"

if not FRED_KEY:
    raise RuntimeError("FRED_API_KEY not set")

def latest_obs_date(series_id: str):
    r = requests.get(
        f"{BASE}/series/observations",
        params={
            "api_key": FRED_KEY,
            "file_type": "json",
            "series_id": series_id,
            "sort_order": "desc",
            "limit": 1,
        },
        timeout=30,
    )

    if r.status_code == 400:
        return None

    r.raise_for_status()
    obs = r.json().get("observations", [])
    if not obs:
        return None

    return obs[0].get("date")

def is_active(series_id: str, days: int = 365):
    d = latest_obs_date(series_id)
    if not d:
        return False, None

    cutoff = date.today() - timedelta(days=days)
    y, m, dd = map(int, d.split("-"))
    last_date = date(y, m, dd)

    return last_date >= cutoff, last_date


# ----------- SAMPLE UNIVERSE -------------
SERIES = [

    # Inflation: CPI/PCE/deflators
    "CPIAUCSL", "CPILFESL", "CPIENGSL", "CPIALLITEMS",  # note: CPIALLITEMS sometimes exists as alt; harmless
    "PCEPI", "PCEPILFE", "PCE", "DPCCRV1Q225SBEA",  # core PCE chain-type (if available)
    "GDPDEF", "A191RD3A086NBEA",  # GDP deflator alt / real GDP deflator growth
    "CPIMEDSL", "CPIAUCSL", "CPILFESL",

    # Expectations
    "T10YIE", "T5YIE", "T5YIFR", "T10YIFR",

    # Policy / Rates
    "FEDFUNDS",
    "IORB", "OBFR", "SOFR", "EFFR",
    "DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30",
    "DTB3", "DTB6", "DTB1YR",

    # Curve spreads
    "T10Y2Y", "T10Y3M", "T5Y3M", "T30Y10Y",
    "DFF", "DFII10",  # Fed Funds effective / 10Y TIPS yield (may vary)

    # Credit / Financial conditions
    "BAA10Y", "AAA10Y", "BAMLH0A0HYM2", "BAMLH0A0HYM2EY",
    "BAMLCC0A0CMTRIV", "BAMLCC0A0CMTRIV",  # corp total return index (dup ok)
    "TEDRATE", "MORTGAGE30US", "MORTGAGE15US",
    "NFCI", "ANFCI", "STLFSI4",

    # Money / Liquidity / Banking
    "M2SL", "M1SL",
    "BOGMBASE", "BASE",  # monetary base variants
    "WALCL",  # Fed balance sheet
    "RRPONTSYD",  # ON RRP
    "WTREGEN",  # Treasury General Account (if available)
    "TOTRESNS",  # total reserves
    "IOER",  # legacy (may be discontinued; ok)
    "H8B1021NCBCMG", "H8B1047NCBCMG", "H8B1053NCBCMG",  # H.8 loans/deposits examples

    # Growth / GDP / Production
    "GDPC1", "GDP", "A191RL1Q225SBEA", "A191RP1Q027SBEA",  # real GDP level / growth variants
    "GDI", "INDPRO", "CAPUTLG3311A2S", "TCU",
    "IPMAN", "IPB50001N", "IPCONGD",

    # Labor
    "UNRATE", "U6RATE",
    "PAYEMS", "CES0500000003", "CES0600000003", "CES3000000003",  # avg hourly earnings sectors
    "ICSA", "CCSA",
    "JTSJOL", "JTSQUR", "JTSHIL",
    "CIVPART", "EMRATIO",
    "AHETPI",  # productivity proxy if exists

    # Consumption / Income
    "PCE", "PCEDG", "PCESV", "RSXFS",
    "DSPIC96", "W875RX1", "PSAVERT",
    "UMCSENT", "DSPIC96",

    # Housing / Real estate
    "HOUST", "HOUST1F", "PERMIT", "PERMIT1F",
    "CSUSHPINSA", "USSTHPI",
    "MSPUS", "MORTGAGE30US",
    "RHORUSQ156N",  # homeownership rate

    # Trade / External
    "BOPGSTB", "NETEXP", "IMPGS", "EXPGS",
    "TWEXBPA", "TWEXAFEGSMTH", "DTWEXBGS",  # USD broad indices variants

    # Prices / Commodities / Energy
    "DCOILWTICO", "DCOILBRENTEU", "GASREGW",
    "GOLDAMGBD228NLBM", "SLVPRUSD",  # gold/silver (availability can vary)
    "PPIACO", "PPIIDC", "PPIFGS",

    # Risk / Equity / Vol
    "SP500", "VIXCLS",
    "DEXUSEU", "DEXJPUS", "DEXUSUK", "DEXCHUS", "DEXSZUS",  # FX (some may be discontinued; ok)

    # International (core)
    # Euro Area / ECB
    "ECBMAIN", "ECBDFR",  # may exist depending on FRED provider series
    # Japan
    "IRLTLT01JPM156N", "IR3TIB01JPM156N",  # OECD long/short rates Japan
    # UK
    "IRLTLT01GBM156N", "IR3TIB01GBM156N",
    # Germany
    "IRLTLT01DEM156N", "IR3TIB01DEM156N",

    # PMI / Surveys (if present in FRED)
    "NAPM", "NAPMPI", "NAPMNOI", "NAPMPRIC",
]

print("\nScanning series...\n")

for s in SERIES:
    active, last = is_active(s)
    print(f"{s:12} | Active: {active} | Last Obs: {last}")
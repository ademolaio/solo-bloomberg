from __future__ import annotations

from datetime import datetime, timezone

from pipelines.common.clickhouse_core import get_client

# Large institutional macro seed (FRED series ids)
SEED_SERIES = [

    # --------------------
    # Inflation
    # --------------------
    "CPIAUCSL",
    "CPILFESL",
    "CPIENGSL",
    "CPIMEDSL",
    "PCEPI",
    "PCEPILFE",
    "PCE",
    "GDPDEF",
    "T10YIE",
    "T5YIE",
    "T5YIFR",

    # --------------------
    # Policy Rates / Funding
    # --------------------
    "FEDFUNDS",
    "IORB",
    "OBFR",
    "SOFR",
    "EFFR",
    "DFF",

    # --------------------
    # Treasury Curve
    # --------------------
    "DGS1MO",
    "DGS3MO",
    "DGS6MO",
    "DGS1",
    "DGS2",
    "DGS3",
    "DGS5",
    "DGS7",
    "DGS10",
    "DGS20",
    "DGS30",
    "DTB3",
    "DTB6",
    "DTB1YR",
    "T10Y2Y",
    "T10Y3M",
    "DFII10",

    # --------------------
    # Credit / Risk
    # --------------------
    "BAA10Y",
    "AAA10Y",
    "BAMLH0A0HYM2",
    "BAMLH0A0HYM2EY",
    "BAMLCC0A0CMTRIV",

    # --------------------
    # Financial Conditions
    # --------------------
    "NFCI",
    "ANFCI",
    "STLFSI4",

    # --------------------
    # Money Supply / Balance Sheet
    # --------------------
    "M2SL",
    "M1SL",
    "BOGMBASE",
    "WALCL",
    "RRPONTSYD",
    "WTREGEN",
    "TOTRESNS",
    "H8B1047NCBCMG",

    # --------------------
    # GDP / National Accounts
    # --------------------
    "GDPC1",
    "GDP",
    "A191RL1Q225SBEA",
    "A191RP1Q027SBEA",
    "GDI",
    "NETEXP",
    "IMPGS",
    "EXPGS",

    # --------------------
    # Industrial Production
    # --------------------
    "INDPRO",
    "CAPUTLG3311A2S",
    "TCU",
    "IPMAN",
    "IPB50001N",
    "IPCONGD",

    # --------------------
    # Labor Market
    # --------------------
    "UNRATE",
    "U6RATE",
    "PAYEMS",
    "CES0500000003",
    "CES0600000003",
    "CES3000000003",
    "ICSA",
    "CCSA",
    "JTSJOL",
    "JTSQUR",
    "JTSHIL",
    "CIVPART",
    "EMRATIO",
    "AHETPI",

    # --------------------
    # Consumption / Income
    # --------------------
    "PCEDG",
    "PCESV",
    "RSXFS",
    "DSPIC96",
    "W875RX1",
    "PSAVERT",
    "UMCSENT",

    # --------------------
    # Housing
    # --------------------
    "HOUST",
    "HOUST1F",
    "PERMIT",
    "CSUSHPINSA",
    "USSTHPI",
    "MSPUS",
    "MORTGAGE30US",
    "MORTGAGE15US",
    "RHORUSQ156N",

    # --------------------
    # External / FX
    # --------------------
    "BOPGSTB",
    "TWEXAFEGSMTH",
    "DTWEXBGS",
    "DEXUSEU",
    "DEXJPUS",
    "DEXUSUK",
    "DEXCHUS",
    "DEXSZUS",

    # --------------------
    # Commodities
    # --------------------
    "DCOILWTICO",
    "DCOILBRENTEU",
    "GASREGW",

    # --------------------
    # Producer Prices
    # --------------------
    "PPIACO",
    "PPIIDC",

    # --------------------
    # Market Risk
    # --------------------
    "SP500",
    "VIXCLS",

    # --------------------
    # Global Rates
    # --------------------
    "ECBDFR",
    "IRLTLT01JPM156N",
    "IR3TIB01JPM156N",
    "IRLTLT01GBM156N",
    "IR3TIB01GBM156N",
    "IRLTLT01DEM156N",
    "IR3TIB01DEM156N",
]

def upsert_universe(series_ids: list[str], source: str = "manual_seed_large", priority: int = 5):
    client = get_client()
    now = datetime.now(timezone.utc)

    rows = []
    for sid in sorted(set(series_ids)):
        rows.append((sid, 1, None, priority, now, now, source))

    client.insert(
        "economic_data.fred_series_universe",
        rows,
        column_names=[
            "series_id",
            "is_active",
            "macro_series_id",
            "priority",
            "created_at",
            "updated_at",
            "source",
        ],
    )
    print(f"[ok] fred_series_universe upserted: {len(rows)} series")

def main():
    upsert_universe(SEED_SERIES)

if __name__ == "__main__":
    main()
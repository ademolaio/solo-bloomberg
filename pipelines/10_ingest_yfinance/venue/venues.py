from dataclasses import dataclass

@dataclass(frozen=True)
class Venue:
    exchange: str   # human label
    mic: str        # ISO 10383 market identifier code


# Suffix-based venue inference (common Yahoo symbol suffixes)
SUFFIX_VENUE_MAP: dict[str, Venue] = {
    ".SW": Venue("SIX", "XSWX"),            # SIX Swiss Exchange
    ".DE": Venue("XETRA", "XETR"),          # Deutsche Börse XETRA
    ".AS": Venue("EURONEXT_AMS", "XAMS"),   # Euronext Amsterdam
    ".PA": Venue("EURONEXT_PAR", "XPAR"),   # Euronext Paris
    ".L":  Venue("LSE", "XLON"),            # London Stock Exchange
    ".T":  Venue("TSE", "XTKS"),            # Tokyo Stock Exchange
}


# Explicit overrides (when suffix logic is insufficient)
SYMBOL_VENUE_OVERRIDES: dict[str, Venue] = {
    # Yahoo continuous futures style (not true contracts)
    "ES=F": Venue("CME", "XCME"),
    "NQ=F": Venue("CME", "XCME"),
    "CL=F": Venue("NYMEX", "XNYM"),
    "GC=F": Venue("COMEX", "XCEC"),
}


# US default heuristic
DEFAULT_US_VENUE = Venue("USA", "US__")  # placeholder MIC until enriched (XNYS/XNAS split)


def resolve_venue(symbol: str) -> Venue:
    s = symbol.upper()

    if s in SYMBOL_VENUE_OVERRIDES:
        return SYMBOL_VENUE_OVERRIDES[s]

    for suffix, venue in SUFFIX_VENUE_MAP.items():
        if s.endswith(suffix):
            return venue

    return DEFAULT_US_VENUE
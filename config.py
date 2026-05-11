import pytz

ET = pytz.timezone("America/New_York")

# ── Universe ──────────────────────────────────────────────────────────────────

TEST_MODE = True   # set False to switch to full NDX 100

TEST_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "TSLA", "GOOGL", "AVGO", "NFLX", "AMD",
]

NDX100 = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "AVGO", "TSLA", "GOOGL", "GOOG", "COST",
    "NFLX", "AMD", "ADBE", "QCOM", "INTU", "AMAT", "CSCO", "TXN", "AMGN", "ISRG",
    "VRTX", "REGN", "GILD", "HON", "SBUX", "ADI", "KLAC", "LRCX", "MRVL", "PANW",
    "CRWD", "SNPS", "CDNS", "ORLY", "MNST", "PCAR", "ODFL", "FAST", "DXCM", "BIIB",
    "IDXX", "ILMN", "MRNA", "TEAM", "ZS", "WDAY", "NXPI", "MCHP", "MU", "ON",
    "TTD", "DDOG", "ABNB", "PYPL", "EBAY", "INTC", "CEG", "EXC", "CSGP", "GEHC",
    "ARM", "APP", "PLTR", "FTNT", "ANSS", "CTSH", "PAYX", "ADP", "MAR", "BKNG",
    "VRSK", "BKR", "KHC", "TMUS", "CMCSA", "PEP", "AZN", "MDLZ", "LULU", "ROST",
    "EA", "CPRT", "KDP", "CTAS", "CHTR", "MELI", "COIN", "SMCI", "GFS", "FANG",
]

UNIVERSE = TEST_TICKERS if TEST_MODE else NDX100

# ── Pipeline constants ────────────────────────────────────────────────────────

PRICE_ANOMALY_THRESHOLD = 0.04   # ≥4% daily move
MOMENTUM_DISPLAY_N      = 20     # rows shown in screener tables
FUND_STALE_DAYS         = 7      # days before re-fetching fundamentals
PRICE_FETCH_PERIOD      = "4mo"  # default lookback for initial price pull

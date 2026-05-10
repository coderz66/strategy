import pytz

# Nasdaq 100 universe (major components, update quarterly)
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

ET = pytz.timezone("America/New_York")

PRICE_ANOMALY_THRESHOLD = 0.04   # ≥4% daily move triggers event flag
MOMENTUM_DISPLAY_N = 20          # rows shown in screener tables
FUNDAMENTAL_FETCH_N = 60         # tickers to fetch fundamentals for (rate-limit aware)
SCHEDULER_HOUR = 17              # 5:00 PM ET daily refresh
SCHEDULER_MINUTE = 0

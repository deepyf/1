import csv
import random
import time
import pandas as pd
import yfinance as yf
from curl_cffi import requests

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
]

def fetch(symbol):
    for attempt in range(1, 5):
        session = requests.Session()
        session.headers.update({"User-Agent": random.choice(UA_LIST)})
        t = yf.Ticker(symbol, session=session)
        try:
            info = t.info
            return {
                "T": symbol,
                "P": info.get("currentPrice") or "",
                "B": info.get("bid") or "",
                "A": info.get("ask") or "",
                "M": info.get("targetMeanPrice") or "",
                "O": info.get("numberOfAnalystOpinions") or "",
                "C": info.get("marketCap") or "",
                "I": info.get("industry") or "",
                "S": info.get("sector") or "",
            }
        except Exception:
            delay = random.uniform(2 * attempt, 2 * attempt + 0.5)
            time.sleep(delay)
    return {"T": symbol, "P": "", "B": "", "A": "", "M": "", "O": "", "C": "", "I": "", "S": ""}

def main():
    df = pd.read_csv("data.csv", usecols=["T"])
    rows = []
    for sym in df["T"].dropna().astype(str):
        rows.append(fetch(sym))
        time.sleep(random.uniform(2, 2.5))
    pd.DataFrame(rows).to_csv("yf.csv", index=False, encoding="utf-8")

if __name__ == "__main__":
    main()
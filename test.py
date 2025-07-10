import csv
import time
import random
from curl_cffi import requests
import yfinance as yf

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/119.0"
]

def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    return s

with open("data.csv", newline="") as fin:
    reader = csv.DictReader(fin)
    symbols = [row["T"] for row in reader]

with open("yf.csv", "w", newline="", encoding="utf-8") as fout:
    writer = csv.writer(fout)
    writer.writerow(["T","P","B","A","M","O","C","I","S"])
    for sym in symbols:
        info = {}
        for attempt in range(4):
            try:
                sess = get_session()
                yf.utils.requests = sess
                info = yf.Ticker(sym).info or {}
                break
            except Exception:
                if attempt == 0:
                    wait = random.uniform(4,4.5)
                elif attempt == 1:
                    wait = random.uniform(6,6.5)
                elif attempt == 2:
                    wait = random.uniform(8,8.5)
                else:
                    break
                time.sleep(wait)
        P = info.get("currentPrice","") or ""
        B = info.get("bid","") or ""
        A = info.get("ask","") or ""
        M = info.get("targetMeanPrice","") or ""
        O = info.get("numberOfAnalystOpinions","") or ""
        C = info.get("marketCap","") or ""
        I = info.get("industry","") or ""
        S = info.get("sector","") or ""
        writer.writerow([sym, P, B, A, M, O, C, I, S])
        time.sleep(random.uniform(2,2.5))
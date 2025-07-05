import yfinance as yf
import csv
import time
import random
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
]

def fetch_ticker_info(symbol, session, user_agents):
    max_retries = 4
    info = {}
    for retry in range(max_retries):
        try:
            session.headers.update({'User-Agent': random.choice(user_agents)})
            yf.set_session(session)
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return info
        except Exception:
            if retry < max_retries - 1:
                delay = 2 + retry * 2 + random.uniform(0, 0.5)
                time.sleep(delay)
            else:
                return {}
    return {}

retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount('https://', adapter)
session.mount('http://', adapter)

symbols = []
with open('data.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if 'T' in row:
            symbols.append(row['T'])

with open('yf.csv', 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=['T','P','B','A','M','O','C','I','S'])
    writer.writeheader()
    for i, symbol in enumerate(symbols):
        info = fetch_ticker_info(symbol, session, user_agents)
        row = {
            'T': symbol,
            'P': info.get('currentPrice', ''),
            'B': info.get('bid', ''),
            'A': info.get('ask', ''),
            'M': info.get('targetMeanPrice', ''),
            'O': info.get('numberOfAnalystOpinions', ''),
            'C': info.get('marketCap', ''),
            'I': info.get('industry', ''),
            'S': info.get('sector', '')
        }
        writer.writerow(row)
        if i < len(symbols) - 1:
            time.sleep(random.uniform(2, 2.5))
import yfinance as yf
import csv
import time
import random
import pandas as pd
from curl_cffi import requests

def get_user_agent():
    agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.1',
        'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.',
    ]
    return random.choice(agents)

def fetch_data(symbol, session):
    headers = {'User-Agent': get_user_agent()}
    for attempt in range(4):
        try:
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            return {
                'P': info.get('currentPrice', ''),
                'B': info.get('bid', ''),
                'A': info.get('ask', ''),
                'M': info.get('targetMeanPrice', ''),
                'O': info.get('numberOfAnalystOpinions', ''),
                'C': info.get('marketCap', ''),
                'I': info.get('industry', ''),
                'S': info.get('sector', ''),
            }
        except Exception:
            sleep_time = 2 + attempt * 2 + random.uniform(0, 0.5)
            time.sleep(sleep_time)
    return {}

df = pd.read_csv('data.csv')
symbols = df['T'].tolist()
session = requests.Session()

with open('yf.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['T','P','B','A','M','O','C','I','S'])
    writer.writeheader()
    for symbol in symbols:
        data = fetch_data(symbol, session)
        row = {'T': symbol}
        row.update(data)
        writer.writerow(row)
        time.sleep(random.uniform(2, 2.5))
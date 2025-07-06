import random
import time
import csv
import yfinance as yf
from curl_cffi.requests import Session

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0'
]

def fetch_data(symbol):
    for attempt in range(4):
        try:
            session = Session()
            session.headers = {'User-Agent': random.choice(user_agents)}
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            return {
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
        except Exception:
            if attempt < 3:
                time.sleep((2 + attempt * 2) + random.uniform(0, 0.5))
            else:
                return {k: '' for k in ['T','P','B','A','M','O','C','I','S']}
    return {k: '' for k in ['T','P','B','A','M','O','C','I','S']}

symbols = []
with open('data.csv', mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        symbols.append(row['T'])

with open('yf.csv', mode='w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['T','P','B','A','M','O','C','I','S'])
    writer.writeheader()
    for symbol in symbols:
        data = fetch_data(symbol)
        writer.writerow(data)
        time.sleep(random.uniform(2, 2.5))
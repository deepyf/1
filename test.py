import yfinance as yf
import csv
import random
import time
import requests
import pandas as pd

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'
]

max_retries = 4
symbols = pd.read_csv('data.csv')['T'].tolist()

with open('yf.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['T','P','B','A','M','O','C','I','S'])
    writer.writeheader()
    for i, symbol in enumerate(symbols):
        info = {}
        for attempt in range(max_retries):
            if attempt > 0:
                low = 2 * (attempt + 1)
                sleep_time = random.uniform(low, low + 0.5)
                time.sleep(sleep_time)
            try:
                session = requests.Session()
                ua = random.choice(user_agents)
                session.headers['User-Agent'] = ua
                ticker = yf.Ticker(symbol, session=session)
                info = ticker.info
                break
            except Exception as e:
                print(f"Attempt {attempt+1} for {symbol} failed: {e}")
                if attempt == max_retries - 1:
                    print(f"All attempts failed for {symbol}")
                    info = {}
        row = {
            'T': symbol,
            'P': info.get('currentPrice', '') if info else '',
            'B': info.get('bid', '') if info else '',
            'A': info.get('ask', '') if info else '',
            'M': info.get('targetMeanPrice', '') if info else '',
            'O': info.get('numberOfAnalystOpinions', '') if info else '',
            'C': info.get('marketCap', '') if info else '',
            'I': info.get('industry', '') if info else '',
            'S': info.get('sector', '') if info else ''
        }
        writer.writerow(row)
        f.flush()
        if i < len(symbols) - 1:
            time.sleep(random.uniform(2, 2.5))
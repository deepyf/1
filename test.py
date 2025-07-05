import yfinance as yf
import pandas as pd
import time
import random
import requests
import csv
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

def get_info(symbol):
    for retry_count in range(4):
        session = requests.Session()
        try:
            retry_strategy = Retry(total=0)
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            session.headers.update({'User-Agent': random.choice(user_agents)})
            yf.set_session(session)
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return {
                'P': info.get('currentPrice'),
                'B': info.get('bid'),
                'A': info.get('ask'),
                'M': info.get('targetMeanPrice'),
                'O': info.get('numberOfAnalystOpinions'),
                'C': info.get('marketCap'),
                'I': info.get('industry'),
                'S': info.get('sector')
            }
        except:
            if retry_count < 3:
                delay = 2 * (retry_count + 2) + random.uniform(0, 0.5)
                time.sleep(delay)
            else:
                return None
        finally:
            session.close()

df = pd.read_csv('data.csv')
with open('yf.csv', 'w', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['T','P','B','A','M','O','C','I','S'])
    for symbol in df['T']:
        data = get_info(symbol)
        if data is None:
            row = [symbol, "", "", "", "", "", "", "", ""]
        else:
            row = [symbol]
            for key in ['P','B','A','M','O','C','I','S']:
                value = data.get(key)
                row.append("" if value is None else value)
        writer.writerow(row)
        time.sleep(2 + random.uniform(0, 0.5))
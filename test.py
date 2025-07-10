import csv
import time
import random
import yfinance as yf
from curl_cffi.requests import Session

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
]

def get_tickers():
    with open('data.csv', 'r') as f:
        return [row[0] for row in csv.reader(f)][1:]

def fetch_data(symbol, session):
    ticker = yf.Ticker(symbol, session=session)
    info = ticker.info
    return [
        info.get('currentPrice', ''),
        info.get('bid', ''),
        info.get('ask', ''),
        info.get('targetMeanPrice', ''),
        info.get('numberOfAnalystOpinions', ''),
        info.get('marketCap', ''),
        info.get('industry', ''),
        info.get('sector', '')
    ]

def main():
    symbols = get_tickers()
    with open('yf.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['T','P','B','A','M','O','C','I','S'])
        for i, symbol in enumerate(symbols):
            for retry in range(4):
                try:
                    session = Session()
                    session.headers = {'User-Agent': random.choice(user_agents)}
                    row_data = fetch_data(symbol, session)
                    writer.writerow([symbol] + row_data)
                    break
                except Exception:
                    if retry < 3:
                        time.sleep(4 + 2 * retry + random.uniform(0, 0.5))
                    else:
                        writer.writerow([symbol] + [''] * 8)
            if i < len(symbols) - 1:
                time.sleep(random.uniform(2, 2.5))

if __name__ == "__main__":
    main()
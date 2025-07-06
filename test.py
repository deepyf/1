import yfinance as yf
import pandas as pd
import time
import random
from curl_cffi.requests import Session

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
]

def get_ticker_info_with_retries(ticker_symbol, session):
    max_retries = 4
    for i in range(max_retries):
        try:
            ticker = yf.Ticker(ticker_symbol, session=session)
            info = ticker.info
            if info and info.get('symbol'):
                return info
        except Exception as e:
            print(f"Error for {ticker_symbol} on attempt {i+1}: {e}")

        if i < max_retries - 1:
            wait_time = random.uniform(2 * (i + 2), 2 * (i + 2) + 0.5)
            print(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
            session.headers['User-Agent'] = random.choice(USER_AGENTS)

    print(f"Failed to fetch data for {ticker_symbol} after {max_retries} attempts.")
    return None

def main():
    try:
        symbols_df = pd.read_csv('Data.csv')
    except FileNotFoundError:
        print("Error: Data.csv not found. Please ensure the file exists in the repository.")
        return

    tickers = symbols_df['T'].tolist()
    all_data = []

    session = Session(impersonate="chrome110")

    for symbol in tickers:
        print(f"Processing: {symbol}")

        session.headers['User-Agent'] = random.choice(USER_AGENTS)

        info = get_ticker_info_with_retries(symbol, session)

        if info:
            data_row = {
                'T': info.get('symbol', symbol),
                'P': info.get('currentPrice', ''),
                'B': info.get('bid', ''),
                'A': info.get('ask', ''),
                'M': info.get('targetMeanPrice', ''),
                'O': info.get('numberOfAnalystOpinions', ''),
                'C': info.get('marketCap', ''),
                'I': info.get('industry', ''),
                'S': info.get('sector', '')
            }
            all_data.append(data_row)
        else:
            all_data.append({'T': symbol, 'P': '', 'B': '', 'A': '', 'M': '', 'O': '', 'C': '', 'I': '', 'S': ''})

        time.sleep(random.uniform(2, 2.5))

    output_df = pd.DataFrame(all_data)
    output_df.to_csv('yf.csv', index=False, encoding='utf-8')
    print("Script finished. Data written to yf.csv")

if __name__ == "__main__":
    main()

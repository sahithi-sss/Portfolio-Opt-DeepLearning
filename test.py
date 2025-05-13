import yfinance as yf
import pandas as pd
import time
import random
from datetime import datetime, timedelta

def daterange(start_date, end_date, delta_years=3):
    current = start_date
    while current < end_date:
        next_date = current + timedelta(days=365 * delta_years)
        yield current, min(next_date, end_date)
        current = next_date

def fetch_data_with_retries(ticker, start, end, retries=3, sleep_min=10, sleep_max=15):
    for attempt in range(retries):
        try:
            data = yf.download(ticker, start=start, end=end, auto_adjust=False)
            if not data.empty:
                return data.reset_index()
            else:
                print(f"Empty data received for {start} to {end}. Retrying...")
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
        time.sleep(random.randint(sleep_min, sleep_max))
    print(f"Failed to fetch data for {start} to {end} after {retries} attempts.")
    return None

ticker = "VTI"
start = datetime(2006, 1, 12)
end = datetime(2019, 12, 31)

all_data = []

for start_chunk, end_chunk in daterange(start, end):
    print(f"\nFetching data from {start_chunk.date()} to {end_chunk.date()}...")
    df = fetch_data_with_retries(ticker, start_chunk.strftime("%Y-%m-%d"), end_chunk.strftime("%Y-%m-%d"))
    if df is not None:
        all_data.append(df)
    else:
        print(f"Skipped chunk {start_chunk} to {end_chunk} due to persistent failures.")

# Combine and export
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv("VTI_historical_OHLCV.csv", index=False)
    print("✅ All data saved to VTI_historical_OHLCV.csv")
else:
    print("❌ No data was downloaded.")

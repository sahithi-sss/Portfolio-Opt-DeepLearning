import yfinance as yf
import pandas as pd
import time
from datetime import datetime

ticker = "VTI"
intervals = [
    ("2006-01-01", "2008-12-31"),
    ("2009-01-01", "2011-12-31"),
    ("2012-01-01", "2014-12-31"),
    ("2015-01-01", "2017-12-31"),
    ("2018-01-01", "2019-12-31"),
]

all_data = []

for start, end in intervals:
    print(f"Downloading: {start} to {end}")
    try:
        df = yf.download(ticker, start=start, end=end, progress=False)
        if not df.empty:
            df["Interval"] = f"{start} to {end}"
            all_data.append(df)
        else:
            print(f"⚠️ No data for {start} to {end}")
    except Exception as e:
        print(f"❌ Error during {start} to {end}: {e}")
    time.sleep(10)

# Combine and save
if all_data:
    final_df = pd.concat(all_data)
    final_df.to_csv("VTI_2006_to_2019.csv")
    print("✅ All intervals downloaded and merged.")
else:
    print("❌ All downloads failed.")

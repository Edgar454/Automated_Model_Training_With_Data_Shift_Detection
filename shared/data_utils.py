import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from includes.DataIngestion.scrape_data import get_weather_data
import pandas as pd
import numpy as np


def build_fourier(index, freq=365.25, order=2):
    t = np.arange(len(index), dtype=np.float32)
    k = 2 * np.pi * (1 / freq) * t
    out = {}
    for i in range(1, order+1):
        out[f"sin_{freq}_{i}"] = np.sin(i*k)
        out[f"cos_{freq}_{i}"] = np.cos(i*k)
    return pd.DataFrame(out, index=index)

def fetch_and_prepare_data(start_date, end_date):
    raw_df = get_weather_data(start_date=start_date, end_date=end_date)
    raw_df = raw_df.sort_index().dropna()
    raw_df['day_of_year'] = raw_df.index.dayofyear
    fourier_features = build_fourier(raw_df.index)
    return pd.concat([raw_df, fourier_features], axis=1)

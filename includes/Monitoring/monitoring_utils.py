import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import pandas as pd
from datetime import datetime
from shared.model_utils import safe_predict_with_model
from shared.data_utils import build_fourier


def prepare_data(cutoff_date, data_path , start= 8):
    # Load the data
    data = pd.read_csv(os.path.join(data_path, "weather_data.csv"), index_col=0, parse_dates=True)

    # Ensure the data is sorted and has no missing values
    data.index = data.index.tz_localize(None)
    data = data.sort_index().dropna()
    data['day_of_year'] = data.index.dayofyear

    # Create Fourier features
    fourier_features = build_fourier(data.index, freq=365.25, order=2)
    data = pd.concat([data, fourier_features], axis=1)

    # Make predictions
    cutoff_date_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
    horizon = data.index[-1] - data.index[start]

    predicted_df = safe_predict_with_model(data, horizon=horizon.days, start=start)
    full_index = data.index.union(predicted_df.index)
    data = data.reindex(full_index)
    data["predicted_rain (mm)"] = predicted_df

    # Split the data into before and after the cut-off date
    data_before = data[data.index < cutoff_date_dt]
    data_after = data[data.index >= cutoff_date_dt]

    return data_before, data_after
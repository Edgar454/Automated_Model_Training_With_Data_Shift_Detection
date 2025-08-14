import os
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from dotenv import set_key, load_dotenv

from darts import TimeSeries
from darts.models import CatBoostModel

import mlflow
load_dotenv()

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = os.getenv("EXPERIMENT_NAME", "Weather_Forecast_Model_Training")

# MLflow setup
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Paths
parent_dir = Path(__file__).resolve().parents[2]
DATA_PATH = parent_dir / "data"
csv_path = DATA_PATH / "weather_data.csv"

# Load data
weather_df = pd.read_csv(csv_path, index_col=0, parse_dates=True)

# Params
params = {
    "model_name": "CatBoostModel",
    "feature_created": ["day_of_year", "cos_365.25_1", "cos_365.25_2"],
    "target": "rain_sum (mm)",
    "target_transformation": None,
    "experimentation_rmse": 6.66,
    "past_covariates": [
        "temperature_2m_max (°C)",
        "wind_speed_10m_min (m/s)",
        "surface_pressure_mean (hPa)",
        "precipitation_hours",
        "day_of_year",
        "cos_365.25_1",
        "cos_365.25_2",
    ],
    "lags": 1,
    "lags_past_covariates": 8,
    "n_estimators": 500,
    "learning_rate": 0.04731969574429225,
    "max_depth": 8,
    "output_chunk_length": 7,
    "random_state": 42,
}

# Fourier feature creation
def fourier_features(index, freq, order):
    time = np.arange(len(index), dtype=np.float32)
    k = 2 * np.pi * (1 / freq) * time
    features = {}
    for i in range(1, order + 1):
        features[f"sin_{freq}_{i}"] = np.sin(i * k)
        features[f"cos_{freq}_{i}"] = np.cos(i * k)
    return pd.DataFrame(features, index=index)



def train_and_log_model(weather_df: pd.DataFrame, params: dict):
    # Preprocess
    logger.info("⚙ Loading the weather data and preprocessing...")
    weather_df = weather_df.dropna()
    weather_df.index = pd.to_datetime(weather_df.index)
    weather_df["day_of_year"] = weather_df.index.dayofyear

    # Fourier features
    fourier_df = fourier_features(weather_df.index, 365.25, 4)
    weather_df = pd.concat([fourier_df, weather_df], axis=1)

    # TimeSeries conversion
    rain_series = TimeSeries.from_dataframe(weather_df, value_cols=[params["target"]])
    past_covariates = TimeSeries.from_dataframe(weather_df, value_cols=params["past_covariates"])

    # Train
    logger.info("🔍 Training the model...")
    model = CatBoostModel(
        lags=params["lags"],
        lags_past_covariates=params["lags_past_covariates"],
        output_chunk_length= params["output_chunk_length"],
        n_estimators=params["n_estimators"],
        learning_rate=params["learning_rate"],
        max_depth=params["max_depth"],
        random_state=params["random_state"],
        verbose=-1,
        multi_models=True,
    )
    model.fit(rain_series, past_covariates=past_covariates)
    logger.info("✅ Model training completed successfully.")

    # save the cutoff date
    cut_off_date = weather_df.index[-1].strftime("%Y-%m-%d")
    set_key(".env", "CUT_OFF_DATE", cut_off_date)

    # Save & log to MLflow
    model_path = parent_dir / "models" / "rain_forecasting_model"
    model_path.mkdir(parents=True, exist_ok=True)
    model.save(str(model_path / "catboost_model.pkl"))

    with mlflow.start_run():
        mlflow.log_params(params)
        mlflow.log_metric("rmse", params["experimentation_rmse"])
        mlflow.log_artifacts(str(model_path))
        logger.info("📦 Model and metrics logged to MLflow.")

    return model

if __name__ == "__main__":
    _ = train_and_log_model(weather_df, params)
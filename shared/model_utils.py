import os
import mlflow
import pandas as pd
from pathlib import Path
from mlflow.tracking import MlflowClient
from datetime import timedelta
from dotenv import load_dotenv

from shared.variables import  past_covariate_cols , target_col
from darts.models import CatBoostModel
from darts import TimeSeries

import logging


# Load environment variables
load_dotenv()

experiment_name = os.getenv("EXPERIMENT_NAME")
mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

client = MlflowClient(tracking_uri=mlflow_tracking_uri)
experiment = client.get_experiment_by_name(experiment_name)

    

# Get the latest run
runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["start_time DESC"],
    max_results=1
)

run_id = runs[0].info.run_id
print(f"Latest run_id: {run_id}")

# Download and load
local_dir = mlflow.artifacts.download_artifacts(
    run_id=run_id,
artifact_path="catboost_model.pkl",
    tracking_uri=mlflow_tracking_uri
)
model = CatBoostModel.load(str(Path(local_dir)))


def safe_predict_with_model( weather_df: pd.DataFrame, horizon=7 , start= 8):

    """Try to predict with darts model; handle exceptions."""
    target_series = TimeSeries.from_dataframe(
        weather_df,
        value_cols=[target_col], )
    
    target_series = target_series[:start]

    past_covariates_ts = TimeSeries.from_dataframe(
        weather_df,
        value_cols=past_covariate_cols
    )

    try:
        pred = model.predict(horizon,series=target_series, past_covariates=past_covariates_ts)

        # convert to pandas series
        if hasattr(pred, "to_dataframe"):
            pred_df = pred.to_dataframe()
            pred_df.columns = ["predicted_rain (mm)"]
            
            # if univariate
            if pred_df.shape[1] == 1:
                series = pred_df.iloc[:,0]
            else:
                series = pred_df.iloc[:,0]
            return series
        else:
            return None
    except Exception as e:
        logging.error(f"Model prediction failed: {e}")
        return None

def persistence_forecast(last_values: pd.Series, horizon: int):
    """Simple persistence forecast: repeat last known value (or mean)"""
    last = last_values.iloc[-1]
    return pd.Series([last]*horizon, index=[last_values.index[-1] + timedelta(days=i) for i in range(1,horizon+1)])


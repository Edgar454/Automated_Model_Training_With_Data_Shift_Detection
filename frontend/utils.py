import mlflow
from pathlib import Path
from mlflow.tracking import MlflowClient
from darts.models import CatBoostModel

client = MlflowClient()
experiment_name = "Weather_Prediction_2025-08-10"  # your experiment name
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
    artifact_path="models/rain_forecasting_model"
)
model = CatBoostModel.load(str(Path(local_dir) / "catboost_model.pkl"))

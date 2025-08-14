import os
import pandas as pd

from evidently import Report, DataDefinition, Dataset, Regression 
from evidently.presets import DataDriftPreset 
from evidently.metrics import ValueDrift 
from evidently.tests import lte 
from evidently.metrics import RMSE, MAE
from evidently.future.tests import Reference

from evidently.ui.workspace import RemoteWorkspace,ProjectModel
from monitoring_utils import prepare_data

from dotenv import set_key ,load_dotenv
load_dotenv()

EVIDENTLY_SERVER_URL = os.getenv('EVIDENTLY_SERVER_URL',"http://127.0.0.1:8000")
ENV_PATH = os.getenv('ENV_PATH')
PROJECT_ID = os.getenv('PROJECT_ID')

remote_ws = RemoteWorkspace(EVIDENTLY_SERVER_URL)

try:
    project = remote_ws.get_project(PROJECT_ID)
except Exception :
    project = remote_ws.add_project(ProjectModel(name="Rain Model Monitoring", description="Monitoring of rain model in prod to spot model decay"))
    project.save()
    set_key(ENV_PATH, "PROJECT_ID" ,str(project.id))


def monitor_drift(**kwargs):
    """
    Function to monitor data drift and regression in the weather data.
    It prepares the data, runs regression tests, and generates a report.
    """
    # Load environment variables
    cut_off_date = os.getenv("CUT_OFF_DATE")
    data_path = os.getenv("DATA_PATH")

    if not cut_off_date or not data_path:
        raise ValueError("CUT_OFF_DATE and DATA_PATH must be set in the environment variables.")

    # Prepare the data
    data_before, data_after = prepare_data(cut_off_date, data_path)

    if data_before.empty or data_after.empty:
        raise ValueError("No data available for the specified cut-off date.")
    

    # Define the features for the regression tests
    features = data_before.columns.tolist()
    
    # Define the data definition for the regression tests
    data_definition = DataDefinition(
        regression=[Regression(target="rain_sum (mm)", prediction="predicted_rain (mm)")],
        numerical_columns=features
    )

    # Create datasets for the regression tests
    reference = Dataset.from_pandas(
        pd.DataFrame(data_before),
        data_definition=data_definition,
    )

    current = Dataset.from_pandas(
        pd.DataFrame(data_after),
        data_definition=data_definition,
    )

    # Define the regression tests
    regression_preset = Report(metrics=[
        RMSE(tests=[lte(Reference(absolute=0.3))]),
        MAE(mean_tests=[lte(Reference(absolute=0.3))]),
        ValueDrift(column="rain_sum (mm)"),
        DataDriftPreset()
    ],
    include_tests=True
    )

    # Run the regression tests
    snapshot = regression_preset.run(reference_data=reference, current_data=current)
    remote_ws.add_run(project.id, snapshot)
    result = snapshot.dict()
    
    tests_results = result['tests']
    mae_test_results= result['tests'][0]['status']
    rmse_test_results = result['tests'][1]['status']

    if 'ti' in kwargs:
        ti = kwargs['ti']
        ti.xcom_push(key='model_decay_test_result', value=str(rmse_test_results))

    

if __name__ == "__main__":
    monitor_drift()
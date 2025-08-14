import os
import pandas as pd

from evidently import Report, DataDefinition, Dataset, Regression
from evidently.presets import DataDriftPreset, RegressionPreset 
from evidently.metrics import ValueDrift 

from monitoring_utils import prepare_data

from dotenv import load_dotenv
load_dotenv()


def monitor_drift():
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
        RegressionPreset(),
        ValueDrift(column="rain_sum (mm)"),
        #DataDriftPreset(),
    ],
    )

    # Run the regression tests
    regression_report = regression_preset.run(reference_data=reference, current_data=current)


    print("Regression Report:")
    print(regression_report)

    return regression_report

if __name__ == "__main__":
    monitor_drift()
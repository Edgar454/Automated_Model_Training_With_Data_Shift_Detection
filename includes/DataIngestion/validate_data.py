# run_validation.py
import os
import great_expectations as gx
import pandas as pd
from pathlib import Path
from datetime import datetime

from pathlib import Path


def run_validation(expectations_path, data_path):
    context = gx.get_context(mode="file", project_root_dir=expectations_path)

    # Load existing suite
    expectation_suite = context.suites.get(name = "weather_data_expectations")

    batch_definition = (
    context.data_sources.get("weather_data_source")
    .get_asset("weather_dataframe_asset")
    .get_batch_definition("batch definition")
)

    # Run validation
    df = pd.read_csv(data_path ,parse_dates=["date"])

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    print("Batch loaded successfully.")

    results = batch.validate(expectation_suite)
    
    if results["success"]:
        print("All expectations passed.")
    else:
        for result in results["results"]:
            if not result["success"]:
                print(f"Column: {result['expectation_config']['kwargs']['column']}")
                print(f"Expectation failed: {result['expectation_config']['type']}")
                print(f"Details: {result['result']}")
        raise ValueError("Some expectations failed.")

if __name__ == "__main__":
    # Example usage
    expectations_path = "great_expectations"
    data_path = 'D:/Personnal_projects/Automated_Localised_Precipitation_Forecasting_Brazzaville/data/brazzaville_weather_data_2023-01-01-2023-01-31.csv'
    run_validation(expectations_path=expectations_path, data_path=data_path)
    print("Validation passed!")

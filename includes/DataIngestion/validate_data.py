# run_validation.py
import os
from pathlib import Path
import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

parent_dir = Path(__file__).resolve().parents[2]  

def run_validation(expectations_path, **kwargs):
    context = gx.get_context(mode="file", project_root_dir=expectations_path)
    expectation_suite = context.suites.get(name="weather_data_expectations")

    try:
        ti = kwargs['ti']
        filename = ti.xcom_pull(task_ids='DataFetching', key='weather_filename')
        if not filename:
            raise ValueError("XCom did not return a valid filename.")
        data_path = parent_dir / 'data' / filename
    except KeyError:
        raise ValueError("No filename found in XCom. Ensure the data fetching task is executed before this task.")

    df = pd.read_csv(data_path, parse_dates=["date"])

    batch_request = BatchRequest(
        datasource_name="weather_data_source",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="weather_dataframe_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=expectation_suite,
    )

    results = validator.validate()

    if results["success"]:
        print("All expectations passed.")
    else:
        for result in results["results"]:
            if not result["success"]:
                print(f"Column: {result['expectation_config']['kwargs']['column']}")
                print(f"Expectation failed: {result['expectation_config']['type']}")
                print(f"Details: {result['result']}")
        raise ValueError("Some expectations failed.")

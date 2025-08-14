import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator , BranchPythonOperator
from airflow.operators.common import BashOperator
from airflow.operators.empty import EmptyOperator

from includes.DataIngestion.scrape_data import get_weather_data
from includes.DataIngestion.ge_setup import setup_expectations
from includes.DataIngestion.validate_data import run_validation
from includes.Callbacks.alert import task_failure_alert

import logging
from pathlib import Path
from dateutil.relativedelta import relativedelta
from pendulum import datetime

DATA_DIR = Path(__file__).parent / 'data'
expectations_path = Path(__file__).parent/'includes'/'DataIngestion'/'great_expectations'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_expectation_existence():
    if not expectations_path.exists():
        return 'CreateExpectationSuite'
    else:
        return 'SkipStep'

# Create the DAG
with DAG("weather_data_ingestion_dag",
            start_date=datetime(2025, 5, 23, tz='local'),
            schedule=None,
            catchup=False,
            catchup=False,
            tags=["weather","portfolio","data_ingestion"],
            on_failure_callback=task_failure_alert,
            default_args={
                "owner": "airflow",
                "retries": 3,
                "retry_delay": relativedelta(seconds=5),
                "on_failure_callback": task_failure_alert
            },
            ) as dag:

            # Define the tasks
            fetch_data_task = PythonOperator(
                        task_id="DataFetching",
                        python_callable=get_weather_data,
                        op_kwargs={
                            "start_date": datetime.now() - relativedelta(years=2),
                            "end_date": datetime.now(),
                            "save_data":True
                        },
                        provide_context=True
                    )

            check_expectation_existence_task = BranchPythonOperator(
                task_id="CheckExpectationExistence",
                python_callable=check_expectation_existence,
            )

            create_expectation_suite = PythonOperator(
                task_id="CreateExpectationSuite",
                python_callable=setup_expectations,
                op_kwargs={
                    "expectations_path": expectations_path
                },
                provide_context=True
            )

            skip_step = EmptyOperator(
                task_id="SkipStep",
            )

            validate_data_task = PythonOperator(
                task_id="DataValidation",
                python_callable=run_validation,
                op_kwargs={
                    "expectations_path": expectations_path
                },
                provide_context=True,
                trigger_rule = "none_failed_min_one_success"
            )

            fetch_data_task >>  check_expectation_existence_task >> [create_expectation_suite, skip_step] >> validate_data_task
import os
import sys
import logging
from pathlib import Path
from dateutil.relativedelta import relativedelta
from pendulum import datetime
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from includes.DataIngestion.scrape_data import get_weather_data
from includes.DataIngestion.ge_setup import setup_expectations
from includes.DataIngestion.validate_data import run_validation
from includes.Monitoring.monitor import monitor_drift
from includes.Training.train import train_and_log_model
from includes.Callbacks.alert import task_failure_alert

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

# Paths
DATA_DIR = os.getenv("DATA_PATH")
EXPECTATIONS_PATH = os.getenv("EXPECTATIONS_PATH")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------
# Branching functions
# ---------------------
def check_model_decay(**kwargs):
    """Decide whether to stop or continue DAG based on decay test result."""
    ti = kwargs["ti"]
    decay_result = ti.xcom_pull(
        task_ids="MonitorModelDecay",
        key="MonitorModelDecay"
    )

    # SUCCESS means NO decay → continue; otherwise → alert Slack & retrain
    if decay_result == "TestStatus.SUCCESS":
        logger.info("✅ No model decay detected")
        return "stop_dag"
    else:
        logger.warning("⚠️ Model decay detected. Triggering Slack alert.")
        return "notify_slack_model_decay"

def check_expectation_existence():
    """Check if expectation suite exists before validation."""
    if not EXPECTATIONS_PATH.exists():
        return 'CreateExpectationSuite'
    else:
        return 'SkipStep'

# ---------------------
# DAG Definition
# ---------------------
with DAG(
    "weather_data_ingestion_dag",
    start_date=datetime(2025, 5, 23, tz='local'),
    schedule=None,
    catchup=False,
    tags=["weather", "portfolio", "data_ingestion"],
    on_failure_callback=task_failure_alert,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": relativedelta(seconds=5),
        "on_failure_callback": task_failure_alert
    },
) as dag:

    # 1. Monitor model drift/decay
    model_monitoring_task = PythonOperator(
        task_id="MonitorModelDecay",
        python_callable=monitor_drift,
        provide_context=True
    )

    # 2. Decide branch based on decay result
    check_model_decay_task = BranchPythonOperator(
        task_id="IsModelDecay?",
        python_callable=check_model_decay,
    )

    # 3. Slack alert if decay detected
    alert_slack_task = SlackAPIPostOperator(
        task_id='notify_slack_model_decay',
        slack_conn_id='slack_default',
        text=(
            ":red_circle: Model Decay detected.\n"
            "Need attention! Instantiating retraining to mitigate it.\n"
            f"*Execution Date*: {datetime.now()}\n"
        ),
        channel="#issues",
        username="airflow-bot"
    )

    # 4. Fetch new data
    fetch_data_task = PythonOperator(
        task_id="DataFetching",
        python_callable=get_weather_data,
        op_kwargs={
            "start_date": datetime.now() - relativedelta(years=2),
            "end_date": datetime.now(),
            "save_data": True
        },
        provide_context=True
    )

    # 5. Version data with DVC
    version_data_task = BashOperator(
        task_id='DataVersioning',
        bash_command=f"dvc add {DATA_DIR}/weather_data.csv"
    )

    # 6. Check expectations existence
    check_expectation_existence_task = BranchPythonOperator(
        task_id="CheckExpectationExistence",
        python_callable=check_expectation_existence,
    )

    create_expectation_suite = PythonOperator(
        task_id="CreateExpectationSuite",
        python_callable=setup_expectations,
        op_kwargs={"expectations_path": EXPECTATIONS_PATH},
        provide_context=True
    )

    skip_step = EmptyOperator(task_id="SkipStep")

    # 7. Validate data
    validate_data_task = PythonOperator(
        task_id="DataValidation",
        python_callable=run_validation,
        op_kwargs={"expectations_path": EXPECTATIONS_PATH},
        provide_context=True,
        trigger_rule="none_failed_min_one_success"
    )

    # 8. Train model
    train_model_task = PythonOperator(
        task_id="ModelTraining",
        python_callable=train_and_log_model,
        op_kwargs={"expectations_path": EXPECTATIONS_PATH},
        provide_context=True,
    )

    # 9. Stop DAG if no decay
    stop_dag = EmptyOperator(task_id="stop_dag")

    # ---------------------
    # Task Dependencies
    # ---------------------
    model_monitoring_task >> check_model_decay_task
    check_model_decay_task >> alert_slack_task >> fetch_data_task
    check_model_decay_task >> stop_dag

    fetch_data_task >> version_data_task
    version_data_task >> check_expectation_existence_task
    check_expectation_existence_task >> [create_expectation_suite, skip_step] >> validate_data_task
    validate_data_task >> train_model_task

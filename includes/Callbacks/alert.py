import logging
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

def task_failure_alert(context):
    ti = context.get('task_instance').id
    dag_name = context.get('task_instance').dag_id
    task_name = context.get('task_instance').task_id
    execution_date = context.get('task_instance').start_date
    log_url = context.get('task_instance').hostname
    dag_run = context.get('task_instance_key_str')

    logging.warning("Slack alert triggered!")
    slack_alert = SlackAPIPostOperator(
        task_id='notify_slack_failure',
        slack_conn_id = 'slack_default',
        text=(
            ":red_circle: Task Failed.\n"
            f"*DAG*: {dag_name}\n"
            f"*Task*: {task_name}\n"
            f"*Execution Time*: {execution_date}\n"
            f"*Task Instance*: {ti}\n"
            f"*Log URL*: {log_url}\n"
            f"*Dag Run*: {dag_run}\n"
        ),
        channel="#issues",
        username="airflow-bot" )
    return  slack_alert.execute(context=context)
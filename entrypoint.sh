#!/bin/bash
airflow connections import /opt/airflow/config/airflow_conns.json
exec airflow api-server

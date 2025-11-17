
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
CONN_ID = "ff0a5449-86d7-4312-8487-15b936f7b038"

with DAG(
    dag_id="example_airbyte_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    catchup=False,
) as dag:

    async_source_destination = AirbyteTriggerSyncOperator(
        task_id="airbyte_async_source_dest_example",
        connection_id=CONN_ID,
        asynchronous=False,
        airbyte_conn_id="airbyte_default",
    )

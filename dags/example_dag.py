"""
Example DAG for testing Airflow installation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_dataops_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['example', 'dataops'],
)

def print_hello():
    """Simple Python function"""
    print("Hello from Airflow!")
    print("This is a test DAG running in your DataOps environment")
    return "Success"

def print_info():
    """Print environment information"""
    import sys
    print(f"Python version: {sys.version}")
    print(f"Task executed at: {datetime.now()}")
    return "Info printed"

# Task 1: Print hello
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 2: Print environment info
task_info = PythonOperator(
    task_id='print_info',
    python_callable=print_info,
    dag=dag,
)

# Task 3: Run a bash command
task_bash = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Define task dependencies
task_hello >> task_info >> task_bash

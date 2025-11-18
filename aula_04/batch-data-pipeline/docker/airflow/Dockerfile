FROM apache/airflow:2.9.0-python3.11

USER root

COPY requirements.txt /requirements.txt
RUN chmod 644 /requirements.txt

# Use default Airflow user to install dependencies
USER 50000

# Install with increased timeout and retries to handle network issues
RUN pip install --no-cache-dir --default-timeout=300 --retries=5 apache-airflow==2.9.0 -r /requirements.txt

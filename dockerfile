
FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
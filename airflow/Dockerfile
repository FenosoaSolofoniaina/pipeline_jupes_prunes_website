FROM apache/airflow:2.7.1-python3.12

WORKDIR /app/airflow/

COPY ./../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY dags/ ${AIRFLOW_HOME}/dags/

FROM apache/airflow:3.1.0-python3.12

USER airflow

RUN mkdir -p /opt/airflow/dags/ /opt/airflow/logs/ /opt/airflow/plugins/

WORKDIR /opt/airflow/

# COPY ./airflow/dags/ /opt/airflow/dags/
# COPY ./airflow/plugins/ /opt/airflow/plugins/

CMD ["bash", "-c", "airflow db init && airflow scheduler & airflow webserver"]
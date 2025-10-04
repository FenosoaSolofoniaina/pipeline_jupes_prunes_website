FROM apache/airflow:3.1.0-python3.12


USER airflow


RUN mkdir -p /opt/airflow/dags/ /opt/airflow/logs/ /opt/airflow/plugins/


WORKDIR /opt/airflow/


CMD ["bash", "-c", "airflow db init && airflow scheduler & airflow webserver"]
FROM apache/airflow:3.1.0-python3.12 AS base

USER airflow

RUN pip install --no-cache-dir docker

WORKDIR /opt/airflow

COPY . .

CMD ["bash", "-c", "'airflow db migrate && aiflow standalone'"]
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 4),
    'retries': 1
}

with DAG(
    'pipeline_les_petites_jupes_de_prunes',
    default_args=default_args,
    schedule_interval='None',
    catchup=False
) as dag:

    # 1. Web scraping
    scrape = DockerOperator(
        task_id='scrape_data',
        image='web_scraper_app:latest',   # ton image scraper
        auto_remove=True,
        command='python3 extract_data.py',          # ton script principal de scraping
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',             # permet aux conteneurs de communiquer
        mount_tmp_dir=False
    )

    # 2️⃣ DBT transformations
    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='dbt_app:latest',           # ton image DBT
        auto_remove=True,
        command='dbt run --profiles-dir /app/dbt_part/.dbt',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    # Définir l’ordre d’exécution : scraping → DBT
    scrape >> dbt_run

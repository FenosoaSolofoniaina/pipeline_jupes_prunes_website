import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
# from docker.types import Mount



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 10),
    'retries': 1,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id='pipeline_les_petites_jupes_de_prunes_v1',
    description='Run pipeline that extract data in the website to push it on the cloud',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    load_dotenv()

    BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), '..', '..'))
    DOCKER_URL = 'unix://var/run/docker.sock'
    PROJECT_PREFIX = os.getenv('PROJECT_PREFIX')
    
    # Task 1
    init_wf = BashOperator(
        task_id='bash.Initializing',
        bash_command='echo "INITIALIZING THE WORKFLOW ..."'
    )

    # Task 2
    scrape_wf = DockerOperator(
        task_id='docker.Web_scraping',
        docker_url=DOCKER_URL,
        image=f'{PROJECT_PREFIX}-scraper:latest',
        command=["python3", "extract_data.py"],
        network_mode='bridge',
        auto_remove='success',
        # mounts = [
        #     Mount(source=f'{BASE_DIR}/service-account.json',
        #           target='/app/scraper/service-account.json',
        #           type='bind')
        # ],
        force_pull=False
    )

    # Task 3
    dbt_wf = DockerOperator(
        task_id='docker.DBT',
        docker_url=DOCKER_URL,
        image=f'{PROJECT_PREFIX}-dbt:latest',
        command=["dbt", "run", "--profiles-dir", "/app/dbt_part/.dbt"],
        network_mode='bridge',
        auto_remove='success',
        # mounts = [
        #     Mount(source=f'{BASE_DIR}/service-account.json',
        #           target='/app/dbt_part/service-account.json',
        #           type='bind')
        # ],
        force_pull=False
    )

    # Task 4
    end_wf = BashOperator(
        task_id='bash.Ending',
        bash_command='echo "CLOSING THE WORKFLOW..."'
    )

    init_wf >> scrape_wf >> dbt_wf >> end_wf

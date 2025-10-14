from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 10),
    'retries': 1
}

with DAG(
    dag_id='pipeline_les_petites_jupes_de_prunes_v1',
    description='Run pipeline that extract data in the website to push it on the cloud',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    catchup=False
) as dag:

    init_wf = BashOperator(
        task_id='bash_model_initializing',
        bash_command='echo "INITIALIZING THE WORKFLOW ..."'
    )

    scrape_wf = DockerOperator(
        task_id='docker_model_scrape_data',
        docker_url='unix://var/run/docker.sock',
        image='web_scraper_app:latest',
        container_name='web_scraper_app',
        mounts=[
            Mount(source='./../../scraper',
                  target='/app/scraper',
                  type='bind')
        ],
        command=["python3", "extract_data.py"],
        auto_remove=True,
        network_mode='bridge',
        dag=dag
    )

    dbt_wf = DockerOperator(
        task_id='docker_model_dbt_transformation',
        docker_url='unix://var/run/docker.sock',
        image='dbt_app:latest',
        container_name='dbt_app',
        mounts = [
            Mount(source='./../../dbt_part',
                  target='/app/dbt_part',
                  type='bind'),
            Mount(source='./../../dbt_part/.dbt/profiles.yml',
                  target='/app/dbt_part/.dbt/profiles.yml',
                  type='bind')
        ],
        command=["dbt", "run", "--profiles-dir", "/app/dbt_part/.dbt"],
        auto_remove=True,
        network_mode='bridge',
        dag=dag
    )

    end_wf = BashOperator(
        task_id='bash_model_ending',
        bash_command='echo "CLOSING THE WORKFLOW..."'
    )

    init_wf >> scrape_wf >> dbt_wf >> end_wf

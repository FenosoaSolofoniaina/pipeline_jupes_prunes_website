from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 10),
    'retries': 1
}

with DAG(
    dag_id='pipeline_les_petites_jupes_de_prunes_v1',
    description='Run pipeline that extract data in the website to push it on the cloud',
    default_args=default_args,
    schedule_interval=None,
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
        auto_remove=True,
        command=["python3", "extract_data.py"],
        network_mode='bridge',
        dag=dag
    )

    dbt_wf = DockerOperator(
        task_id='docker_model_dbt_transformation',
        docker_url='unix://var/run/docker.sock',
        image='dbt_app:latest',
        command=["dbt", "run", "--profiles-dir", "/app/dbt_part/.dbt"],
        container_name='dbt_app',
        auto_remove=True,
        network_mode='bridge',
        dag=dag
    )

    end_wf = BashOperator(
        task_id='bash_model_ending',
        bash_command='echo "CLOSING THE WORKFLOW..."'
    )

    init_wf >> scrape_wf >> dbt_wf >> end_wf

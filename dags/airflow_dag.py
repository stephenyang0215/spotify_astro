from datetime import datetime, timedelta
from Extract import Extract
from Load import load_snoflake
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os

# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/spotify"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="db_conn",
        profile_args={"schema": "raw",
                    "database": "spotify",
                    "account": "zjb36759.us-east-1",
                    "login": "syang215",
                    "password": "siWqyg-7jizwa-xosdox",
                    "warehouse": "compute_wh",
                    "role": "accountadmin"}
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': 'False',
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@once'
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify API Pipeline Processing 1-min',
    schedule=timedelta(minutes=30),
     
)
def spotify_etl():
    print('Job Initiated.')
    extract = Extract()
    result = extract.search_for_artist('ACDC')
    spotify_pd = extract.get_songs_by_artist(result['id'])
    load_snoflake(spotify_pd)
    return spotify_pd    

with dag:
    e1 = EmptyOperator(task_id="pre_processing")

    Spotify_ETL = PythonOperator(
        task_id='Extract_Load',
        python_callable=spotify_etl,
        dag = dag,
    )

    dbt_tg = DbtTaskGroup(
        group_id="DBT_Transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config
    )

    e2 = EmptyOperator(task_id="post_processing")
    
    e1 >> Spotify_ETL >> dbt_tg >> e2
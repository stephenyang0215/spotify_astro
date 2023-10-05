from datetime import datetime, timedelta
from Extract import Extract
from Load import GET_SONGS_BY_ARTIST, load_snoflake_conn, recommendation_load
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
extract = Extract()
def extract_load():
    print('Job Initiated.')
    artist_track = extract.search_for_artist('ACDC')
    return artist_track
    #spotify_pd = extract.get_songs_by_artist(artist['id'])
    #recommendation = extract.get_recommendation(artist['id'], artist['genres'], track['id'])
    #engine, cur = load_snoflake_conn()
    #GET_SONGS_BY_ARTIST(engine, cur, spotify_pd)
    #recommendation(engine, cur, recommendation)
def func1(ti):
    # Pulls the return_value XCOM from "pushing_task"
    artist_track = ti.xcom_pull(task_ids='Extract_Load')
    spotify_pd = extract.get_songs_by_artist(artist_track['artist_id'])
    engine, cur = load_snoflake_conn()
    GET_SONGS_BY_ARTIST(engine, cur, spotify_pd)

def func2(ti):
    # Pulls the return_value XCOM from "pushing_task"
    artist_track = ti.xcom_pull(task_ids='Extract_Load')
    recommendation = extract.get_recommendation(artist_track['artist_id'], artist_track['artist_genres'], artist_track['track_id'])
    engine, cur = load_snoflake_conn()
    recommendation_load(engine, cur, recommendation)

with dag:
    e1 = EmptyOperator(task_id="pre_processing")

    Spotify_EL = PythonOperator(
        task_id='Extract_Load',
        python_callable=extract_load,
        dag = dag,
    )
    
    Spotify_get_songs_by_artist = PythonOperator(
        task_id='get_songs_by_artist',
        python_callable=func1,
        dag = dag,
    )

    Spotify_recommendation = PythonOperator(
        task_id='get_recommendation',
        python_callable=func2,
        dag = dag,
    )

    dbt_tg = DbtTaskGroup(
        group_id="DBT_Transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config
    )

    e2 = EmptyOperator(task_id="post_processing")
    
    e1 >> Spotify_EL >>  [Spotify_get_songs_by_artist, Spotify_recommendation] >> dbt_tg >> e2
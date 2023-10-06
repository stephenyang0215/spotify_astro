from datetime import datetime, timedelta
from Extract import Extract
from Load import load_snoflake_conn, load_snowflake
from airflow import DAG
import pandas as pd
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os

load_dotenv()
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
        profile_args={
            "schema": os.getenv('snowflake_schema'),
            "database": os.getenv('snowflake_db'),
            "account": os.getenv('snowflake_account'),
            "login": os.getenv("snowflake_user"),
            "password": os.getenv("snowflake_password"),
            "warehouse": os.getenv("snowflake_warehouse"),
            "role": os.getenv("snowflake_role")}
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

def export_from_snowflake():
    album_id_pd = extract.export_from_snowflake()
    return album_id_pd

def search_for_artist():
    print('Job Initiated.')
    artist_track = extract.search_for_artist('ACDC')
    return artist_track

def load_new_releases():
    sql = """CREATE OR REPLACE TABLE new_releases
        (album_type string, 
        album_total_tracks string, 
        album_available_markets string,
        album_href string, 
        album_id string, 
        album_name string, 
        album_release_date string,
        album_release_date_precision string, 
        album_uri string)"""
    new_releases = extract.get_new_releases()
    engine, cur = load_snoflake_conn()
    load_snowflake(engine, cur, new_releases, sql, 'new_releases')

def load_search_for_artist(ti):
    sql = """CREATE OR REPLACE TABLE GET_SONGS_BY_ARTIST
        (ALBUM string,
        ALBUM_ID string,
        ALBUM_TYPE string,
        TRACK string)"""
    # Pulls the return_value XCOM from "pushing_task"
    artist_track = ti.xcom_pull(task_ids='Extract_Load')
    spotify_pd = extract.get_songs_by_artist(artist_track['artist_id'])
    engine, cur = load_snoflake_conn()
    load_snowflake(engine, cur, spotify_pd, sql, 'get_songs_by_artist')

def load_recommendation(ti):
    sql = """CREATE OR REPLACE TABLE RECOMMENDATION
        (ALBUM_TYPE string, 
        ALBUM_TOTAL_TRACKS string, 
        ALBUM_AVAILABLE_MARKETS string,
        ALBUM_HREF string, 
        ALBUM_id string, 
        ALBUM_name string, 
        ALBUM_release_date string,
        ALBUM_release_date_precision string, 
        ALBUM_URI string,
        ARTIST_HREF string, 
        ARTIST_ID string, 
        ARTIST_NAME string, 
        ARTIST_TYPE string, 
        ARTIST_URI string)"""
    # Pulls the return_value XCOM from "pushing_task"
    artist_track = ti.xcom_pull(task_ids='Extract_Load')
    recommendation = extract.get_recommendation(artist_track['artist_id'], artist_track['artist_genres'], artist_track['track_id'])
    engine, cur = load_snoflake_conn()
    load_snowflake(engine, cur, recommendation, sql, 'recommendation')

def new_releases_album_tracks_load(ti):
    sql = """CREATE OR REPLACE TABLE new_releases_album_tracks
        (artists_href string, 
        artists_id string, 
        artists_name string,
        artists_type string, 
        artists_uri string, 
        track_href string, 
        track_id string,
        track_name string, 
        track_type string,
        track_uri string)"""
    main_df = pd.DataFrame(columns=['artists_href', 'artists_id', 'artists_name', 'artists_type',
       'artists_uri', 'track_href', 'track_id', 'track_name', 'track_type','track_uri'])
    # Pulls the return_value XCOM from "pushing_task"
    album_id_lst = ti.xcom_pull(task_ids='new_releases_album_id')
    for album_id in album_id_lst:
        album_tracks = extract.get_track_by_album(album_id)
        main_df = pd.concat([main_df,album_tracks], ignore_index=True)
    engine, cur = load_snoflake_conn()
    load_snowflake(engine, cur, main_df, sql, 'new_releases_album_tracks')
    
with dag:
    e1 = EmptyOperator(task_id="pre_processing")

    spotify_load_new_releases = PythonOperator(
        task_id='new_releases',
        python_callable=load_new_releases,
        dag = dag,
    )

    spotify_new_releases_album_id = PythonOperator(
        task_id='new_releases_album_id',
        python_callable=export_from_snowflake,
        dag = dag,
    )

    spotify_new_releases_album_tracks_load = PythonOperator(
        task_id='new_release_album_track',
        python_callable=new_releases_album_tracks_load,
        dag = dag,
    )
    
    Spotify_Extract = PythonOperator(
        task_id='Extract_Load',
        python_callable=search_for_artist,
        dag = dag,
    )
    
    spotify_load_get_songs_by_artist = PythonOperator(
        task_id='get_songs_by_artist',
        python_callable=load_search_for_artist,
        dag = dag,
    )

    spotify_load_recommendation = PythonOperator(
        task_id='get_recommendation',
        python_callable=load_recommendation,
        dag = dag,
    )

    dbt_songs_recommendation = DbtTaskGroup(
        group_id="DBT_Transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config
    )

    e2 = EmptyOperator(task_id="post_processing")
    
    e1 >> spotify_load_new_releases >> spotify_new_releases_album_id >> spotify_new_releases_album_tracks_load
    e1 >> Spotify_Extract >> [spotify_load_get_songs_by_artist, spotify_load_recommendation] 
    [spotify_new_releases_album_tracks_load, spotify_load_recommendation] >> dbt_songs_recommendation >> e2
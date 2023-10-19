from datetime import datetime, timedelta
from Extract import Extract
from Load import load_snoflake_conn, load_snowflake, verify_internal_stage, staged_files_load, write_sql_file
from schema_load import json_schema_auto
from airflow import DAG
import pandas as pd
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os
import json

extract = Extract()

load_dotenv()

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

_, conn = load_snoflake_conn()

@dag(schedule='@daily', start_date=datetime.now(), catchup=False,)
def taskflow():
    @task(task_id='extract_load_json', retries=0)
    def extract_load_json(table_name: str, url:str):
        column_lst = extract.extract_spotify_json_file(url, table_name)
        verify_internal_stage(conn)
        staged_files_load(conn, table_name, column_lst, f'{table_name}.json')
        sql = json_schema_auto(f"{os.environ['AIRFLOW_HOME']}"+f'/files/{table_name}.json', table_name)
        write_sql_file(sql, table_name)
        return table_name
    
    @task(task_id='export_id')
    def export_id(target_column: str, schema: str, table_name: str):#parameters require upper format 
            id_lst = extract.export_id_list(target_column, schema, table_name)
            return id_lst
    
    @task(task_id='concatenated_table')
    def concatenate_table(id_lst: list, column_lst: list, table_name: str):
        main_df = pd.DataFrame(columns=column_lst)
        for id in id_lst:
            if table_name == 'new_releases':
                data = extract.get_track_by_album(id)
            elif table_name == 'featured_playlists':
                data = extract.get_playlist(id)
            elif table_name == 'browse_categories':
                data = extract.get_category_playlists(id)
            main_df = pd.concat([main_df,data], ignore_index=True)
        return main_df

    @task(task_id='load_table')
    def load_table(table, sql, table_name):
        engine, cur = load_snoflake_conn()
        load_snowflake(engine, cur, table, sql, table_name)
        return 
    
    @task(task_id='bash_dbt')
    def dbt_run(model):
        GLOBAL_CLI_FLAGS = "--no-write-json"
        dbt_verb = "run"
        #model = node.split('.')[-1]
        DBT_DIR = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/spotify/models"
        PROJECT_DIR = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/spotify/dbt_project.yml"
        BashOperator(
                    task_id='test',
                    bash_command=f"""
                    cd {DBT_DIR} && dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --select {model}
                    """
                )
        print('Run DBT Seccessfully!')
        return model

    browse_categories_sql = """CREATE OR REPLACE TABLE browse_categories_playists
            (collaborative string, 
            description string, 
            href string,
            id string, 
            name string, 
            public string, 
            snapshot_id string,
            type string, 
            uri string)
    """
    new_releases_sql = """CREATE OR REPLACE TABLE new_releases_album_tracks
            (artists_href string, 
            artists_id string, 
            artists_name string,
            artists_type string, 
            artists_uri string, 
            track_href string, 
            track_id string,
            track_name string, 
            track_type string,
            track_uri string,
            album_id string)"""
    
    featured_playlists_sql = """CREATE OR REPLACE TABLE featured_playlists_albums_artists_tracks
            (album_type string, 
            album_total_tracks string, 
            album_available_markets string,
            album_id string, 
            album_name string, 
            album_release_date string, 
            album_uri string,
            artist_id string, 
            artist_name string, 
            artist_uri string, 
            track_id string, 
            track_name string,
            track_popularity string, 
            track_uri string, 
            total string)"""
    
    browse_categories_col_lst = ['collaborative', 'description', 'href', 'id', 
            'name', 'public', 'snapshot_id', 'type', 'uri']
    new_releases_col_lst = ['artists_href', 'artists_id', 'artists_name', 'artists_type',
        'artists_uri', 'track_href', 'track_id', 'track_name', 'track_type','track_uri', 'album_id']
    featured_playlists_col_lst = ['artists_href', 'artists_id', 'artists_name', 'artists_type',
        'artists_uri', 'track_href', 'track_id', 'track_name', 'track_type','track_uri', 'album_id']

    browse_categories = extract_load_json(table_name = 'browse_categories',
                                   url = 'https://api.spotify.com/v1/browse/categories?country=US&limit=50')

    featured_playlists = extract_load_json(table_name = 'featured_playlists',
                                   url = 'https://api.spotify.com/v1/browse/featured-playlists?country=US&limit=50')
    
    new_releases = extract_load_json(table_name = 'new_releases',
                                   url = 'https://api.spotify.com/v1/browse/new-releases?country=US&limit=30')
    
    staging_model = dbt_run('staging')
    
    [browse_categories, featured_playlists, new_releases] >> staging_model

    browse_categories_id_lst = export_id('CATEGORIES_ITEMS_ID', staging_model, 'browse_categories')
    featured_playlists_id_lst = export_id('PLAYLISTS_ITEMS_ID',staging_model, 'featured_playlists')
    new_releases_id_lst = export_id('ALBUMS_ITEMS_ID', staging_model, 'new_releases')

    load_task1 = concatenate_table(browse_categories_id_lst, browse_categories_col_lst, 'browse_categories')
    load_task2 = concatenate_table(featured_playlists_id_lst, featured_playlists_col_lst, 'featured_playlists')
    load_task3 = concatenate_table(new_releases_id_lst, new_releases_col_lst, 'new_releases')

    load1 = load_table(load_task1, browse_categories_sql, 'browse_categories_playists')
    load2 = load_table(load_task2, featured_playlists_sql, 'featured_playlists_albums_artists_tracks')
    load3 = load_table(load_task3, new_releases_sql, 'new_releases_album_tracks')

    browse_categories_model = dbt_run('browse_categories')
    featured_playlists_model = dbt_run('featured_playlists')
    new_releases_model = dbt_run('new_releases')

    load1 >> browse_categories_model
    load2 >> featured_playlists_model
    load3 >> new_releases_model

taskflow()


'''dbt_enrich = DbtTaskGroup(
        group_id="DBT_Transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config)'''
'''def load_manifest():
        local_filepath = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/target/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)

        return data
    def make_dbt_task(node, dbt_verb):
        """Returns an Airflow operator either run and test an individual model"""
        DBT_DIR = "/usr/local/airflow/dags/dbt"
        GLOBAL_CLI_FLAGS = "--no-write-json"
        model = node.split(".")[-1]

        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                bash_command=f"""
                cd {DBT_DIR} &&
                dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
                """,
                dag=dag,
            )

        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node_test,
                bash_command=f"""
                cd {DBT_DIR} &&
                dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
                """,
                dag=dag,
            )

        return dbt_task'''
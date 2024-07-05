from datetime import datetime, timedelta
from Extract import Extract
from Load import load_snoflake_conn, load_snowflake, verify_internal_stage, json_files_load, write_sql_file
from semi_struct_flat import json_schema_auto
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from cosmos import ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os
import yaml

extract = Extract()

with open('profiles.yml', 'r') as file:
    yaml_content = yaml.safe_load(file)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="db_conn",
        profile_args={
            "schema": yaml_content['spotify']['outputs']['dev']['schema'],
            "database": yaml_content['spotify']['outputs']['dev']['database'],
            "account": yaml_content['spotify']['outputs']['dev']['account'],
            "login": yaml_content['spotify']['outputs']['dev']['user'],
            "password": yaml_content['spotify']['outputs']['dev']['password'],
            "warehouse": yaml_content['spotify']['outputs']['dev']['warehouse'],
            "role": yaml_content['spotify']['outputs']['dev']['role']}
    ),
)

with open('files/browse_category_playlist.sql', 'r') as browse_category, \
    open('files/featured_playlist_album_artist_track.sql', 'r') as featured_playlist, \
    open('files/new_release_album_track.sql', 'r') as new_release: \

    browse_category_sql = browse_category.read()
    featured_playlist_sql = featured_playlist.read()
    new_release_sql = new_release.read()

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

@dag(schedule='@daily', start_date=datetime.now(), catchup=False,)
def taskflow():
    @task(task_id='extract_load_json', retries=0)
    def extract_json(table_name: str, url:str):
        _, conn = load_snoflake_conn(schema='raw')
        column_lst = extract.extract_spotify_json_file(url, table_name)
        verify_internal_stage(conn)
        json_files_load(conn, table_name, column_lst, f'{table_name}.json')
        sql = json_schema_auto(f"{os.environ['AIRFLOW_HOME']}"+f'/files/{table_name}.json', table_name)
        write_sql_file(sql, table_name)
        return table_name


    @task(task_id='sub_extraction')
    def sub_extract(target_column: str, column_lst: list, table_name: str):
        id_lst = extract.export_id_list(target_column, table_name)
        main_df = pd.DataFrame(columns=column_lst)
        for id in id_lst:
            if table_name == 'new_release':
                data = extract.get_track_by_album(id)
            elif table_name == 'featured_playlist':
                data = extract.get_playlist(id)
            elif table_name == 'browse_category':
                if any(i.isdigit() for i in id):
                    data = extract.get_category_playlists(id)
                else:
                    data = None
            try:
                main_df = pd.concat([main_df,data], ignore_index=True)
            except:
                pass
        return main_df


    @task(task_id='load_table')
    def load(table, sql, table_name):
        engine, conn = load_snoflake_conn(schema='staging')
        load_snowflake(engine, conn, table, sql, table_name)
        return 


    # Fetch the json file for browse categories through endpoints
    browse_category = extract_json(table_name = 'browse_category',
                                   url = 'https://api.spotify.com/v1/browse/categories?country=US&limit=50')
    # Fetch the json file for featured playlists through endpoints
    featured_playlist = extract_json(table_name = 'featured_playlist',
                                   url = 'https://api.spotify.com/v1/browse/featured-playlists?country=US&limit=50')
    # Fetch the json file for new releases through endpoints
    new_release = extract_json(table_name = 'new_release',
                                   url = 'https://api.spotify.com/v1/browse/new-releases?country=US&limit=30')

    dbt_verb = "run"
    DBT_DIR = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/spotify"
    dbt_flatten_bash1 = BashOperator(
                task_id='dbt_flatten_bash1',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {browse_category}_flatten
                """
            )
    dbt_flatten_bash2 = BashOperator(
                task_id='dbt_flatten_bash2',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {featured_playlist}_flatten
                """
            )
    dbt_flatten_bash3 = BashOperator(
                task_id='dbt_flatten_bash3',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {new_release}_flatten
                """
            )

    browse_category_col_lst = ['collaborative', 'description', 'href', 'id',
                                 'name', 'public', 'snapshot_id', 'type', 'uri', 'category_id']

    new_release_col_lst = ['artists_href', 'artists_id', 'artists_name', 'artists_type',
                            'artists_uri', 'track_href', 'track_id', 'track_name', 'track_type', 'track_uri',
                            'album_id']

    featured_playlist_col_lst = ['album_type', 'album_total_tracks', 'album_available_markets', 'album_id',
                                  'album_name', 'album_release_date', 'album_uri', 'artist_id', 'artist_name',
                                  'artist_uri', 'track_id', 'track_name', 'track_popularity', 'track_uri', 'total',
                                  'playlist_id']

    sub_extract_1 = sub_extract('CATEGORIES_ITEMS_ID', browse_category_col_lst, 'browse_category')
    sub_extract_2 = sub_extract('PLAYLISTS_ITEMS_ID', featured_playlist_col_lst, 'featured_playlist')
    sub_extract_3 = sub_extract('ALBUMS_ITEMS_ID', new_release_col_lst, 'new_release')

    load_1 = load(sub_extract_1, browse_category_sql, 'browse_category_playist')
    load_2 = load(sub_extract_2, featured_playlist_sql, 'featured_playlist_album_artist_track')
    load_3 = load(sub_extract_3, new_release_sql, 'new_release_track')

    dbt_bash1 = BashOperator(
                task_id='dbt_bash1',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --select {browse_category}_agg --target prod
                """
            )
    
    dbt_bash2 = BashOperator(
                task_id='dbt_bash2',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --select {featured_playlist}  --target prod
                """
            )
    
    dbt_bash3 = BashOperator(
                task_id='dbt_bash3',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {new_release}_album_track  --target prod
                """
            )

    browse_category >> dbt_flatten_bash1 >> sub_extract_1 >> load_1 >> dbt_bash1
    featured_playlist >> dbt_flatten_bash2 >> sub_extract_2 >> load_2 >> dbt_bash2
    new_release >> dbt_flatten_bash3 >> sub_extract_3 >> load_3 >> dbt_bash3

taskflow()
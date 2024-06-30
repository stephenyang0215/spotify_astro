from datetime import datetime, timedelta
from Extract import Extract
from Load import load_snoflake_conn, load_snowflake, verify_internal_stage, json_files_load, write_sql_file
from schema_load import json_schema_auto
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

with open('files/browse_categories_playlists.sql', 'r') as browse_categories, \
    open('files/featured_playlists_albums_artists_tracks.sql', 'r') as featured_playlists, \
    open('files/new_releases_album_tracks.sql', 'r') as new_releases: \

    browse_categories_sql = browse_categories.read()
    featured_playlists_sql = featured_playlists.read()
    new_releases_sql = new_releases.read()

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
        json_files_load(conn, table_name, column_lst, f'{table_name}.json')
        sql = json_schema_auto(f"{os.environ['AIRFLOW_HOME']}"+f'/files/{table_name}.json', table_name)
        write_sql_file(sql, table_name)
        return table_name


    @task(task_id='concatenated_table')
    def concatenate_table(target_column: str, column_lst: list, table_name: str):
        id_lst = extract.export_id_list(target_column, table_name)
        main_df = pd.DataFrame(columns=column_lst)
        for id in id_lst:
            if table_name == 'new_releases':
                data = extract.get_track_by_album(id)
            elif table_name == 'featured_playlists':
                data = extract.get_playlist(id)
            elif table_name == 'browse_categories':
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
    def load_table(table, sql, table_name):
        engine, cur = load_snoflake_conn()
        load_snowflake(engine, cur, table, sql, table_name)
        return 


    # Fetch the json file for browse categories through endpoints
    browse_categories = extract_load_json(table_name = 'browse_categories',
                                   url = 'https://api.spotify.com/v1/browse/categories?country=US&limit=50')
    # Fetch the json file for featured playlists through endpoints
    featured_playlists = extract_load_json(table_name = 'featured_playlists',
                                   url = 'https://api.spotify.com/v1/browse/featured-playlists?country=US&limit=50')
    # Fetch the json file for new releases through endpoints
    new_releases = extract_load_json(table_name = 'new_releases',
                                   url = 'https://api.spotify.com/v1/browse/new-releases?country=US&limit=30')

    dbt_verb = "run"
    DBT_DIR = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/spotify"
    dbt_flatten_bash1 = BashOperator(
                task_id='dbt_flatten_bash1',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {browse_categories}_flatten
                """
            )
    dbt_flatten_bash2 = BashOperator(
                task_id='dbt_flatten_bash2',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {featured_playlists}_flatten
                """
            )
    dbt_flatten_bash3 = BashOperator(
                task_id='dbt_flatten_bash3',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {new_releases}_flatten
                """
            )




    browse_categories_col_lst = ['collaborative', 'description', 'href', 'id',
                                 'name', 'public', 'snapshot_id', 'type', 'uri', 'category_id']

    new_releases_col_lst = ['artists_href', 'artists_id', 'artists_name', 'artists_type',
                            'artists_uri', 'track_href', 'track_id', 'track_name', 'track_type', 'track_uri',
                            'album_id']

    featured_playlists_col_lst = ['album_type', 'album_total_tracks', 'album_available_markets', 'album_id',
                                  'album_name', 'album_release_date', 'album_uri', 'artist_id', 'artist_name',
                                  'artist_uri', 'track_id', 'track_name', 'track_popularity', 'track_uri', 'total',
                                  'playlist_id']



    concat_task_1 = concatenate_table('CATEGORIES_ITEMS_ID', browse_categories_col_lst, 'browse_categories')
    concat_task_2 = concatenate_table('PLAYLISTS_ITEMS_ID', featured_playlists_col_lst, 'featured_playlists')
    concat_task_3 = concatenate_table('ALBUMS_ITEMS_ID', new_releases_col_lst, 'new_releases')

    
    load_task_1 = load_table(concat_task_1, browse_categories_sql, 'browse_categories_playists')
    load_task_2 = load_table(concat_task_2, featured_playlists_sql, 'featured_playlists_albums_artists_tracks')
    load_task_3 = load_table(concat_task_3, new_releases_sql, 'new_releases_album_tracks')

    dbt_bash1 = BashOperator(
                task_id='dbt_bash1',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --select {browse_categories}
                """
            )
    
    dbt_bash2 = BashOperator(
                task_id='dbt_bash2',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --select {featured_playlists}
                """
            )
    
    dbt_bash3 = BashOperator(
                task_id='dbt_bash3',
                bash_command=f"""source {os.environ['AIRFLOW_HOME']}/venv/bin/activate &&
                cd {DBT_DIR} && dbt {dbt_verb} --models {new_releases}
                """
            )

    browse_categories >> dbt_flatten_bash1 >> concat_task_1 >> load_task_1 >> dbt_bash1
    featured_playlists >> dbt_flatten_bash2 >> concat_task_2 >> load_task_2 >> dbt_bash2
    new_releases >> dbt_flatten_bash3 >> concat_task_3 >> load_task_3 >> dbt_bash3

taskflow()
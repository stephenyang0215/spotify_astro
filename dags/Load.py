from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import os
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer

def load_snoflake_conn():
    snowflake_user=os.getenv('snowflake_user')
    snowflake_password=os.getenv('snowflake_password')
    snowflake_account=os.getenv('snowflake_account')
    snowflake_db=os.getenv('snowflake_db')
    snowflake_schema=os.getenv('snowflake_schema')
    snowflake_warehouse=os.getenv('snowflake_warehouse')
    ctx = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account
        )
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    engine = create_engine(
        'snowflake://{0}:{1}@{2}/{3}/{4}?warehouse={5}'.format(
            snowflake_user,
            snowflake_password,
            snowflake_account,
            snowflake_db,
            snowflake_schema,
            snowflake_warehouse)
        )
        
    cur = ctx.cursor()
    sql = "USE ROLE ACCOUNTADMIN"
    cur.execute(sql)

    sql = "CREATE DATABASE IF NOT EXISTS SPOTIFY"
    cur.execute(sql)

    sql = "USE DATABASE SPOTIFY"
    cur.execute(sql)

    sql = "CREATE SCHEMA IF NOT EXISTS RAW"
    cur.execute(sql)

    sql = "USE SCHEMA RAW"
    cur.execute(sql)
    return engine, cur

def load_snowflake(engine, cur, data, sql, tb_name):
    cur.execute(sql)
    # Write the data from the DataFrame to the table named "GET_SONGS_BY_ARTIST".
    #change my columns in my dataframe to uppercase
    data.columns = map(lambda x: str(x).upper(), data.columns)
    data.to_sql(tb_name, engine, index=False, if_exists='replace', method=pd_writer)
    #success, nchunks, nrows, _ = write_pandas(ctx, data, 'GET_SONGS_BY_ARTIST')
    print('Successfully load the data to snowflake.')
    
'''
import sqlite3

db_path = 'sqlite:///spotify.sqlite'
def Load(db_path):
    #Create database engine and connections
    engine = sqlalchemy.create_engine(db_path)
    conn = sqlite3.connect('spotify.sqlite')
    cursor = conn.cursor()

    #Use SQL query to create average price table
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS artist_top_track(
        ALBUM VARCHAR(200),
        ALBUM_ID VARCHAR(200),
        ALBUM_TYPE VARCHAR(200),
        TRACK VARCHAR(200))
    """
    #Use SQL query to create average votes table
    sql_query_2 = """
    CREATE TABLE IF NOT EXISTS top_track_album(
        ALBUM VARCHAR(200),
        TRACK INTEGER(50))
    """
    cursor.execute(sql_query_1)
    cursor.execute(sql_query_2)
    print("Execute the database successfully")

    try:
        #append the data to the table
        spotify_pd.to_sql("artist_top_track", engine, index=False, if_exists='append')
    except:
        print("It failed to load the data to the database.")
    try:
        spotify_transform.to_sql("top_track_album", engine, index=False, if_exists='append')
    except:
        print("It failed to load the data to the database.")
    #Close the database connection
    conn.close()
    print('The database connection is closed!')
'''

    
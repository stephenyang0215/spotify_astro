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
    data.columns = map(lambda x: str(x).upper(), data.columns)
    data.to_sql(tb_name, engine, index=False, if_exists='replace', method=pd_writer)
    print('Successfully load the table to snowflake.')
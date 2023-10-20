from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import os
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
import json

def load_snoflake_conn():
    conn = snowflake.connector.connect(
        user=os.getenv('snowflake_user'),
        password=os.getenv('snowflake_password'),
        account=os.getenv('snowflake_account'),
        warehouse=os.getenv('snowflake_warehouse'),
        database=os.getenv('snowflake_db'),
        schema=os.getenv('snowflake_schema')
        )
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    engine = create_engine(
        'snowflake://{0}:{1}@{2}/{3}/{4}?warehouse={5}'.format(
            os.getenv('snowflake_user'),
            os.getenv('snowflake_password'),
            os.getenv('snowflake_account'),
            os.getenv('snowflake_db'),
            os.getenv('snowflake_schema'),
            os.getenv('snowflake_warehouse'))
        )
    
    sql_cmd = \
            "USE ROLE ACCOUNTADMIN;"    \
            "CREATE DATABASE IF NOT EXISTS SPOTIFY;"    \
            "USE DATABASE SPOTIFY;"     \
            "CREATE SCHEMA IF NOT EXISTS RAW;"          \
            "USE SCHEMA RAW;"           \
            "CREATE OR REPLACE STAGE internal_stage;"   \
            "CREATE OR REPLACE FILE FORMAT my_json_format  \
                    TYPE = json;"
    
    conn.execute_string(sql_cmd)
    return engine, conn
    
def verify_internal_stage(conn):
    sql_cmd = """
        List @internal_stage;
    """
    cursor_list = conn.cursor().execute(sql_cmd).fetchall()
    print(cursor_list)

def load_snowflake(engine, conn, data, sql, tb_name):
    conn.cursor().execute(sql)
    data.columns = map(lambda x: str(x).upper(), data.columns)
    data.to_sql(tb_name, engine, index=False, if_exists='replace', method=pd_writer)
    print('Successfully load the table to snowflake.')

def staged_files_load(conn, table_name, column_lst, file_name):
    sql_cmd =  f"PUT file://files/{file_name} @internal_stage;"\
                f"CREATE OR REPLACE TABLE {table_name} \n("+ \
            ', '.join([item+' VARIANT' for item in column_lst])+');'
    conn.execute_string(sql_cmd)
    sql_cmd = f"COPY INTO {table_name} FROM "+\
        f"(SELECT \n"+',\n'.join(['$1:'+item.lower()+'::variant' for item in column_lst])+f'\n FROM @internal_stage/{file_name})\n'+\
        " FILE_FORMAT = (format_name = MY_JSON_FORMAT);"
    print('sql command:', sql_cmd)
    conn.execute_string(sql_cmd)
    print('Successfully load the data from staged files to an existing table.')

def write_sql_file(statement, name):
    directory_path = os.path.join(f"{os.environ['AIRFLOW_HOME']}/dags/dbt/spotify/models/staging", name)
    '''if not os.path.exists(directory_path):
        os.mkdir(directory_path) 
        print("Directory '% s' created" % name) '''

    with open(directory_path+'_flatten.sql', "w") as file1:
        # Writing data to a file
        file1.write(statement)

if __name__ == "__main__":
    print()
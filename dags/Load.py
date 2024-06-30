from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import os
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
import yaml

def load_snoflake_conn():
    with open('profiles.yml', 'r') as file:
        yaml_content = yaml.safe_load(file)

    user=yaml_content['spotify']['outputs']['dev']['user']
    password = yaml_content['spotify']['outputs']['dev']['password']
    account = yaml_content['spotify']['outputs']['dev']['account']
    warehouse = yaml_content['spotify']['outputs']['dev']['warehouse']
    database = yaml_content['spotify']['outputs']['dev']['database']
    schema = yaml_content['spotify']['outputs']['dev']['schema']

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
        )
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    engine = create_engine(
        'snowflake://{0}:{1}@{2}/{3}/{4}?warehouse={5}'.format(
            user,
            password,
            account,
            database,
            schema,
            warehouse)
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

# engine: SQLAlchemy engine
# conn: Snowflake connector
# data: JSON file
def load_snowflake(engine, conn, data, sql, tb_name):
    conn.cursor().execute(sql)
    data.columns = map(lambda x: str(x).upper(), data.columns)
    data.to_sql(tb_name, engine, index=False, if_exists='replace', method=pd_writer)
    print('Successfully load the table to snowflake.')

def json_files_load(conn, table_name, column_lst, file_name):
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
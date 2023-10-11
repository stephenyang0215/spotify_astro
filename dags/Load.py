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
            "PUT file://files/new_releases.json @internal_stage;"
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
    sql_cmd =   "CREATE OR REPLACE FILE FORMAT my_json_format  \
                    TYPE = json;"   \
                f"CREATE OR REPLACE TABLE {table_name} \n("+ \
            ', '.join([item+' VARIANT' for item in column_lst])+');'
    conn.execute_string(sql_cmd)
    sql_cmd = f"COPY INTO {table_name}      \
        FROM @internal_stage/{file_name}        \
        FILE_FORMAT = (format_name = MY_JSON_FORMAT);"
    conn.execute_string(sql_cmd)
    print('Successfully load the data from staged files to an existing table.')

def json_schema_auto(file_path, table):
    parent_node_lst = []
    file = open(file_path)
    data = json.load(file)
    print('json_data: ', data)
    visited = set() # Set to keep track of visited nodes of graph.
    # Driver Code
    print("Following is the Depth-First Search")
    for key in data.keys():
        dfs(visited, data, key, parent_node_lst)#Pick first one key of the json object
    distinct_parent_node = []
    [distinct_parent_node.append(element) for element in parent_node_lst if element not in distinct_parent_node]
    flat_name_set = set()
    statement = []
    for element in list(visited):
        element = element[1:].upper()
        parse_element = element.split('.')[0:]
        name = 'FLAT_'+'_'.join(parse_element[:-1])
        flat_name = 'FLAT_'+'_'.join(parse_element)
        flat_name_set.add(name)
        statement.append(name+'.VALUE:'+parse_element[-1].lower()+f'::string AS {flat_name},')
    statement[-1] = statement[-1][:-1]
    distinct_parent_node[0]
    flat_lst = []
    name = 'FLAT_'+distinct_parent_node[0]
    flat_lst.append(f'LATERAL FLATTEN(input => '+distinct_parent_node[0].upper()+') '+name.upper()+',')
    for idx in range(1, len(distinct_parent_node)):
        split_name = distinct_parent_node[idx].split('.')
        flat_name = '_'.join(split_name[:-1]).upper()
        full_flat_name = '_'.join(split_name).upper()
        flat_lst.append(f'LATERAL FLATTEN(input => '+'FLAT_'+flat_name+'.value:'+distinct_parent_node[idx].split('.')[-1]+') '+'FLAT_'+full_flat_name+',')
    flat_lst[-1]=flat_lst[-1][:-1]
    snowflake_sql_statement = 'SELECT \n'+'\n'.join(statement)+'\nFROM '+"{{source('spotify', '"+table.lower()+"')}}"+',\n'+'\n'.join(flat_lst)
    print('SQL statement for this table: ', snowflake_sql_statement)
    return snowflake_sql_statement

def dfs(visited, graph, node, parent_node_lst, parent_node = ''):   
    if (type(graph) == dict) or (type(graph) == list and type(graph[0]) == dict):
        #visited.add(parent_node+'.'+node)
        if parent_node!='.'+node:
            parent_node = parent_node+'.'+node
        parent_node_lst.append(parent_node[1:])
        if type(graph) == dict:
            for key in graph.keys():
                dfs(visited, graph[key], key, parent_node_lst, parent_node)
        elif type(graph[0])==dict:
            for key in graph[0].keys():
                dfs(visited, graph[0][key], key, parent_node_lst, parent_node)
    else:
        visited.add(parent_node+'.'+node)

def write_sql_file(statement, file_name):
    with open(file_name, "w") as file1:
        # Writing data to a file
        file1.write(statement)

if __name__ == "__main__":
    print()
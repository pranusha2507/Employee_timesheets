import psycopg2
import psycopg2.extras
import io

import pymssql
import pandas as pd
from postgres_conn import psql


def write_to_postgres(df, conn, cur, table_name ):
    output = io.StringIO() # For Python3 use StringIO
    df.to_csv(output, sep='\t', header=True, index=False)
    output.seek(0) # Required for rewinding the String object
    if not db_table_exists(conn, table_name):
        create_stmt = pd.io.sql.get_schema(df, table_name)
        cur.execute(create_stmt)
    copy_query = f"COPY {table_name} FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
    cur.copy_expert(copy_query, output)
    conn.commit()
    
def db_table_exists(conn, tablename):
    sql = f"select * from information_schema.tables where table_name='{tablename}'" 
    
    # return results of sql query from conn as a pandas dataframe
    results_df = pd.read_sql_query(sql, conn)

    # True if we got any results back, False if we didn't
    return bool(len(results_df))


def lambda_handler(event, context):

    # CHANGE tables as per actuals
    TABLES = ["DB1.Table1, DB2.Table2"]

    session = boto3.Session()
    ssmc = session.client('ssm')
    env_prefix = os.environ['stage_prefix']
    mssql_conf = {
        "server": ssmc.get_parameter(Name='/{0}/rds-sqls-hostname'.format(env_prefix), WithDecryption=True)['Parameter']['Value'],
        "user": ssmc.get_parameter(Name='/{0}/rds-sqls-username'.format(env_prefix), WithDecryption=True)['Parameter']['Value'],
        "password": ssmc.get_parameter(Name='/{0}/rds-sqls-password'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
    }
    
    mssql_conn = pymssql.connect(**mssql_conf)  
    
    SOURCE_QUERY = "SELECT * FROM {0}"
    psql.conn.cursor_factory = psycopg2.extras.RealDictCursor
    cur = psql.conn.cursor()
    for table in TABLES:
    
        stmt = SOURCE_QUERY.format(table)
        source_df = pd.read_sql(stmt, mssql_conn)
        write_to_postgres(source_df, psql.conn, cur, table)

    


        
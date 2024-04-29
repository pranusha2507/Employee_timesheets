import psycopg2
import psycopg2.extras
import io

def dbConnect (host_name, database_name, username,pwd):
    # Parse in connection information
    credentials = {'host': host_name, 'database': database_name, 'user': username, 'password': pwd}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = True  # auto-commit each entry to the database
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    cur = conn.cursor()
    print ("Connected Successfully to DB: " + str(database_name) + "@" + str(host_name))
    return conn, cur

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
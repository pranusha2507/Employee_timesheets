import boto3
import json
from datetime import datetime
import time
import calendar
import os
import sqlalchemy
from sqlalchemy import create_engine
import urllib
from io import StringIO
from boto3.s3.transfer import TransferConfig

os.sys.path.append('/opt')
import psycopg2
from postgres_conn import psql
import pandas as pd

env_prefix = 'cel-dm-dev'


def lambda_handler(event, context):
    db_cl = psql()
    cfgs = {}
    session = boto3.Session()
    ssmc = session.client('ssm')
    db_usr = ssmc.get_parameter(Name='/{0}/rds-pg-dw-username'.format(env_prefix), WithDecryption=True)['Parameter'][
        'Value']
    db_ps = ssmc.get_parameter(Name='/{0}/rds-pg-dw-password'.format(env_prefix), WithDecryption=True)['Parameter'][
        'Value']
    pg_info = ssmc.get_parameter(Name='/{0}/rds-pg-dw-endpoint'.format(env_prefix), WithDecryption=True)['Parameter'][
        'Value']
    db_endpoint = pg_info.split(':')[0]
    db_port = pg_info.split(':')[1]
    db_name = 'postgres'
    dest_con = 'postgresql://' + db_usr + ':' + db_ps + '@' + db_endpoint + '/app'
    conn1 = db_cl.conn
    # df = pd.read_sql('SELECT * FROM app.illogical_loan', conn1)
    # db_schema = 'app'

    config = TransferConfig(max_concurrency=3)
    s3_client = boto3.client('s3')
    source_bucket = 'cel-data-dev-datalake-preland'
    # input_file_key = 'CELINK_TBLTRANSACTIONS_20210813101137.txt'
    input_file_key = 'tbltrx_1M.csv'
    csv_obj = s3_client.get_object(Bucket=source_bucket, Key=input_file_key)
    csv_file_body = csv_obj['Body']
    csv_file_string = csv_file_body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_file_string), sep='|', low_memory=False)

    # engine = create_engine('postgresql://postgres:$79~paid~GREEN~hang~SAIL~14$@cel-dm-dev-rds-pg-dw.ca2f63zjrxyg.us'
    #                        '-east-1.rds.amazonaws.com:5432')

    engine = create_engine('postgresql+psycopg2://postgres:$79~paid~GREEN~hang~SAIL~14$@cel-dm-dev-rds-pg-dw'
                           '.ca2f63zjrxyg.us'
                           '-east-1.rds.amazonaws.com:5432/postgres')

    # engine = create_engine(connection_string)
    # engine.execution_options(schema_translate_map={None: db_schema})
    # con = engine.connect()
    # con.execute('SET search_path TO {schema}'.format(schema=db_schema))

    print(df)
    connection = engine.raw_connection()
    df.to_sql('illogical_loan_full_csv', con=engine, schema='lake', if_exists='replace', chunksize=200000)

    print(db_usr)
    print(db_ps)
    print(db_endpoint)
import boto3
import botocore
import os
os.sys.path.append('/opt')

import psycopg2

# #################### Notes ##################################################################
# There are variables here that are configured from env variables
# DB syntax can vary slightly between Postgres and MySql (case sensitivity is different as well)
# 
# env_prefix = 'cel-dm-stg'
# db_endpoint = 'cel-dm-stg-rds-pg-dw.cml1xdm83jsf.us-east-1.rds.amazonaws.com'

# #################### Notes ##################################################################
env_prefix = os.environ['stage_prefix']
# db_endpoint = os.environ['db_endpoint']

session = boto3.Session()
ssmc = session.client('ssm')
db_usr = ssmc.get_parameter(Name='/{0}/rds-pg-dw-username'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
db_ps = ssmc.get_parameter(Name='/{0}/rds-pg-dw-password'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
pg_info = ssmc.get_parameter(Name='/{0}/rds-pg-dw-endpoint'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
db_endpoint = pg_info.split(':')[0]
db_port = pg_info.split(':')[1]
db_name = 'postgres'

class psql():
    def __init__(self):
        self.host = db_endpoint
        self.u_name = db_usr
        self.u_pass = db_ps
        self.db_name = db_name

        self.conn = psycopg2.connect(user = db_usr,
                            password = db_ps,
                            host = db_endpoint,
                            port = db_port,
                            database = db_name)
    def reconnect(self):
        self.conn = psycopg2.connect(user = self.u_name, password = self.u_pass, host = self.host,
                                    port = 5432, database = self.db_name, sslmode='require')

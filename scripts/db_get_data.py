import json
import pandas as pd
import numpy as np
import logging
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from sqlalchemy import text
import time

EngineLogging.configure_console_logging(logging.DEBUG)

'''
Usage of script: to query for data in db2 or postgres
sample query statement are given
example of how to query is given
assumes that user has appropriate credentials for the db they want to query
'''

def query_db(query, dbtype='db2'):

    if dbtype is 'db2':
        with open('./dev_resources/credentials_as_dev.json', encoding='utf-8') as F:
            credentials = json.loads(F.read())
    elif dbtype is 'postgres':
        with open('./dev_resources/credentials_as_postgre.json', encoding='utf-8') as F:
            credentials = json.loads(F.read())

    db = Database(credentials=credentials)

    df = pd.read_sql_query(sql=text(query), con=db.connection)

    csvname = dbtype+'results'+time.strftime("%Y%m%d-%H%M%S")
    df.to_csv('data/'+csvname)

    db.connection.dispose()


#db2 queries
db2_localdb2 = "SELECT AUTHID, APPL_NAME, CLIENT_NNAME, TPMON_CLIENT_WKSTN, AGENT_ID, APPL_ID, APPL_STATUS \
                FROM SYSIBMADM.APPLICATIONS \
                WHERE APPL_ID LIKE '%LOCAL.DB2%'"

db2_allaps = "SELECT AUTHID, APPL_NAME, CLIENT_NNAME, TPMON_CLIENT_WKSTN, AGENT_ID, APPL_ID, APPL_STATUS \
              FROM SYSIBMADM.APPLICATIONS"

#postgres queries
postgres_library = "SELECT * \
                    FROM pg_stat_activity \
                    WHERE datname='ibmclouddb' and application_name like '%LIB%';"

postgres_allapps = "SELECT * \
                    FROM pg_stat_activity \
                    WHERE datname='ibmclouddb'"

#run the selected query on the corresponding db
#supported db type: 'db2', 'postgres'
query_db(db2_allaps, 'db2')
#query_db(postgres_allapps, 'postgres')



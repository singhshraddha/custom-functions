import datetime as dt
import json
import pandas as pd
import numpy as np
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.metadata import Granularity

EngineLogging.configure_console_logging(logging.DEBUG)

'''
You can test functions locally before registering them on the server to
understand how they work.
Supply credentials by pasting them from the usage section into the UI.
Place your credentials in a separate file that you don't check into the repo. 
'''

with open('./dev_resources/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)


def local_func_execute(fn):
    '''
    The local test will generate data instead of using server data.
    By default it will assume that the input data items are numeric.
    Required data items will be inferred from the function inputs.
    The function below executes an expression involving a column called x1
    The local test function will generate data dataframe containing the column x1
    '''
    df = fn.execute_local_test(db=db, db_schema=db_schema, to_csv=False)
    print(df)
    print('-------------------------END OF LOCAL FUNC EXECUTE-------------------------------')

def local_pipeline_execute(fn):

    hourly = Granularity(name='hourly', freq='1H',  # pandas frequency string
                        timestamp='evt_timestamp',  # build time aggregations using this datetime col
                        entity_id='id',  # aggregate by id
                        dimensions=None, entity_name=None)
    fn.granularity = hourly

    entity_name = 'shraddha_boiler_type_1'
    entity_type = db.get_entity_type(name=entity_name)

    #add the local function to test
    entity_type._functions.extend([fn])
    entity_type._granularities_dict={'hourly': hourly}

    entity_type.exec_local_pipeline(**{'_production_mode': False})

    '''
    view entity data
    '''
    # print("Read Table of new entity")
    # df = db.read_table(table_name='dm_'+entity_name, schema=db_schema)
    # print(list(df['KEY'].unique()))
    # print(df.head())

    print('---------------------END OF LOCAL PIPELINE EXECUTE-----------------------')


'''Import and instantiate the functions to be tested '''
from custom.functions import SS_HelloWorldAggregator
fn = SS_HelloWorldAggregator(
        source = ['pressure'],
        expression = '${GROUP}.max()'
        )

from custom.functions import SS_SimpleAggregator
fn = SS_SimpleAggregator(
        input_items = ['pressure'],
        expression = 'x.max()',
        output_items=['out_pressure']
        )

local_pipeline_execute(fn)

import datetime as dt
import json
import pandas as pd
import numpy as np
import logging
from iotfunctions.db import Database
from iotfunctions.metadata import ServerEntityType
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

with open('./dev_resources/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

'''
To do anything with IoT Platform Analytics, you will need one or more entity type.
This example assumes that the entity to which we are adding dimensions already exists
We add the custom function to this entity type to test it locally
'''
entity_name = 'Alert_Purge_test'
entity_type = db.get_entity_type(name=entity_name)

start_ts = '2020-06-30-19.15.00.000000'
end_ts = '2020-06-30-19.35.00.000000'


df = entity_type.get_data(start_ts=start_ts, end_ts=end_ts)

print(df)

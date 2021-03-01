import json
import logging
from iotfunctions.db import Database

#from iotfunctions.enginelog import EngineLogging
#EngineLogging.configure_console_logging(logging.DEBUG)

with open('../dev_resources/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

from custom.functions import SS_ComplexAggregator

try:
    db.unregister_functions(['SS_ComplexAggregator'])
except:
    pass
db.register_functions([SS_ComplexAggregator])

import json
import logging
from iotfunctions.db import Database

#from iotfunctions.enginelog import EngineLogging
#EngineLogging.configure_console_logging(logging.DEBUG)

with open('../dev_resources/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

from custom.data_quality import SS_DataQualityChecks

try:
    db.unregister_functions(['SS_DataQualityChecks'])
except:
    pass
db.register_functions([SS_DataQualityChecks])

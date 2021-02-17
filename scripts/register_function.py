import json
import logging
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)

with open('../dev_resources/credentials_iotls.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

from custom.supervised import BayesRidgeRegressor

db.register_functions([BayesRidgeRegressor])
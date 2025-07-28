import configparser
import os
import signal
from concurrent.futures import ThreadPoolExecutor

import duckdb

from mallard.tiingo.tiingo_util import logger

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']


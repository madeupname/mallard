import io
import logging
import os

import configparser
import datetime
from datetime import datetime, timedelta
import signal

import duckdb
import pandas as pd
import pytz
import requests

# Get config file
config_file = os.getenv("MALLARD_CONFIG")
config = configparser.ConfigParser()
config.read(config_file)

logger = logging.getLogger(__name__)
# Set logging format of created, level, filename, message
logging.basicConfig(filename=config['DEFAULT']['log_file'], level=config['DEFAULT']['log_level'],
                    format=config['DEFAULT']['log_format'])

db_file = config['DEFAULT']['db_file']

msg = f"Updating Tiingo splits data. Using database {db_file}. "
print(msg)
logger.info(msg)

# Global flag to indicate shutdown
shutdown_flag = False


def signal_handler(signal, frame):
    global shutdown_flag
    print('Signal received, shutting down.')
    shutdown_flag = True


if hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, signal_handler)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# splits_dir = config['tiingo']['dir'] + '/splits'
# os.makedirs(splits_dir, exist_ok=True)
splits_table = config['tiingo']['splits_table']


def load_splits(date: datetime) -> bool:
    """Given a date, downloads CSV with split data on that date. Then inserts or replaces that data in the DB. 
    Returns False if the CSV file has no data, True otherwise."""
    url = f"{config['tiingo']['splits_url']}/?exDate={date.strftime('%Y-%m-%d')}&&format=csv&token={config['tiingo']['api_token']}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.content
    if not data or not data.startswith(
            b'permaTicker,ticker,exDate,splitFrom,splitTo,splitFactor'):
        return False
    data_without_header = r.content.split(b'\n', 1)[1]
    if not data_without_header:
        return False
    # Use Pandas to create a DataFrame from the CSV data, but using DB columns.
    df = pd.read_csv(io.BytesIO(data),
                     names=['vendor_symbol_id', 'symbol', 'date', 'split_from', 'split_to', 'split_factor'],
                     skiprows=1)

    with duckdb.connect(db_file) as duckdb_con:
        duckdb_con.execute(f"INSERT OR REPLACE INTO {splits_table} BY NAME FROM df")
    return True


attempts = 0
days_loaded = 0
day = datetime.today()
while days_loaded < 2:
    attempts += 1
    if attempts > 7:
        raise Exception(f"Failed to load splits data. Days loaded: {days_loaded}")
    if load_splits(day):
        days_loaded += 1
    # Increment day by 1
    day += timedelta(days=1)

msg = f"Loaded {days_loaded} days of splits data."
logger.info(msg)
print(msg)
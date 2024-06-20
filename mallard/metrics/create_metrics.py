# Creates/updates daily metrics
import configparser
import os
import subprocess
import sys
from datetime import datetime

import duckdb

from mallard.metrics.daily_metrics import update_macd, avg_daily_trading_value
from mallard.metrics.fundamental_metrics import calculate_ttm, roic, nopat
from mallard.metrics.parallelize import parallelize
from mallard.tiingo.tiingo_util import logger

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']
daily_metrics = config['DEFAULT']['metrics'].split(",")
eod_table = config['tiingo']['eod_table']
fundamentals_reported_table = config['tiingo']['fundamentals_reported_table']

# Run script db.py
# Determine the path to db.py
db_script_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'db.py')

# Run the db.py script
subprocess.check_call([sys.executable, db_script_path])

# Get argument --start
start = None
for i in range(len(sys.argv)):
    if sys.argv[i] == '--start':
        # Make sure there is a value after --start matching the format 'YYYY-MM-DD'
        if i + 1 >= len(sys.argv):
            raise Exception("Missing value after --start")
        # Check if the value after --start is a valid date
        try:
            datetime.strptime(sys.argv[i + 1], '%Y-%m-%d')
        except ValueError:
            raise Exception("Invalid date format after --start, use 'YYYY-MM-DD' format")
        start = sys.argv[i + 1]
        break

if start is None:
    start = config['DEFAULT']['metrics_start'] if config.has_option('DEFAULT', 'metrics_start') else '1900-01-01'


def update_metrics():
    """Updates all metrics specified in the config file.
    Uses a ThreadPoolExecutor to run them in parallel if more than 1 worker specified."""
    workers = int(config['DEFAULT']['threads'])
    os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
    msg = f"Updating metrics: {daily_metrics}"
    print(msg)
    logger.info(msg)
    # Get symbols to compile metrics on using EOD table.
    with duckdb.connect(db_file) as con:
        symbols_query = f"""
        SELECT DISTINCT vendor_symbol_id, symbol
        FROM tiingo_eod"""
        symbol_ids = con.sql(symbols_query)
        eod_symbols = symbol_ids.fetchall()

    vendor_symbol_ids = [row[0] for row in eod_symbols]
    duckdb_con = duckdb.connect(db_file)
    if 'adtval' in daily_metrics:
        msg = "Calculating avg_daily_trading_value..."
        print(msg)
        logger.info(msg)
        avg_daily_trading_value(duckdb_con)
        msg = "Done calculating avg_daily_trading_value"
        print(msg)
        logger.info(msg)
    if 'macd' in daily_metrics:
        msg = "Calculating MACD..."
        print(msg)
        logger.info(msg)
        macd_results = parallelize(workers, vendor_symbol_ids, update_macd, duckdb_con, start_date=start)
        msg = f"MACD: Skipped {macd_results['count_skip']}  Updated {macd_results['count_success']}  Failed {macd_results['count_fail']}"
        print(msg)
        logger.info(msg)
    if 'roic' in daily_metrics:
        msg = "Calculating ROIC (roic) with dependencies EBIT TTM, tax expense TTM, and NOPAT."
        print(msg)
        logger.info(msg)
        msg = "Calculating EBIT TTM."
        print(msg)
        logger.info(msg)
        calculate_ttm("ebit", duckdb_con)
        msg = "Calculating tax expense TTM."
        print(msg)
        logger.info(msg)
        calculate_ttm("taxExp", duckdb_con)
        msg = "Calculating NOPAT."
        print(msg)
        logger.info(msg)
        nopat(duckdb_con)
        msg = "Calculating ROIC."
        print(msg)
        logger.info(msg)
        roic(duckdb_con)
        msg = f"ROIC backtesting metric complete"
        print(msg)
        logger.info(msg)


update_metrics()

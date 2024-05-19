# Creates/updates daily metrics
import concurrent
import configparser
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import duckdb

from mallard.metrics.daily_metrics import update_macd, avg_daily_trading_value
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


def parallelize(workers, vendor_symbol_ids, fun, *args, **kwargs):
    """Run the given function in parallel for each vendor_symbol_id."""
    print(f"Running {fun.__name__} in parallel with {workers} workers for {len(vendor_symbol_ids)} symbols.")
    results = {'count_skip': 0, 'count_success': 0, 'count_fail': 0}
    if workers == 1:
        for id in vendor_symbol_ids:
            status = fun(id, *args, **kwargs)
            if status == 'skip':
                results['count_skip'] += 1
            elif status == 'success':
                results['count_success'] += 1
            elif status == 'fail':
                results['count_fail'] += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit the tasks to the thread pool
            futures = {executor.submit(fun, id, *args, **kwargs): id for id in vendor_symbol_ids}
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                try:
                    status = future.result()
                    if status == 'skip':
                        results['count_skip'] += 1
                    elif status == 'success':
                        results['count_success'] += 1
                    elif status == 'fail':
                        results['count_fail'] += 1
                except Exception as e:
                    print(f"Error processing {row}\n{e}")
                    results['count_fail'] += 1
                total = results['count_skip'] + results['count_success'] + results['count_fail']
                if total > 0 and total % 500 == 0:
                    print(
                        f"{fun.__name__}: Skipped {results['count_skip']}  Updated {results['count_success']}  Failed {results['count_fail']}")
    return results


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
        eod_symbols = parallelize(workers, vendor_symbol_ids, update_macd, duckdb_con, start_date=start)
        msg = f"MACD: Skipped {eod_symbols['count_skip']}  Updated {eod_symbols['count_success']}  Failed {eod_symbols['count_fail']}"
        print(msg)
        logger.info(msg)


update_metrics()

"""
This script downloads the Tiingo-generated daily metrics (P/E, etc.) into tiingo_fundamentals_daily table.
Must be run after fundamentals.py, which loads fundamentals meta file.
"""

import concurrent
import configparser
import datetime
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from io import StringIO

import duckdb
import polars as pl
import requests

from mallard.RateLimiterContext import RateLimiterContext
from mallard.normalization import get_column_mapper
from mallard.tiingo.tiingo_util import logger, quarantine_data

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
config = configparser.ConfigParser()
config.read(config_file)

if not config.getboolean('tiingo', 'has_fundamentals_addon'):
    raise ValueError(
        "Tiingo fundamentals addon required. If you have paid for this, update your config file. Exiting...")

db_file = config['DEFAULT']['db_file']

# Global flag to indicate shutdown
shutdown_flag = False


def signal_handler(signal, frame):
    global shutdown_flag
    print('Signal received, shutting down.')
    shutdown_flag = True


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGBREAK, signal_handler)

fundamentals_daily_dir = os.path.join(config['tiingo']['dir'], 'fundamentals/daily')
os.makedirs(fundamentals_daily_dir, exist_ok=True)
exclude_dir = config['tiingo']['dir'] + '/exclude'
os.makedirs(exclude_dir, exist_ok=True)
quarantine_dir = config['tiingo']['dir'] + '/quarantine'
os.makedirs(quarantine_dir, exist_ok=True)
fundamentals_daily_table = config['tiingo']['fundamentals_daily_table']
base_url = config['tiingo']['fundamentals_url']
count_skip = 0
count_new = 0
count_update = 0
count_fail = 0

# Use the fundamentals meta table with the fundamentals daily table to get symbols with their most recent update date.
# Also uses symbols table to capture filters used on that data, like minimum lifespan.
with duckdb.connect(db_file, read_only=True) as con:
    result = con.execute(f"""
            SELECT f.symbol, e.max_date, f.vendor_symbol_id, f.is_active, f.daily_last_updated
            FROM {config['tiingo']['fundamentals_meta_table']} f
            LEFT JOIN (
                SELECT vendor_symbol_id, MAX(date) AS max_date
                FROM {fundamentals_daily_table}
                GROUP BY vendor_symbol_id
            ) e ON f.vendor_symbol_id = e.vendor_symbol_id
            WHERE f.symbol in (SELECT DISTINCT symbol FROM tiingo_symbols)
            ORDER BY f.symbol""").fetchall()


def update_symbol(symbol, start_date, duckdb_con, vendor_symbol_id=None, is_active=None):
    """Update the daily fundamental metrics of the given symbol, dates, and existing history.
    Updates both the file and the database."""
    if shutdown_flag:
        return 'skip'

    file_type = 'fundamentals_daily'
    try:
        # We append an identifier to the file name to differentiate it in exclude and quarantine directories.
        # It also allows you to open different data files for the same symbol in Excel at once.
        file_name = f"{symbol}_{vendor_symbol_id}_fundamentals_daily.csv"
        # Check if the file is in the exclude or quarantine directories first
        if (os.path.exists(os.path.join(exclude_dir, file_name)) or
                os.path.exists(os.path.join(quarantine_dir, file_name))):
            return 'skip'
        file = os.path.join(fundamentals_daily_dir, file_name)
        is_append = False
        if os.path.exists(file):
            is_append = True
        # Specify the column order as it is not guaranteed for this endpoint and changes will require a DB table update.
        # Even though date is a column, it will fail if you specify it. It will be the first column, though.
        url = f"{base_url}/{vendor_symbol_id}/daily?startDate={start_date}&format=csv&columns=marketCap,enterpriseVal,peRatio,pbRatio,trailingPEG1Y&token={config['tiingo']['api_token']}"
        with RateLimiterContext():
            r = requests.get(url)
        r.raise_for_status()
        data = r.content
        # Tiingo will literally return the word "None" in some cases, usually when there is no data after a date because
        # the symbol is inactive. It may also be blank or [] - it's not consistent across APIs.
        if not data or not data.startswith(
                b'date,marketCap,enterpriseVal,peRatio,pbRatio,trailingPEG1Y'):
            quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
            return 'fail'
        data_without_header = data.split(b'\n', 1)[1]
        if not data_without_header:
            if not is_append:
                quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
                return 'fail'
            # If we're updating, there's a good chance the start date falls on a weekend and there legitimately
            # isn't any data to fetch.
            return 'skip'
        try:
            # Create a DataFrame from the CSV data and rename the columns to match the EOD table
            df = pl.read_csv(StringIO(data.decode('utf-8')))
            # Rename columns to match the EOD table
            col = get_column_mapper('tiingo')
            df = df.rename({col('market_cap'): 'market_cap', col('enterprise_val'): 'enterprise_val',
                            col('pe_ratio'): 'pe_ratio', col('pb_ratio'): 'pb_ratio',
                            col('trailing_peg_1y'): 'trailing_peg_1y'})
            # Add symbol columns
            df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])
            # Insert the DataFrame into the EOD table
            with duckdb_con.cursor() as local_con:
                local_con.execute(f"INSERT INTO {config['tiingo']['fundamentals_daily_table']} BY NAME FROM df")
        except duckdb.ConnectionException as e:
            msg = f"Can't connect to DB, exiting. Error:\n{e}"
            print(msg)
            logger.error(msg)
            exit(1)
        except Exception as e:
            logger.error(f"Error inserting {symbol} into EOD table\n{e}")
            quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
            return 'fail'
        if is_append:
            mode = 'ab'
            file_data = data_without_header
        else:
            mode = 'wb'
            file_data = data
        with open(file, mode) as f:
            f.write(file_data)
        if is_append:
            return 'update'
        else:
            return 'new'
    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error when getting {row}\nHalting as the Tiingo service could be down or there is a problem with your account\n{e}")
        return 'fail'
    except Exception as e:
        logger.error(f"Error getting {row}\n{e}")
        return 'fail'


# Create a ThreadPoolExecutor
workers = int(config['DEFAULT']['threads'])
os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
duckdb_con = duckdb.connect(db_file)

# Print the number of symbols to process
msg = f"Starting daily fundamentals download. Processing {len(result)} symbols."
logger.info(msg)
print(msg)

if workers == 1:
    for row in result:
        symbol = row[0]
        db_end_date = row[1]
        vendor_symbol_id = row[2]
        is_active = row[3]
        daily_last_updated: datetime.datetime = row[4]
        start_date: datetime.date = db_end_date + timedelta(days=1) if db_end_date else '1900-01-01'
        if db_end_date and daily_last_updated and start_date > daily_last_updated.date():
            count_skip += 1
            continue
        update_symbol(symbol, start_date, duckdb_con, vendor_symbol_id, is_active)
else:
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks to the thread pool
        futures = {}
        for row in result:
            symbol = row[0]
            db_end_date = row[1]
            vendor_symbol_id = row[2]
            is_active = row[3]
            daily_last_updated: datetime.datetime = row[4]
            start_date: datetime.date = db_end_date + timedelta(days=1) if db_end_date else '1900-01-01'
            try:
                if db_end_date and daily_last_updated and start_date > daily_last_updated.date():
                    count_skip += 1
                    continue
            except Exception as e:
                print(f"Error processing {row}\n{e}")
                exit(1)
            futures[executor.submit(update_symbol, symbol, start_date, duckdb_con, vendor_symbol_id, is_active)] = row

        for future in concurrent.futures.as_completed(futures):
            row = futures[future]
            try:
                status = future.result()
                if status == 'skip':
                    count_skip += 1
                elif status == 'new':
                    count_new += 1
                elif status == 'update':
                    count_update += 1
                elif status == 'fail':
                    count_fail += 1
            except Exception as e:
                logger.error(f"Error processing {row}\n{e}")
                count_fail += 1
            total = count_skip + count_new + count_update + count_fail
            if total > 0 and total % 500 == 0:
                print(
                    f"Update: {total} processed | Skipped {count_skip} | Downloaded {count_new} | Updated {count_update} | Failed {count_fail}")

duckdb_con.close()
msg = f"Fundamentals daily processing complete: Skipped {count_skip} | Downloaded {count_new} | Updated {count_update} | Failed {count_fail}"
logger.info(msg)
print(msg)

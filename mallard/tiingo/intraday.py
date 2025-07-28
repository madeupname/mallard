# Downloads minute bars for backtesting. Does not keep them up to date.

import concurrent
import configparser
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime, date
from io import StringIO
from pathlib import Path
import polars as pl

import duckdb
import requests

from mallard.RateLimiterContext import RateLimiterContext
from mallard.normalization import get_column_mapper
from mallard.tiingo.tiingo_util import quarantine_file, quarantine_data

# Get config file
config_file = os.getenv("MALLARD_CONFIG")
config = configparser.ConfigParser()
config.read(config_file)

logger = logging.getLogger(__name__)
# Set logging format of created, level, filename, message
logging.basicConfig(filename=config['DEFAULT']['log_file'], level=config['DEFAULT']['log_level'],
                    format=config['DEFAULT']['log_format'])

db_file = config['DEFAULT']['db_file']
file_type = 'intraday'

msg = f"Updating Tiingo intraday data. Using database {db_file}. "
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

# Download/update files
intraday_dir = config['tiingo']['dir'] + '/intraday'
os.makedirs(intraday_dir, exist_ok=True)
exclude_dir = config['tiingo']['dir'] + '/exclude'
os.makedirs(exclude_dir, exist_ok=True)
quarantine_dir = config['tiingo']['dir'] + '/quarantine'
os.makedirs(quarantine_dir, exist_ok=True)
base_url = config['tiingo']['intraday_url']
count_skip = 0
count_new = 0
count_update = 0
count_fail = 0

# Use the fundamentals meta file to ensure you're getting symbols that are reporting financials.
with duckdb.connect(db_file, read_only=True) as con:
    result = con.execute(f"""
        SELECT symbol, vendor_symbol_id, is_active
        FROM tiingo_fundamentals_meta
        WHERE is_active = TRUE
    """).fetchall()
    msg = "Using Tiingo fundamentals meta file as the source of symbols. Fetching all active stocks."


def add_required_symbols(result: list, symbols: list = []):
    """Takes list symbols and adds those rows to result if they are not already there."""

    if symbols:
        # If there are duplicate symbols, remove them
        symbols = list(set(symbols))
        # Remove symbols that are already in result
        for symbol in symbols:
            for row in result:
                if row[0] == symbol:
                    symbols.remove(symbol)
                    break
        if symbols:
            msg = f"Adding required symbols: {symbols}"
            print(msg)
            logger.info(msg)

            # Add them to result with vendor_symbol_id = symbol
            for symbol in symbols:
                result.append((symbol, None, symbol, True))


required_symbols = config['tiingo'].get('symbols', '').split(',')
# Add required symbols to result
add_required_symbols(result, required_symbols)

msg += f"Processing {len(result)} symbols as required. "
print(msg)
logger.info(msg)


def update_symbol(symbol, start_date, end_date, duckdb_con, vendor_symbol_id=None, is_active=None):
    """Load the price history of the given symbol starting at start_date.
    Writes both the file and the database. Does NOT handle updates."""
    if shutdown_flag:
        return 'skip'
    try:
        # We append an identifier to the file name to differentiate it.
        # It also allows you to open different data files for the same symbol in Excel at once.
        file_name = f"{symbol}_{vendor_symbol_id}_minute.csv"
        file = os.path.join(intraday_dir, file_name)
        url = f"{base_url}/{symbol}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=1min&format=csv&token={config['tiingo']['api_token']}"
        with RateLimiterContext():
            r = requests.get(url)
        r.raise_for_status()
        data = r.content
        if not data or not data.startswith(
                b'date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor'):
            logger.info(f"No data for {symbol} (active: {is_active}) at {start_date}")
            return 'fail'
        data_without_header = r.content.split(b'\n', 1)[1]
        if not data_without_header:
            logger.info(f"No data for {symbol} (active: {is_active}) at {start_date}")
            return 'fail'
        try:
            # Create a DataFrame from the CSV data and rename the columns to match the EOD table
            df = pl.read_csv(StringIO(data.decode('utf-8')))
            # Rename columns to match the EOD table
            col = get_column_mapper('tiingo')
            df = df.rename({col('adj_close'): 'adj_close', col('adj_high'): 'adj_high', col('adj_low'): 'adj_low',
                            col('adj_open'): 'adj_open', col('adj_volume'): 'adj_volume', col('div_cash'): 'div_cash',
                            col('split_factor'): 'split_factor'})
            # Add symbols columns
            df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])
            # Insert the DataFrame into the EOD table
            with duckdb_con.cursor() as local_con:
                local_con.execute(f"INSERT OR REPLACE INTO {config['tiingo']['intraday']} BY NAME FROM df")
        except duckdb.ConnectionException as e:
            print(f"Can't connect to DB, exiting. Error:\n{e}")
            exit(1)
        except Exception as e:
            print(f"Error inserting {symbol} into EOD table\n{e}")
            quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
            return 'fail'
        mode = 'wb'
        file_data = data
        with open(file, mode) as f:
            f.write(file_data)
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

# If workers is 1, use a for loop to process the symbols. This is useful for debugging.
if workers == 1:
    for row in result:
        symbol = row[0]
        db_end_date = row[1]
        if db_end_date and isinstance(db_end_date, date) and db_end_date >= datetime.today().date():
            count_skip += 1
            continue
        vendor_symbol_id = None
        is_active = None
        if len(row) == 4:
            vendor_symbol_id = row[2]
            is_active = row[3]
        start_date = db_end_date + timedelta(days=1) if db_end_date else '1900-01-01'
        status = update_symbol(symbol, start_date, duckdb_con, vendor_symbol_id, is_active)
        if status == 'skip':
            count_skip += 1
        elif status == 'new':
            count_new += 1
        elif status == 'update':
            count_update += 1
        elif status == 'fail':
            count_fail += 1
        # Print status every 500 symbols
        total = count_skip + count_new + count_update + count_fail
        if total > 0 and total % 500 == 0:
            print(
                f"Update: {total} processed | Skipped {count_skip} | New {count_new} | Updated {count_update} | Failed {count_fail}")
            if count_fail == total:
                msg = "Everything failed. Check API key and config.ini for issues. Exiting."
                print(msg)
                logger.error(msg)
                exit(1)
else:
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks to the thread pool
        futures = {}
        for row in result:
            symbol = row[0]
            db_end_date = row[1]
            if db_end_date and isinstance(db_end_date, date) and db_end_date >= datetime.today().date():
                count_skip += 1
                continue
            vendor_symbol_id = None
            is_active = None
            if len(row) == 4:
                vendor_symbol_id = row[2]
                is_active = row[3]
            start_date = db_end_date + timedelta(days=1) if db_end_date else '1900-01-01'
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
            # Print status every 500 symbols
            total = count_skip + count_new + count_update + count_fail
            if total > 0 and total % 500 == 0:
                print(
                    f"Update: {total} processed | Skipped {count_skip} | New {count_new} | Updated {count_update} | Failed {count_fail}")
                if count_fail == total:
                    msg = "Everything failed. Check API key and config.ini for issues. Exiting."
                    print(msg)
                    logger.error(msg)
                    exit(1)

# if config.getboolean('tiingo', 'delete_infinity'):
#     delete_infinity(duckdb_con)

duckdb_con.close()
msg = f"EOD update complete: Skipped {count_skip} | New {count_new} | Updated {count_update} | Failed {count_fail}"
print(msg)
logger.info(msg)



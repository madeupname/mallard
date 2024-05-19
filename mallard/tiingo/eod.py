# Downloads end of day price history files, including necessary updates per supported ticker file start/end dates.
import concurrent
import configparser
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime, date
from io import StringIO
from pathlib import Path

import duckdb
import polars as pl
import requests

from mallard.RateLimiterContext import RateLimiterContext
from mallard.normalization import get_column_mapper
from mallard.tiingo.tiingo_util import quarantine_file, quarantine_data

# Get config file
config_file = os.getenv("MALLARD_CONFIG")
config = configparser.ConfigParser()
config.read(config_file)

logger = logging.getLogger(__name__)
# Format with created, level, filename, message
logging.basicConfig(filename=config['DEFAULT']['log_file'], level=config['DEFAULT']['log_level'],
                    format=config['DEFAULT']['log_format'])

db_file = config['DEFAULT']['db_file']

msg = f"Updating Tiingo EOD data. Using database {db_file}. "
print(msg)
logger.info(msg)


def is_valid_eod_file(sym: str, file_path: str, duckdb_con):
    """Check the file for problems"""
    contents = Path(file_path).read_text()
    with duckdb_con.cursor() as local_con:
        is_active = local_con.execute(f"""SELECT COUNT(symbol) FROM {config['tiingo']['fundamentals_meta_table']} 
        WHERE symbol = '{sym}' AND is_active = true""").fetchone()[0]
    # See if the file contains only [], which Tiingo sometimes returns instead of data.
    error = ""
    # Check for empty or inf (infinite) values
    if not contents or contents.startswith('[]'):
        error = "File empty. "
    elif not contents.startswith(
            'date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor'):
        error = "File does not contain expected headers. "
    elif "inf" in contents:
        error = "File contains infinite values. "
    else:
        try:
            with duckdb_con.cursor() as local_con:
                date = local_con.execute(f"SELECT MAX(date) FROM '{file_path}'").fetchall()[0][0]
            if not date:
                error = "No date in file. "
        except:
            error = "No date in file. "
    if error:
        if is_active:
            # Move the file to quarantine_dir
            os.rename(file_path, os.path.join(quarantine_dir, os.path.basename(file_path)))
            logger.error(f"{sym}: {error} Quarantined active symbol.")
        else:
            # Move the file to exclude_dir
            os.rename(file_path, os.path.join(exclude_dir, os.path.basename(file_path)))
            logger.info(f"{sym}: {error} Excluded inactive symbol.")
        return False
    return True


# Download/update files
daily_dir = config['tiingo']['dir'] + '/daily'
os.makedirs(daily_dir, exist_ok=True)
exclude_dir = config['tiingo']['dir'] + '/exclude'
os.makedirs(exclude_dir, exist_ok=True)
quarantine_dir = config['tiingo']['dir'] + '/quarantine'
os.makedirs(quarantine_dir, exist_ok=True)
base_url = config['tiingo']['daily_prices_url']
count_skip = 0
count_new = 0
count_update = 0
count_fail = 0

# If tickers is the source, use the supported tickers file. Not advised.
if config['tiingo']['eod_source'] == 'tickers':
    with duckdb.connect(db_file, read_only=True) as con:
        result = con.execute(f"""
        SELECT s.symbol, e.max_date
        FROM {config['tiingo']['symbols_table']} s
        LEFT JOIN (
            SELECT symbol, MAX(date) AS max_date
            FROM {config['tiingo']['eod_table']}
            GROUP BY symbol
        ) e ON s.symbol = e.symbol
        WHERE s.end_date > e.max_date""").fetchall()
    msg = "Using Tiingo supported tickers file as the source of symbols. "
else:
    # Use the fundamentals meta file to ensure you're getting symbols that are reporting financials.
    with duckdb.connect(db_file, read_only=True) as con:
        result = con.execute(f"""
                SELECT f.symbol, e.max_date, f.vendor_symbol_id, f.is_active
                FROM {config['tiingo']['fundamentals_meta_table']} f
                LEFT JOIN (
                    SELECT vendor_symbol_id, MAX(date) AS max_date
                    FROM {config['tiingo']['eod_table']}
                    GROUP BY vendor_symbol_id
                ) e ON f.vendor_symbol_id = e.vendor_symbol_id
                WHERE f.symbol in (SELECT DISTINCT symbol FROM {config['tiingo']['symbols_table']})
                ORDER BY f.symbol""").fetchall()
        msg = "Using Tiingo fundamentals meta file as the source of symbols. "

msg += f"Processing {len(result)} symbols as required. "
print(msg)
logger.info(msg)

def load_from_file(file_name, duckdb_con):
    """This is a utility function you can use if you already have downloaded price files."""
    # file_name format is `{symbol}_{vendor_symbol_id}_daily.csv`. Split to retrieve those variables.
    symbol, vendor_symbol_id, _ = file_name.split('_')
    file = os.path.join(daily_dir, file_name)
    # Get data from the file
    with open(file, 'rb') as f:
        data = f.read()
    try:
        # Create a DataFrame from the CSV data and rename the columns to match the EOD table
        df = pl.read_csv(StringIO(data.decode('utf-8')))
        # Rename columns to match the EOD table
        col = get_column_mapper('tiingo')
        df = df.rename({col('adj_close'): 'adj_close', col('adj_high'): 'adj_high', col('adj_low'): 'adj_low',
                        col('adj_open'): 'adj_open', col('adj_volume'): 'adj_volume', col('div_cash'): 'div_cash',
                        col('split_factor'): 'split_factor'})
        # Add symbol column
        df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])
        # Insert the DataFrame into the EOD table
        with duckdb_con.cursor() as local_con:
            # Delete existing rows for this vendor_symbol_id
            # local_con.execute(f"DELETE FROM {config['tiingo']['eod_table']} WHERE vendor_symbol_id = '{vendor_symbol_id}'")
            local_con.execute(f"INSERT INTO {config['tiingo']['eod_table']} BY NAME FROM df")
        return 'success'
    except duckdb.ConnectionException as e:
        print(f"Can't connect to DB, exiting. Error:\n{e}")
        exit(1)
    except Exception as e:
        print(f"Error inserting {symbol} into EOD table\n{e}")
        quarantine_file(symbol, file_name)
        return 'fail'


def update_symbol(symbol, start_date, duckdb_con, vendor_symbol_id=None, is_active=None):
    """Update the price history of the given symbol, dates, and existing history.
    Updates both the file and the database."""
    try:
        # We append an identifier to the file name to differentiate it in exclude and quarantine directories.
        # It also allows you to open different data files for the same symbol in Excel at once.
        file_name = f"{symbol}_{vendor_symbol_id}_daily.csv"
        # Check if the file is in the exclude or quarantine directories first
        if (os.path.exists(os.path.join(exclude_dir, file_name)) or
                os.path.exists(os.path.join(quarantine_dir, file_name))):
            return 'skip'
        file = os.path.join(daily_dir, file_name)
        is_append = False
        if os.path.exists(file):
            is_append = True
        url = f"{base_url}/{vendor_symbol_id}/prices?startDate={start_date}&format=csv&token={config['tiingo']['api_token']}"
        with RateLimiterContext():
            r = requests.get(url)
        r.raise_for_status()
        data = r.content
        if r.content is None or r.content.startswith(b'[]'):
            quarantine_data(symbol, vendor_symbol_id, is_active, data)
            return 'fail'
        data_without_header = r.content.split(b'\n', 1)[1]
        if not data_without_header or not data.startswith(
                b'date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor'):
            if not is_append:
                quarantine_data(symbol, vendor_symbol_id, is_active, data)
                return 'fail'
            # If we're updating, there's a good chance the start date falls on a weekend and there legitimately
            # isn't any data to fetch.
            return 'skip'
        try:
            # Create a DataFrame from the CSV data and rename the columns to match the EOD table
            df = pl.read_csv(StringIO(data.decode('utf-8')))
            # If any values in divCash aren't 0, or any values in splitFactor aren't 1, we need to refetch the entire
            # file.
            if is_append:
                divCash_non_zero = (df['divCash'] != 0).sum() > 0
                splitFactor_not_one = (df['splitFactor'] != 1).sum() > 0
                if (divCash_non_zero or splitFactor_not_one):
                    # Delete the file
                    os.remove(file)
                    logger.info(
                        f"{symbol}: Dividend or split detected. Refetching full history for {symbol} / {vendor_symbol_id}.")
                    return update_symbol(symbol, '1900-01-01', duckdb_con, vendor_symbol_id, is_active)

            # Rename columns to match the EOD table
            col = get_column_mapper('tiingo')
            df = df.rename({col('adj_close'): 'adj_close', col('adj_high'): 'adj_high', col('adj_low'): 'adj_low',
                            col('adj_open'): 'adj_open', col('adj_volume'): 'adj_volume', col('div_cash'): 'div_cash',
                            col('split_factor'): 'split_factor'})
            # Add symbol column
            df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])
            # Insert the DataFrame into the EOD table
            with duckdb_con.cursor() as local_con:
                local_con.execute(f"INSERT OR REPLACE INTO {config['tiingo']['eod_table']} BY NAME FROM df")
        except duckdb.ConnectionException as e:
            print(f"Can't connect to DB, exiting. Error:\n{e}")
            exit(1)
        except Exception as e:
            print(f"Error inserting {symbol} into EOD table\n{e}")
            quarantine_data(symbol, vendor_symbol_id, is_active, data)
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
            print(f"Update: {total} processed | Skipped {count_skip} | Downloaded {count_new} | Updated {count_update} | Failed {count_fail}")
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
                print(f"Update: {total} processed | Skipped {count_skip} | Downloaded {count_new} | Updated {count_update} | Failed {count_fail}")


duckdb_con.close()
logger.info(f"EOD update complete: Skipped {count_skip} | Downloaded {count_new} | Updated {count_update} | Failed {count_fail}")

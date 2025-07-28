"""
This script downloads the Tiingo-generated daily metrics (P/E, etc.) into tiingo_fundamentals_daily table.
Must be run after fundamentals.py, which loads fundamentals meta file.
"""

import concurrent
import configparser
import datetime
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from io import StringIO
import pytz

import duckdb
import polars as pl
import requests
import exchange_calendars as xcals

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

# Eastern timezone for cutoff time
ET = pytz.timezone('US/Eastern')


def signal_handler(signal, frame):
    global shutdown_flag
    print('Signal received, shutting down.')
    shutdown_flag = True


def get_next_trading_date_to_download(max_date_in_db):
    """
    Get the next trading date that we should download data for.
    Uses NYSE calendar since data is finalized by 5PM ET for all exchanges.
    
    Args:
        max_date_in_db: Latest date we have in the database (can be None)
    
    Returns:
        datetime.date or None: Next trading date to download, or None if no data should be downloaded yet
    """
    cal = xcals.get_calendar('XNYS')  # NYSE calendar for all exchanges

    # Get current time in ET
    now_et = datetime.datetime.now(ET)
    today = now_et.date()

    # Get the calendar's valid date range
    cal_start = cal.first_session.date()
    cal_end = cal.last_session.date()

    # Determine the next date we should try to download
    if max_date_in_db is None:
        # No data in DB, start from a reasonable date (e.g., 30 days ago)
        candidate_date = today - timedelta(days=30)
    else:
        # Start from the day after our last data
        candidate_date = max_date_in_db + timedelta(days=1)

    # If candidate_date is before the calendar's start date, we can't use the calendar
    # In this case, just return the candidate_date if it's a reasonable trading day
    if candidate_date < cal_start:
        # For dates before the calendar, we'll assume Mon-Fri are trading days
        # and skip obvious holidays, but this is a fallback for old data
        if candidate_date > today:
            return None

        # Simple weekday check for pre-calendar dates
        while candidate_date.weekday() >= 5:  # Skip weekends
            candidate_date += timedelta(days=1)
            if candidate_date >= cal_start:
                break

        if candidate_date >= cal_start:
            # We've reached the calendar range, use the calendar logic below
            pass
        else:
            # Still before calendar range, do time check and return
            if candidate_date == today:
                cutoff_time = ET.localize(datetime.datetime.combine(candidate_date, datetime.time(17, 0)))
                if now_et < cutoff_time:
                    return None
            elif candidate_date > today:
                return None
            return candidate_date

    # Use calendar for dates within its range
    end_date = min(today + timedelta(days=5), cal_end)

    try:
        trading_days = cal.sessions_in_range(max(candidate_date, cal_start), end_date)
    except Exception as e:
        logger.warning(f"Error getting trading days from calendar: {e}")
        # Fallback to simple weekday logic
        if candidate_date > today:
            return None
        while candidate_date.weekday() >= 5:  # Skip weekends
            candidate_date += timedelta(days=1)
        if candidate_date == today:
            cutoff_time = ET.localize(datetime.datetime.combine(candidate_date, datetime.time(17, 0)))
            if now_et < cutoff_time:
                return None
        return candidate_date if candidate_date <= today else None

    if len(trading_days) == 0:
        return None

    # Find the first trading day we don't have data for
    next_trading_date = None
    for session in trading_days:
        session_date = session.date()

        # If this date is after our max date in DB (or we have no data), this is our candidate
        if max_date_in_db is None or session_date > max_date_in_db:
            next_trading_date = session_date
            break

    if next_trading_date is None:
        return None

    # Check if we should download data for this date
    # Data is available after 5 PM ET on the trading day
    cutoff_time = ET.localize(datetime.datetime.combine(next_trading_date, datetime.time(17, 0)))

    # If the next trading date is today, check if it's after 5 PM ET
    if next_trading_date == today:
        if now_et < cutoff_time:
            return None

    # If the next trading date is in the future, skip it
    if next_trading_date > today:
        return None

    return next_trading_date


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
if hasattr(signal, 'SIGBREAK'):
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

# Add columns to fundamentals_meta_table if they don't exist
with duckdb.connect(db_file) as con:
    # Check if columns already exist
    columns_info = con.execute(f"PRAGMA table_info({config['tiingo']['fundamentals_meta_table']})").fetchdf()
    existing_columns = columns_info['name'].tolist()

    # Add columns if they don't exist
    if 'exchange' not in existing_columns:
        con.execute(f"ALTER TABLE {config['tiingo']['fundamentals_meta_table']} ADD COLUMN exchange VARCHAR")
    if 'asset_type' not in existing_columns:
        con.execute(f"ALTER TABLE {config['tiingo']['fundamentals_meta_table']} ADD COLUMN asset_type VARCHAR")
    if 'price_currency' not in existing_columns:
        con.execute(f"ALTER TABLE {config['tiingo']['fundamentals_meta_table']} ADD COLUMN price_currency VARCHAR")
    if 'start_date' not in existing_columns:
        con.execute(f"ALTER TABLE {config['tiingo']['fundamentals_meta_table']} ADD COLUMN start_date DATE")
    if 'end_date' not in existing_columns:
        con.execute(f"ALTER TABLE {config['tiingo']['fundamentals_meta_table']} ADD COLUMN end_date DATE")

    # First, reset all the columns to NULL to ensure idempotency
    print("Resetting exchange, asset type, price currency, and date range columns to NULL...")
    con.execute(f"""
    UPDATE {config['tiingo']['fundamentals_meta_table']}
    SET 
        exchange = NULL,
        asset_type = NULL,
        price_currency = NULL,
        start_date = NULL,
        end_date = NULL
    """)

    # Update fundamentals_meta_table with data from symbols_table based on date ranges
    print("Updating fundamentals meta table with exchange, asset type, price currency, and date ranges...")

    # For each symbol in fundamentals_meta_table, find the most recent date in fundamentals_reported_table
    # and match it with the appropriate date range in symbols_table
    con.execute(f"""
    UPDATE {config['tiingo']['fundamentals_meta_table']} AS meta
    SET 
        exchange = s.exchange,
        asset_type = s.asset_type,
        price_currency = s.price_currency,
        start_date = s.start_date,
        end_date = s.end_date
    FROM 
        {config['tiingo']['symbols_table']} AS s,
        (
            SELECT 
                fr.symbol,
                MAX(fr.date) AS latest_date
            FROM 
                {config['tiingo']['fundamentals_reported_table']} AS fr
            GROUP BY 
                fr.symbol
        ) AS latest
    WHERE 
        UPPER(meta.symbol) = s.symbol
        AND UPPER(meta.symbol) = latest.symbol
        AND latest.latest_date BETWEEN s.start_date AND s.end_date
    """)

    # Count how many rows were updated
    updated_count = con.execute(f"""
    SELECT COUNT(*) FROM {config['tiingo']['fundamentals_meta_table']} 
    WHERE exchange IS NOT NULL
    """).fetchone()[0]

    total_count = con.execute(f"""
    SELECT COUNT(*) FROM {config['tiingo']['fundamentals_meta_table']}
    """).fetchone()[0]

    print(
        f"Updated {updated_count} out of {total_count} rows in fundamentals meta table with exchange and date range information")

    # Report on symbols that couldn't be matched
    unmatched_count = total_count - updated_count
    if unmatched_count > 0:
        print(
            f"Warning: {unmatched_count} symbols in fundamentals meta table could not be matched with appropriate date ranges")

        # Optionally, log some examples of unmatched symbols for debugging
        unmatched_examples = con.execute(f"""
        SELECT symbol, vendor_symbol_id 
        FROM {config['tiingo']['fundamentals_meta_table']}
        WHERE exchange IS NULL
        LIMIT 10
        """).fetchdf()

        if not unmatched_examples.empty:
            print("Examples of unmatched symbols:")
            print(unmatched_examples)

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
            f"HTTP error when getting {symbol}\nHalting as the Tiingo service could be down or there is a problem with your account\n{e}")
        return 'fail'
    except Exception as e:
        logger.error(f"Error getting {symbol}\n{e}")
        return 'fail'


# Create a ThreadPoolExecutor
workers = int(config['DEFAULT']['threads'])
os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
duckdb_con = duckdb.connect(db_file)

# Filter results to only include symbols that have valid trading dates to download
filtered_result = []
skipped_time_check = 0

# Simplified filtering loop
for row in result:
    symbol = row[0]
    db_end_date = row[1]
    vendor_symbol_id = row[2]
    is_active = row[3]
    daily_last_updated = row[4]

    # Get the next trading date we should download (simplified - no exchange parameter)
    next_trading_date = get_next_trading_date_to_download(db_end_date)

    if next_trading_date is None:
        skipped_time_check += 1
        continue

    # Add the calculated start date to the row (remove exchange from tuple)
    filtered_result.append((symbol, db_end_date, vendor_symbol_id, is_active, daily_last_updated, next_trading_date))

# Print the number of symbols to process
msg = f"Starting daily fundamentals download. Processing {len(filtered_result)} symbols (skipped {skipped_time_check} due to time/date constraints)."
logger.info(msg)
print(msg)

if len(filtered_result) == 0:
    print("No symbols need updating at this time.")
    duckdb_con.close()
    sys.exit(0)

if workers == 1:
    for row in filtered_result:
        symbol = row[0]
        db_end_date = row[1]
        vendor_symbol_id = row[2]
        is_active = row[3]
        daily_last_updated = row[4]
        start_date = row[5]  # This is now the validated next trading date

        status = update_symbol(symbol, start_date, duckdb_con, vendor_symbol_id, is_active)
        if status == 'skip':
            count_skip += 1
        elif status == 'new':
            count_new += 1
        elif status == 'update':
            count_update += 1
        elif status == 'fail':
            count_fail += 1
else:
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks to the thread pool
        futures = {}
        for row in filtered_result:
            symbol = row[0]
            db_end_date = row[1]
            vendor_symbol_id = row[2]
            is_active = row[3]
            daily_last_updated = row[4]
            start_date = row[5]  # This is now the validated next trading date

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
                    f"Update: {total} processed | Skipped {count_skip} | New {count_new} | Updated {count_update} | Failed {count_fail}")

duckdb_con.close()
msg = f"Fundamentals daily processing complete: Skipped {count_skip} | New {count_new} | Updated {count_update} | Failed {count_fail}"
logger.info(msg)
print(msg)

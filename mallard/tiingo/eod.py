# Downloads end of day price history files, including necessary updates per supported ticker file start/end dates.
import concurrent
import configparser
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from io import StringIO
from pathlib import Path

import duckdb
import polars as pl
import requests

from mallard.RateLimiterContext import RateLimiterContext
from mallard.normalization import get_column_mapper
from mallard.tiingo.tiingo_util import quarantine_file, quarantine_data, get_next_trading_date_to_download

# Get config file
config_file = os.getenv("MALLARD_CONFIG")
config = configparser.ConfigParser()
config.read(config_file)

logger = logging.getLogger(__name__)
# Set logging format of created, level, filename, message
logging.basicConfig(filename=config['DEFAULT']['log_file'], level=config['DEFAULT']['log_level'],
                    format=config['DEFAULT']['log_format'])

db_file = config['DEFAULT']['db_file']
file_type = 'daily'

msg = f"Updating Tiingo EOD data. Using database {db_file}. "
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


@DeprecationWarning
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
            logger.error(f"{sym}: {error} No data for active symbol.")
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

tiingo_symbols = config['tiingo']['symbols_table']
tiingo_eod = config['tiingo']['eod_table']

# If tickers is the source, use the supported tickers file. Not advised unless fetching ETFs, mutual funds,
# or stocks without fundamentals like SPX.
if config['tiingo']['eod_source'] == 'tickers':
    with duckdb.connect(db_file, read_only=True) as con:
        result = con.execute(f"""
        SELECT s.symbol, e.max_date
        FROM {config['tiingo']['symbols_table']} s
        LEFT JOIN (
            SELECT symbol, MAX(date) AS max_date
            FROM {tiingo_eod}
            GROUP BY symbol
        ) e ON s.symbol = e.symbol
        WHERE s.end_date > e.max_date
        """).fetchall()
    msg = "Using Tiingo supported tickers file as the source of symbols. "
else:
    # Use the fundamentals meta file to ensure you're getting symbols that are reporting financials.
    with duckdb.connect(db_file, read_only=True) as con:
        result = con.execute(f"""
            SELECT 
                f.symbol,
                f.vendor_symbol_id,
                f.is_active,
                e.max_date
            FROM 
                {config['tiingo']['fundamentals_meta_table']} f
            LEFT JOIN (
                SELECT 
                    vendor_symbol_id, 
                    MAX(date) AS max_date
                FROM 
                    {tiingo_eod}
                GROUP BY 
                    vendor_symbol_id
            ) e ON f.vendor_symbol_id = e.vendor_symbol_id
            ORDER BY 
                f.symbol            
        """).fetchall()
        msg = "Using Tiingo fundamentals meta file as the source of symbols. "


def add_required_symbols(result: list, vendor_symbol_ids: list = [], symbols: list = []):
    """Takes lists of vendor_symbol_ids and symbols and adds those rows to result if they are not already there."""

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

            # First we check if the symbols are already in the database
            quoted_symbols = [f"'{s}'" for s in symbols]
            with duckdb.connect(db_file, read_only=True) as con:
                existing_symbols = con.execute(f"""
                SELECT s.symbol, e.max_date
                FROM {config['tiingo']['symbols_table']} s
                LEFT JOIN (
                    SELECT symbol, MAX(date) AS max_date
                    FROM {config['tiingo']['eod_table']}
                    WHERE symbol IN ({','.join(quoted_symbols)})
                    GROUP BY symbol
                ) e ON s.symbol = e.symbol
                WHERE s.end_date > e.max_date AND s.symbol IN ({','.join(quoted_symbols)})""").fetchall()
            # Add the new symbols to result
            for row in existing_symbols:
                if row not in result:
                    # We use symbol for vendor_symbol_id because so much code assumes it exists.
                    # Must be symbol, vsid, is_active, and date, matching fundamentals query above.
                    result.append((row[0], row[0], True, row[1]))
                    symbols.remove(row[0])
            # If there are still symbols left, we add them to result with vendor_symbol_id = symbol
            for symbol in symbols:
                result.append((symbol, symbol, True, None))

    if vendor_symbol_ids:
        # If there are duplicate vendor_symbol_ids, remove them
        vendor_symbol_ids = list(set(vendor_symbol_ids))
        # Remove vendor_symbol_ids that are already in result
        for vendor_symbol_id in vendor_symbol_ids:
            for row in result:
                if row[2] == vendor_symbol_id:
                    vendor_symbol_ids.remove(vendor_symbol_id)
                    break
        if not vendor_symbol_ids:
            return

        msg = f"Adding required vendor_symbol_ids: {vendor_symbol_ids}"
        print(msg)
        logger.info(msg)
        # First we check if the vendor_symbol_ids are already in the database
        quoted_vendor_symbol_ids = [f"'{s}'" for s in vendor_symbol_ids]
        with duckdb.connect(db_file, read_only=True) as con:
            new_vendor_symbol_ids = con.execute(f"""
                SELECT f.symbol, 
                       CASE WHEN (SELECT COUNT(*) FROM {config['tiingo']['eod_table']}) > 0 
                            THEN e.max_date 
                            ELSE NULL 
                       END as max_date, 
                       f.vendor_symbol_id, 
                       f.is_active
                FROM {config['tiingo']['fundamentals_meta_table']} f
                LEFT JOIN (
                    SELECT vendor_symbol_id, MAX(date) AS max_date
                    FROM {config['tiingo']['eod_table']}
                    WHERE vendor_symbol_id IN ({','.join(quoted_vendor_symbol_ids)})
                    GROUP BY vendor_symbol_id
                ) e ON f.vendor_symbol_id = e.vendor_symbol_id
                JOIN {config['tiingo']['symbols_table']} s ON f.symbol = s.symbol
                WHERE f.vendor_symbol_id IN ({','.join(quoted_vendor_symbol_ids)}) 
                  AND ((SELECT COUNT(*) FROM {config['tiingo']['eod_table']}) = 0 OR e.max_date < s.end_date)
                ORDER BY f.symbol
            """).fetchall()
        # Add the new vendor_symbol_ids to result
        for row in new_vendor_symbol_ids:
            if row not in result:
                result.append((row[0], row[1], row[2], row[3]))
                vendor_symbol_ids.remove(row[2])
        # If there are still vendor_symbol_ids left, we get their symbols and add them to result
        if vendor_symbol_ids:
            quoted_vendor_symbol_ids = [f"'{s}'" for s in vendor_symbol_ids]
            with duckdb.connect(db_file, read_only=True) as con:
                symbols = con.execute(f"""
                SELECT vendor_symbol_id, symbol, is_active
                FROM {config['tiingo']['fundamentals_meta_table']}
                WHERE vendor_symbol_id IN ({','.join(quoted_vendor_symbol_ids)})""").fetchall()
            for row in symbols:
                result.append((row[1], None, row[0], row[2]))


def get_etfs():
    etf_results = []
    with duckdb.connect(db_file, read_only=True) as con:
        # First, get the maximum end_date from the symbols table to identify active ETFs
        max_end_date = con.execute(f"""
        SELECT MAX(end_date) 
        FROM {config['tiingo']['symbols_table']}
        """).fetchone()[0]

        # Then query for ETFs with that max end_date (active ETFs)
        etfs = con.execute(f"""
        SELECT 
            s.symbol, 
            CONCAT(s.symbol, '_', s.start_date) AS vendor_symbol_id,
            TRUE AS is_active,
            e.max_date
        FROM {config['tiingo']['symbols_table']} s
        LEFT JOIN (
            SELECT 
                symbol, 
                MAX(date) AS max_date
            FROM {tiingo_eod}
            GROUP BY 
                symbol
        ) e ON s.symbol = e.symbol
        WHERE 
            s.asset_type = 'ETF' 
            AND s.end_date = '{max_end_date}'
        ORDER BY 
            s.symbol
        """).fetchall()

        etf_results = list(etfs)

        msg = f"Found {len(etf_results)} active ETFs to process."
        print(msg)
        logger.info(msg)

    return etf_results


required_vsids = [vsid for vsid in config['tiingo'].get('vendor_symbol_ids', '').split(',') if vsid]
required_symbols = config['tiingo'].get('symbols', '').split(',')

# Add required symbols to result
add_required_symbols(result, required_vsids, required_symbols)

msg += f"Processing {len(result)} symbols as required. "
print(msg)
logger.info(msg)

# Get ETFs if needed
ticker_requirements = config['tiingo'].get('ticker_requirements', '').split(',')
if 'etf' in ticker_requirements:
    etf_results = get_etfs()

    # Combine results
    if etf_results:
        # Create a set of existing vendor_symbol_ids to avoid duplicates
        existing_symbols = {row[0] for row in result if row[0] is not None}

        # Add ETFs that aren't already in the result
        for etf in etf_results:
            if etf[0] not in existing_symbols:
                result.append(etf)

        msg = f"Combined result now contains {len(result)} symbols (including ETFs)."
        print(msg)
        logger.info(msg)


@DeprecationWarning
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
    if shutdown_flag:
        return 'skip'

    try:
        # Format start_date as string for URL if it's a date object
        start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, date) else start_date

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
        if '_' in vendor_symbol_id:
            # Extract the part before the underscore
            vsid = vendor_symbol_id.split('_')[0]
        else:
            vsid = vendor_symbol_id

        url = f"{base_url}/{vsid}/prices?startDate={start_date_str}&format=csv&token={config['tiingo']['api_token']}"

        # Get number of days since start_date
        days_missing = (datetime.now().date() - start_date).days if isinstance(start_date, date) else 0

        with RateLimiterContext():
            r = requests.get(url)
        r.raise_for_status()
        data = r.content

        # Check for empty or invalid data
        if not data or not data.startswith(
                b'date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor'):
            # Due to errors from the vendor, don't quarantine automatically anymore.
            if not is_append or is_active == False or days_missing >= 7:
                logger.warning(f"Empty or invalid data format for {symbol} ({vendor_symbol_id})")
                return 'fail'
            else:
                return 'skip'

        # Check if there's any data after the header
        data_without_header = r.content.split(b'\n', 1)[1]
        if not data_without_header:
            # Apply the same logic as above
            if not is_append or is_active == False or days_missing >= 7:
                logger.warning(f"Empty or invalid data format for {symbol} ({vendor_symbol_id})")
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
                    # Delete all daily metrics
                    with duckdb.connect(db_file) as con:
                        con.execute(f"""
                        DELETE FROM daily_metrics 
                        WHERE vendor_symbol_id = '{vendor_symbol_id}';
                        DELETE FROM tiingo_fundamentals_daily
                        WHERE vendor_symbol_id = '{vendor_symbol_id}';
                        """)
                    logger.info(
                        f"{symbol}: Dividend or split detected. Refetching full history for {symbol} / {vendor_symbol_id}.")

                    return update_symbol(symbol, '1900-01-01', duckdb_con, vendor_symbol_id, is_active)

            # Rename columns to match the EOD table
            col = get_column_mapper('tiingo')
            df = df.rename({col('adj_close'): 'adj_close', col('adj_high'): 'adj_high', col('adj_low'): 'adj_low',
                            col('adj_open'): 'adj_open', col('adj_volume'): 'adj_volume', col('div_cash'): 'div_cash',
                            col('split_factor'): 'split_factor'})
            # Add symbols columns
            df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])

            # Insert the DataFrame into the EOD table
            with duckdb_con.cursor() as local_con:
                local_con.execute(f"INSERT OR REPLACE INTO {config['tiingo']['eod_table']} BY NAME FROM df")
        except duckdb.ConnectionException as e:
            print(f"Can't connect to DB, exiting. Error:\n{e}")
            exit(1)
        except Exception as e:
            print(f"Error inserting {symbol} into EOD table\n{e}")
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


def delete_infinity(duckdb_con):
    """Price data can have adjusted values of infinity, which is obviously impossible.
    This finds rows with infinity and deletes them and everything before it.
    At current time, this affects only a few stocks."""
    eod_table = config['tiingo']['eod_table']
    with duckdb_con.cursor() as local_con:
        local_con.execute(f"""
        DELETE FROM {eod_table} eod
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT
                    vendor_symbol_id,
                    MAX(date) AS max_date
                FROM {eod_table}
                WHERE adj_open = 'infinity'
                    OR adj_close = 'infinity'
                    OR adj_high = 'infinity'
                    OR adj_low = 'infinity'
                GROUP BY vendor_symbol_id
            ) AS latest_infinities
            WHERE eod.vendor_symbol_id = latest_infinities.vendor_symbol_id
                AND eod.date <= latest_infinities.max_date
        );
        """)


# Create a ThreadPoolExecutor
workers = int(config['DEFAULT']['threads'])
os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
duckdb_con = duckdb.connect(db_file)

### DANGER: TODO Single threading is BROKEN, code is modified for one-off testing 
# If workers is 1, use a for loop to process the symbols. This is useful for debugging.
# Replace the entire threading section (around lines 430-480) with:
if workers == 1:
    for row in result:
        symbol = row[0]
        vendor_symbol_id = row[1]
        is_active = row[2]
        db_end_date = row[3]

        # Get the next trading date we should download
        start_date = get_next_trading_date_to_download(db_end_date)

        if start_date is None:
            count_skip += 1
            continue

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
            vendor_symbol_id = row[1]
            is_active = row[2]
            db_end_date = row[3]

            # Get the next trading date we should download
            start_date = get_next_trading_date_to_download(db_end_date)

            if start_date is None:
                count_skip += 1
                continue

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

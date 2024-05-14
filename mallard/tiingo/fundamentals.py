"""This script does 3 things:
1. Downloads the Tiingo fundamentals metadata, stores it in a table, and optionally removes symbols from the supported
symbols table if they have no fundamental data.
NOTE: This table is recreated from scratch every time.

2. Downloads/updates latest/amended financial statements for all stocks, storing in a fundamentals table.

3. Downloads/updates reported/original financial statements for all stocks, storing in a reported fundamentals table.
"""
import concurrent
import configparser
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import StringIO

import duckdb
import polars as pl
import requests

from mallard.RateLimiterContext import RateLimiterContext
from mallard.normalization import get_sql_column_provider
from mallard.tiingo.tiingo_util import logger, quarantine_data

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']

# Download fundamentals meta CSV, which is mostly a company file, but has update timestamps for their statement and
# daily meta endpoints.

meta_csv = os.path.join(config['tiingo']['dir'], config['tiingo']['fundamentals_meta_csv'])
meta_url = config['tiingo']['fundamentals_meta_url'] + "?format=csv&token=" + config['tiingo']['api_token']
response = requests.get(meta_url)
with open(meta_csv, 'wb') as f:
    f.write(response.content)
# Truncate fundamentals meta table
print(f"Truncating {config['tiingo']['fundamentals_meta_table']}")
with duckdb.connect(db_file) as con:
    con.execute(f"DELETE FROM {config['tiingo']['fundamentals_meta_table']}")

# Store the fundamentals meta CSV file in its own table. We use normalized column names in the table.
# If the user doesn't have the fundamentals addon, we leave out columns with warnings instead of data.
col = get_sql_column_provider('tiingo')
ticker_requirements = config['DEFAULT']['ticker_requirements'].split(",")
# If the user only wants USD reporting companies, add WHERE clause
where = f"WHERE reporting_currency = 'usd'" if "usd" in ticker_requirements else ""
if config.getboolean('tiingo', 'has_fundamentals_addon'):
    select_query = f"""
        SELECT {col('vendor_symbol_id')}, {col('symbol')}, {col('name')}, {col('is_active')}, {col('is_adr')},
        {col('sector')}, {col('industry')}, {col('sic_code')}, {col('sic_sector')}, {col('sic_industry')}, {col('reporting_currency')},
        {col('location')}, {col('company_website')}, {col('sec_filing_website')}, {col('statement_last_updated')}, {col('daily_last_updated')},
        {col('vendor_entity_id')} 
        FROM read_csv('{meta_csv}', header=true)
        {where}"""
else:
    select_query = f"""
        SELECT {col('vendor_symbol_id')}, {col('symbol')}, {col('name')}, {col('is_active')}, {col('is_adr')},
        {col('reporting_currency')}, {col('statement_last_updated')}, {col('daily_last_updated')} 
        FROM read_csv('{meta_csv}', header=true)
        {where}"""
print(f"Loading {config['tiingo']['fundamentals_meta_table']} with query:\n{select_query}")
with duckdb.connect(db_file) as con:
    con.execute(f"""
        INSERT INTO {config['tiingo']['fundamentals_meta_table']} BY NAME
        {select_query}
        """)
    # Convert symbol column to uppercase
    con.execute(f"""
        UPDATE {config['tiingo']['fundamentals_meta_table']} SET symbol = UPPER(symbol), reporting_currency = UPPER(reporting_currency)
        """)
    print(f"Rows in {config['tiingo']['fundamentals_meta_table']}:")
    con.sql(f"SELECT COUNT(symbol) FROM {config['tiingo']['fundamentals_meta_table']}").show()

# Delete rows from symbols table if symbol not in fundamentals meta table (if configured).
ticker_requirements = config["DEFAULT"]["ticker_requirements"].split(",")
if "fundamentals" in ticker_requirements:
    with duckdb.connect(db_file) as con:
        print(
            f"Deleting rows in {config['tiingo']['symbols_table']} that aren't in {config['tiingo']['fundamentals_meta_table']}. Current count:")
        con.sql(f"SELECT COUNT(symbol) FROM {config['tiingo']['symbols_table']}").show()
        con.execute(f"""
            DELETE FROM {config['tiingo']['symbols_table']} WHERE symbol NOT IN (SELECT UPPER(symbol) FROM {config['tiingo']['fundamentals_meta_table']})
            """)
        print(f"{config['tiingo']['symbols_table']} new row count:")
        con.sql(f"SELECT COUNT(symbol) FROM {config['tiingo']['symbols_table']}").show()


def update_symbol(vendor_symbol_id, symbol, is_active, duckdb_con, as_reported=False):
    """Update the fundamentals for the given symbol. If fundamentals have been updated, we have no idea if a new
    statement was added or a previous one was amended, so we download and replace everything. Updates both the file and
    the database."""
    data = None
    try:
        if as_reported:
            fundamentals_table = config['tiingo']['fundamentals_reported_table']
            file_name = f"{symbol}_{vendor_symbol_id}_fundamentals_reported.csv"
        else:
            fundamentals_table = config['tiingo']['fundamentals_amended_table']
            file_name = f"{symbol}_{vendor_symbol_id}_fundamentals_amended.csv"
        # Skip bad files
        if (os.path.exists(os.path.join(exclude_dir, file_name)) or
                os.path.exists(os.path.join(quarantine_dir, file_name))):
            return 'skip'
        reported_param = "&asReported=true" if as_reported else ""
        url = f"{config['tiingo']['fundamentals_url']}/{vendor_symbol_id}/statements?startDate=1900-01-01{reported_param}&format=csv&token={config['tiingo']['api_token']}"
        with RateLimiterContext():
            r = requests.get(url)
        r.raise_for_status()
        data = r.content
        # Check if data is None, is empty, or is equivalent to the string '[]'
        if not data or data == b'[]' or not data.startswith(
                b'date,year,quarter,statementType,dataCode,value'):
            quarantine_data(symbol, vendor_symbol_id, is_active, data)
            return 'fail'
        data_without_header = data.split(b'\n', 1)[1]
        if not data_without_header:
            quarantine_data(symbol, vendor_symbol_id, is_active, data)
            return 'fail'
        # Create a DataFrame from the CSV data and rename the columns to match the EOD table
        df = pl.read_csv(StringIO(data.decode('utf-8')))
        df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])
    except Exception as e:
        print(f"Error reading CSV for {vendor_symbol_id} / {symbol}: {e}")
        if data is not None:
            quarantine_data(symbol, vendor_symbol_id, is_active, data)
        return 'fail'
    # Insert the data into the table
    try:
        with duckdb_con.cursor() as local_con:
            # Delete existing rows for this vendor_symbol_id
            local_con.execute(f"DELETE FROM {fundamentals_table} WHERE vendor_symbol_id = '{vendor_symbol_id}'")
            local_con.execute(f"INSERT INTO {fundamentals_table} BY NAME FROM df")
    except duckdb.ConnectionException as e:
        print(f"Can't connect to DB, exiting. Error:\n{e}")
        exit(1)
    except Exception as e:
        print(f"Error inserting data for {vendor_symbol_id} / {symbol}: {e}")
        quarantine_data(symbol, vendor_symbol_id, is_active, data)
        return 'fail'
    # Save the CSV file
    update_dir = fundamentals_reported_dir if as_reported else fundamentals_amended_dir
    with open(os.path.join(update_dir, file_name), 'wb') as f:
        f.write(data)
    return 'success'


def update_fundamentals(as_of, as_reported=False):
    """Uses a thread pool to download fundamentals for all symbols that require updating and updates the DB."""

    # Get symbols to update based on whether they have changed since the last time this was run.
    where = f"WHERE statement_last_updated > '{last_update}'" if last_update else ""
    with duckdb.connect(db_file) as con:
        symbols_query = f"""
            SELECT vendor_symbol_id, symbol, is_active FROM {config['tiingo']['fundamentals_meta_table']}
            {where}
            """
        symbol_ids = con.sql(symbols_query)
        print(f"Downloading fundamentals for symbols:")
        symbol_ids.count('vendor_symbol_id').show()
        result = symbol_ids.fetchall()
    count_success = 0
    count_fail = 0
    count_skip = 0
    workers = int(config['DEFAULT']['threads'])
    os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
    duckdb_con = duckdb.connect(db_file)
    # If single threaded, don't use a ThreadPoolExecutor. This enables us to use the debugger.
    if workers == 1:
        for row in result:
            try:
                status = update_symbol(row[0], row[1], row[2], duckdb_con, as_reported)
                if status == 'skip':
                    count_skip += 1
                elif status == 'success':
                    count_success += 1
                elif status == 'fail':
                    count_fail += 1
            except Exception as e:
                print(f"Error processing {row}\n{e}")
                count_fail += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit the tasks to the thread pool
            futures = {executor.submit(update_symbol, row[0], row[1], row[2], duckdb_con, as_reported): row for row in
                       result}
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                try:
                    status = future.result()
                    if status == 'skip':
                        count_skip += 1
                    elif status == 'success':
                        count_success += 1
                    elif status == 'fail':
                        count_fail += 1
                except Exception as e:
                    print(f"Error processing {row}\n{e}")
                    count_fail += 1
    duckdb_con.close()
    # Save the update timestamp
    update_dir = fundamentals_reported_dir if as_reported else fundamentals_amended_dir
    last_update_file = os.path.join(update_dir, 'fundamentals_last_updated.txt')
    with open(last_update_file, 'w') as f:
        f.write(update_timestamp.isoformat())
    logger.info(f"Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}")


def get_last_update(as_reported=False):
    update_dir = fundamentals_reported_dir if as_reported else fundamentals_amended_dir
    last_update_file = os.path.join(update_dir, 'fundamentals_last_updated.txt')
    if os.path.exists(last_update_file):
        with open(last_update_file, 'r') as f:
            last_update = datetime.fromisoformat(f.read())
    else:
        last_update = None
    return last_update


def create_fundamentals_amended_distinct():
    """In tiingo_fundamentals_amended, some quarters have multiple filings on different dates.
     In addition, those filings might have different statements or fields (dataCode).
     This query combines those separate filings into one, taking the most recent distinct dataCode in each quarter."""
    with duckdb.connect(db_file) as con:
        con.execute(f"""
        CREATE OR REPLACE TABLE tiingo_fundamentals_amended_distinct AS
        SELECT
            vendor_symbol_id,
            symbol,
            date,
            year,
            quarter,
            statementType,
            dataCode,
            value
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY vendor_symbol_id, year, quarter, dataCode
                       ORDER BY date DESC
                   ) as rn
            FROM tiingo_fundamentals_amended
        ) sub
        WHERE rn = 1;
        """)


# Download fundamentals (financial statements)
if config.getboolean('tiingo', 'has_fundamentals_addon'):
    fundamentals_amended_dir = os.path.join(config['tiingo']['dir'], 'fundamentals/amended')
    os.makedirs(fundamentals_amended_dir, exist_ok=True)
    fundamentals_reported_dir = os.path.join(config['tiingo']['dir'], 'fundamentals/reported')
    os.makedirs(fundamentals_reported_dir, exist_ok=True)
    exclude_dir = config['tiingo']['dir'] + '/exclude'
    os.makedirs(exclude_dir, exist_ok=True)
    quarantine_dir = config['tiingo']['dir'] + '/quarantine'
    os.makedirs(quarantine_dir, exist_ok=True)
    update_timestamp = datetime.now()

    # Update amended fundamentals
    last_update = get_last_update()
    update_fundamentals(last_update)

    # Update reported fundamentals
    last_update = get_last_update(True)
    update_fundamentals(last_update, as_reported=True)
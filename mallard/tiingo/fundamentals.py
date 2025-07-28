"""This script does the following:
1. Downloads the Tiingo fundamentals metadata, stores it in a table, and optionally removes symbols from the supported
symbols table if they have no fundamental data.
NOTE: This table is recreated from scratch every time.

2. Downloads/updates latest/amended financial statements for all stocks, storing in an amended fundamentals table.

3. Downloads/updates reported/original financial statements for all stocks, storing in a reported fundamentals table.

4. Forward fills the fundamentals tables.

"""
import concurrent
import configparser
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import StringIO

import duckdb
import polars as pl
import requests
from polars import DataFrame

from mallard.RateLimiterContext import RateLimiterContext
from mallard.normalization import get_sql_column_provider
from mallard.tiingo.tiingo_util import logger, quarantine_data

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
config = configparser.ConfigParser()
config.read(config_file)
metrics = set(config['DEFAULT']['metrics'].split(","))

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
if hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, signal_handler)

# Download fundamentals meta CSV, which is mostly a company file, but has update timestamps for their statement and
# daily meta endpoints.

meta_csv = os.path.join(config['tiingo']['dir'], config['tiingo']['fundamentals_meta_csv'])
meta_url = config['tiingo']['fundamentals_meta_url'] + "?format=csv&token=" + config['tiingo']['api_token']
response = requests.get(meta_url)
with open(meta_csv, 'wb') as f:
    f.write(response.content)

fundamentals_meta_table = config['tiingo']['fundamentals_meta_table']
fundamentals_amended_distinct_table = config['tiingo']['fundamentals_amended_distinct_table']
fundamentals_amended_table = config['tiingo']['fundamentals_amended_table']
fundamentals_last_updated_table = config['tiingo']['fundamentals_last_updated_table']

# Truncate fundamentals meta table
print(f"Truncating {fundamentals_meta_table}")
with duckdb.connect(db_file) as con:
    con.execute(f"DELETE FROM {fundamentals_meta_table}")

# Store the fundamentals meta CSV file in its own table. We use normalized column names in the table.
# If the user doesn't have the fundamentals addon, we leave out columns with warnings instead of data.
col = get_sql_column_provider('tiingo')
ticker_requirements = config['DEFAULT']['ticker_requirements'].split(",")
# If the user only wants USD reporting companies, add WHERE clause
# where = f"WHERE reporting_currency = 'usd'" if "usd" in ticker_requirements else ""

# Extract the symbols list from config
config_symbols = config['DEFAULT']['symbols'].split(',') if 'symbols' in config['DEFAULT'] else []
config_symbols_str = ', '.join([f"'{s.strip()}'" for s in config_symbols])

# If the user only wants USD reporting companies, add WHERE clause
# Modified to include symbols from config.ini
if "usd" in ticker_requirements:
    where = f"WHERE reporting_currency = 'usd' OR UPPER(ticker) IN ({config_symbols_str})"

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
print(f"Loading {fundamentals_meta_table} with query:\n{select_query}")
with duckdb.connect(db_file) as con:
    con.execute(f"""
        INSERT INTO {fundamentals_meta_table} BY NAME
        {select_query}
        """)
    # Convert symbol column to uppercase
    con.execute(f"""
        UPDATE {fundamentals_meta_table} SET symbol = UPPER(symbol), reporting_currency = UPPER(reporting_currency)
        """)
    print(f"Rows in {fundamentals_meta_table}:")
    con.sql(f"SELECT COUNT(symbol) FROM {fundamentals_meta_table}").show()

# Delete rows from symbols table if symbol not in fundamentals meta table (if configured).
ticker_requirements = config["DEFAULT"]["ticker_requirements"].split(",")
symbols_table = config['tiingo']['symbols_table']
if "fundamentals" in ticker_requirements:
    with duckdb.connect(db_file) as con:
        
        # TODO investigate consequences of this
        # print(
        #     f"Deleting rows in {symbols_table} that aren't in {fundamentals_meta_table}. Current count:")
        # con.sql(f"SELECT COUNT(symbol) FROM {symbols_table}").show()
        # con.execute(f"""
        #     DELETE FROM {symbols_table} WHERE symbol NOT IN (SELECT UPPER(symbol) FROM {fundamentals_meta_table})
        #     """)
        # print(f"{symbols_table} new row count:")
        # con.sql(f"SELECT COUNT(symbol) FROM {symbols_table}").show()

        # Delete rows from fundamentals meta table if symbol not in symbols table (if configured).
        print(
            f"Deleting rows in {fundamentals_meta_table} that aren't in {symbols_table}. Current count:")
        con.sql(f"SELECT COUNT(symbol) FROM {fundamentals_meta_table}").show()
        con.execute(f"""
            DELETE FROM {fundamentals_meta_table} WHERE UPPER(symbol) NOT IN (SELECT symbol FROM {symbols_table})
            """)
        print(f"{fundamentals_meta_table} new row count:")
        con.sql(f"SELECT COUNT(symbol) FROM {fundamentals_meta_table}").show()

# For each row in fundamentals meta table, create a row in the last updated table with vendor_symbol_id if it doesn't
# already exist. This is used to track which symbols have been updated.
with duckdb.connect(db_file) as con:
    try:
        con.execute(f"""
        INSERT OR IGNORE INTO {fundamentals_last_updated_table} (vendor_symbol_id)
        SELECT vendor_symbol_id
        FROM {fundamentals_meta_table}
        """)
    except Exception as e:
        print(f"Error updating {fundamentals_last_updated_table}: {e}")
        exit(1)


def update_fundamentals_last_updated(vendor_symbol_id: str, duckdb_con, as_reported: bool = False):
    """Insert the vendor_symbol_id and the timestamp in tiingo_fundamentals_meta.statement_last_updated
    into tiingo_fundamentals_last_updated. Update if it already exists."""
    with duckdb_con.cursor() as con:
        try:
            con.execute(f"""
            UPDATE {config['tiingo']['fundamentals_last_updated_table']} 
            SET {'reported_last_updated' if as_reported else 'amended_last_updated'} = (
                SELECT statement_last_updated 
                FROM {config['tiingo']['fundamentals_meta_table']} 
                WHERE vendor_symbol_id = '{vendor_symbol_id}')
            WHERE vendor_symbol_id = '{vendor_symbol_id}'
            """)

        except Exception as e:
            print(f"Error updating {config['tiingo']['fundamentals_last_updated_table']}: {e}")


def update_symbol(vendor_symbol_id, symbol, is_active, duckdb_con, as_reported=False):
    """Update the fundamentals for the given symbol. If fundamentals have been updated, we have no idea if a new
    statement was added or a previous one was amended, so we download and replace everything. Updates both the file and
    the database."""
    if shutdown_flag:
        return 'skip'

    data = None
    file_type = 'fundamentals_reported' if as_reported else 'fundamentals_amended'
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
            logger.info(f"Skipping {file_name} because it's in the quarantine or exclude directory")
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
            quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
            return 'fail'
        data_without_header = data.split(b'\n', 1)[1]
        if not data_without_header:
            quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
            return 'fail'
        # Create a DataFrame from the CSV data and add the vendor_symbol_id and symbol columns.
        df = pl.read_csv(StringIO(data.decode('utf-8')))
        df = df.with_columns([pl.lit(vendor_symbol_id).alias('vendor_symbol_id'), pl.lit(symbol).alias('symbol')])
        # Remove duplicate rows
        df = df.unique()
        # Convert/pivot the DataFrame from long to wide format
        df_wide = df.pivot(
            index=["date", "year", "quarter", "vendor_symbol_id", "symbol"],
            columns="dataCode",
            values="value"
        )
    except Exception as e:
        print(f"Error reading CSV for {vendor_symbol_id} / {symbol}: {e}")
        if data is not None:
            quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
        return 'fail'
    # Insert the data into the table
    try:
        with duckdb_con.cursor() as local_con:
            # Delete existing rows for this vendor_symbol_id
            local_con.execute(f"DELETE FROM {fundamentals_table} WHERE vendor_symbol_id = '{vendor_symbol_id}'")
            local_con.execute(f"INSERT INTO {fundamentals_table} BY NAME FROM df_wide")
    except duckdb.ConnectionException as e:
        print(f"Can't connect to DB, exiting. Error:\n{e}")
        exit(1)
    except Exception as e:
        print(f"Error inserting data for {vendor_symbol_id} / {symbol}: {e}")
        quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type)
        return 'fail'
    # Save the CSV file
    update_dir = fundamentals_reported_dir if as_reported else fundamentals_amended_dir
    with open(os.path.join(update_dir, file_name), 'wb') as f:
        f.write(data)
    # Update the last updated timestamp
    update_fundamentals_last_updated(vendor_symbol_id, duckdb_con, as_reported)
    return 'success'


# Upon review of the raw fundamental data, this does not appear necessary.
@DeprecationWarning
def forward_fill_reported(duckdb_con):
    """Forward fills values in the reported fundamentals table because subsequent filings might be missing data from
    previous filings for the same year/quarter."""
    fundamentals_table = config['tiingo']['fundamentals_reported_table']
    with duckdb_con.cursor() as con:
        # Get list of metric columns, excluding identifier and time columns, from fundamentals_table
        column_names = con.sql(f"SELECT * FROM {fundamentals_table}").columns
        column_names = [col for col in column_names if
                        col not in ("vendor_symbol_id", "symbol", "date", "year", "quarter")]
        # Create LAST_VALUE window function query for each metric column
        window_queries = [
            f"""LAST_VALUE({col} IGNORE NULLS) OVER (PARTITION BY vendor_symbol_id, "year", quarter ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {col}"""
            for col in column_names]
        set_queries = [f"{col} = ff.{col}" for col in column_names]
        query = f"""
        WITH ForwardFilled AS (
            SELECT
                vendor_symbol_id,
                date,
                year,
                quarter,
                {', '.join(window_queries)}
            FROM tiingo_fundamentals_reported
        )
        
        UPDATE tiingo_fundamentals_reported AS t
        SET 
            {', '.join(set_queries)}
        FROM ForwardFilled ff
        WHERE t.vendor_symbol_id = ff.vendor_symbol_id
            AND t.date = ff.date
            AND t.year = ff.year
            AND t.quarter = ff.quarter;
        """
        # print(query)


def update_fundamentals(symbol_ids, as_reported=False):
    """Uses a thread pool to download fundamentals for all symbols that require updating and updates the DB."""
    count_success = 0
    count_fail = 0
    count_skip = 0
    workers = int(config['DEFAULT']['threads'])
    os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
    duckdb_con = duckdb.connect(db_file)
    # If single threaded, don't use a ThreadPoolExecutor. This enables us to use the debugger.
    if workers == 1:
        for row in symbol_ids:
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
            # Print progress
            if count_success % 100 == 0:
                if as_reported:
                    print(
                        f"Reported fundamentals progress: Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}")
                else:
                    print(
                        f"Amended fundamentals progress: Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}")

    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit the tasks to the thread pool
            futures = {executor.submit(update_symbol, row[0], row[1], row[2], duckdb_con, as_reported): row for row in
                       symbol_ids}
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
                # Print progress
                if (count_success + count_fail + count_skip) % 500 == 0:
                    if as_reported:
                        print(
                            f"Reported fundamentals progress: Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}")
                    else:
                        print(
                            f"Amended fundamentals progress: Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}")

    duckdb_con.close()
    # Save the update timestamp
    update_dir = fundamentals_reported_dir if as_reported else fundamentals_amended_dir
    last_update_file = os.path.join(update_dir, 'fundamentals_last_updated.txt')
    with open(last_update_file, 'w') as f:
        f.write(update_timestamp.isoformat())
    if as_reported:
        msg = f"Reported fundamentals finished: Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}"
        logger.info(msg)
        print(msg)
    else:
        msg = f"Amended fundamentals finished: Skipped {count_skip} | Downloaded {count_success} | Failed {count_fail}"
        logger.info(msg)
        print(msg)


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
    """Creates a table that combines multiple filings per quarter into one, taking the most recent data for each field."""
    msg = f"Creating the {fundamentals_amended_distinct_table} table with the latest data for each metric, per quarter."
    print(msg)
    logger.info(msg)
    df = forward_fill_metrics_table(fundamentals_amended_table)

    # List of metric columns, excluding identifier and time columns
    column_names = [col for col in df.columns if col not in ("vendor_symbol_id", "symbol", "date", "year", "quarter")]

    # Add custom metrics to column names list
    # Group by the key columns and aggregate using the last non-null value for each of the columns
    df_result = df.group_by(["vendor_symbol_id", "year", "quarter"]).agg(
        [pl.col("symbol").last().alias("symbol"),
         pl.col("date").last().alias("date")] +
        [pl.col(column).last().alias(column) for column in column_names]
    )

    with duckdb.connect(db_file) as con:
        con.execute(f"CREATE OR REPLACE TABLE {fundamentals_amended_distinct_table} AS SELECT * FROM df_result")
        # Add margin columns
        con.execute(f"ALTER TABLE {fundamentals_amended_distinct_table} ADD COLUMN IF NOT EXISTS gross_margin DOUBLE")
        con.execute(
            f"ALTER TABLE {fundamentals_amended_distinct_table} ADD COLUMN IF NOT EXISTS operating_margin DOUBLE")
        con.execute(f"ALTER TABLE {fundamentals_amended_distinct_table} ADD COLUMN IF NOT EXISTS net_margin DOUBLE")


def forward_fill_metrics_table(table) -> DataFrame:
    """Takes a table name with metrics columns, such as the fundamentals tables, and does a forward fill on null
    values.
    NOTE: I'm not sure this is necessary, but doesn't hurt."""
    with duckdb.connect(db_file) as con:
        df = con.sql(
            f"SELECT * FROM {fundamentals_amended_table} ORDER BY vendor_symbol_id, year, quarter, date").pl()

    # List of metric columns, excluding identifier and time columns
    column_names = [col for col in df.columns if
                    col not in ("vendor_symbol_id", "symbol", "date", "year", "quarter")]

    # Apply forward fill directly on each metric column
    return df.with_columns(
        [pl.col(column).forward_fill().over(["vendor_symbol_id", "year", "quarter"]).alias(column)
         for column in column_names]
    )


def get_symbols_to_update(as_reported=False):
    """Get a list of symbols that need updating. This is a list of tuples with vendor_symbol_id, symbol, and is_active."""
    with duckdb.connect(db_file) as con:
        # Get vendor_symbol_ids where the statement_last_updated timestamp in tiingo_fundamentals_meta is greater than
        # the one in tiingo_fundamentals_last_updated. We do this once for both amended and reported fundamentals
        # because the meta file doesn't designate which was updated.
        needs_update = f"""
        SELECT m.vendor_symbol_id,
               m.symbol,
               is_active
        FROM tiingo_fundamentals_meta m
        LEFT JOIN tiingo_fundamentals_last_updated u
        ON m.vendor_symbol_id = u.vendor_symbol_id
        WHERE
            {'u.reported_last_updated' if as_reported else 'u.amended_last_updated'} IS NULL OR
            m.statement_last_updated > {'u.reported_last_updated' if as_reported else 'u.amended_last_updated'};"""

        symbol_ids = con.sql(needs_update)
        symbols_count = symbol_ids.count('vendor_symbol_id').fetchall()[0][0]
        msg = f"Downloading fundamentals for {symbols_count} symbols:"
        print(msg)
        logger.info(msg)
        result = symbol_ids.fetchall()
    return result


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
    result = get_symbols_to_update()
    update_fundamentals(result)

    # Update reported fundamentals
    result = get_symbols_to_update(as_reported=True)
    update_fundamentals(result, as_reported=True)

    # Create a distinct table for amended fundamentals if roic, margins, or rank is in metrics list
    if {'roic', 'margins', 'rank'}.intersection(metrics):
        create_fundamentals_amended_distinct()

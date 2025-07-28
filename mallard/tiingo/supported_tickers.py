# Script to build a starting universe of tickers from Tiingo's supported_tickers.csv file
# Applies filters to data per config.ini.
# NOTE: drops/creates the table every time to ensure it contains exactly what is specified in config file.
import configparser
import logging
import os
import zipfile

import duckdb
import requests
import pandas as pd

from mallard.normalization import get_sql_column_provider

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise ValueError("MALLARD_CONFIG environment variable not set.")
config = configparser.ConfigParser()
config.read(config_file)

# Download latest supported tickers file
# It's up to the user to schedule this script so it only runs on weekday nights to avoid unnecessary processing/download
supported_tickers_url = config['tiingo']['supported_tickers_url']
tiingo_dir = config['tiingo']['dir']
os.makedirs(tiingo_dir, exist_ok=True)
supported_tickers_zip = os.path.join(tiingo_dir, config['tiingo']['supported_tickers_zip'])
supported_tickers_csv = os.path.join(tiingo_dir, config['tiingo']['supported_tickers_csv'])

# Configure logging
log_file = config['DEFAULT']['log_file']
log_level = config['DEFAULT']['log_level']
log_format = config['DEFAULT']['log_format']
logging.basicConfig(filename=log_file, level=log_level, format=log_format)
logger = logging.getLogger(__name__)

msg = f"Downloading {supported_tickers_zip}"
logger.info(msg)
print(msg)

r = requests.get(supported_tickers_url)
r.raise_for_status()
with open(supported_tickers_zip, 'wb') as f:
    f.write(r.content)

with zipfile.ZipFile(supported_tickers_zip, 'r') as zip_ref:
    zip_ref.extractall(tiingo_dir)

# Get ticker and endDate where there are more than 2 rows with the same ticker
db_file = config['DEFAULT']['db_file']
with duckdb.connect(db_file) as con:
    # Load the CSV directly into a pandas DataFrame to avoid DuckDB date parsing issues
    df = pd.read_csv(supported_tickers_csv)
    print(f"Loaded {len(df)} rows from supported_tickers.csv")

    # Filter out rows with null tickers
    df = df[df['ticker'].notna()]
    print(f"After removing null tickers: {len(df)} rows")

    # Filter symbols per config
    ticker_requirements = config['DEFAULT']['ticker_requirements'].split(",")
    msg = f"Filtering tickers with requirements: {ticker_requirements}"
    print(msg)
    logger.info(msg)

    # Convert dates to datetime for further processing
    df['start_date'] = pd.to_datetime(df['startDate'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['endDate'], errors='coerce')
    # Drop old columns
    df.drop(columns=['startDate', 'endDate'], inplace=True)

    # If there are any required symbols, we save them and add them back in after the filtering.
    required_symbols = config['DEFAULT'].get('symbols', None)
    if required_symbols:
        required_symbols = required_symbols.split(",")
        # required_symbols = [f"'{s}'" for s in required_symbols]
        # required_symbols_str = ",".join(required_symbols)
        required_rows = df[df['ticker'].isin(required_symbols)]
        print(f"Found {len(required_rows)} required symbols")

    # Filter out rows with invalid dates
    df = df[df['start_date'].notna() & df['end_date'].notna()]
    print(f"After removing invalid dates: {len(df)} rows")

    # Apply filters step by step, materializing at each step
    filtered_df = df.copy()

    if ticker_requirements:
        if 'exchange' in ticker_requirements:
            filtered_df = filtered_df[filtered_df['exchange'].notna()]
            print(f"After 'exchange' filter: {len(filtered_df)} rows")

        # If ETF specified, also include stocks.
        if 'etf' in ticker_requirements:
            filtered_df = filtered_df[filtered_df['assetType'].isin(['Stock', 'ETF'])]
            print(f"After 'stock' filter: {len(filtered_df)} rows")
        elif 'stock' in ticker_requirements:
            filtered_df = filtered_df[filtered_df['assetType'] == 'Stock']
            print(f"After 'stock' filter: {len(filtered_df)} rows")

        if 'usd' in ticker_requirements:
            filtered_df = filtered_df[filtered_df['priceCurrency'] == 'USD']
            print(f"After 'usd' filter: {len(filtered_df)} rows")

        if '99days' in ticker_requirements:
            filtered_df = filtered_df[(filtered_df['end_date'] - filtered_df['start_date']).dt.days >= 99]
            print(f"After '99days' filter: {len(filtered_df)} rows")

        if 'nocolon' in ticker_requirements:
            filtered_df = filtered_df[~filtered_df['ticker'].str.contains(':')]
            print(f"After 'nocolon' filter: {len(filtered_df)} rows")

    # Handle overlapping date ranges for the same ticker
    msg = "Checking for overlapping date ranges and keeping most recent entries..."
    print(msg)
    logger.info(msg)

    # Find tickers with multiple entries
    ticker_counts = filtered_df['ticker'].value_counts()
    multi_entry_tickers = ticker_counts[ticker_counts > 1].index.tolist()

    if multi_entry_tickers:
        msg = f"Found {len(multi_entry_tickers)} tickers with multiple entries"
        print(msg)
        logger.info(msg)

        # Process each ticker with multiple entries
        rows_to_keep = []

        for ticker in multi_entry_tickers:
            ticker_data = filtered_df[filtered_df['ticker'] == ticker].copy()

            # Sort by end_date in descending order (most recent first)
            ticker_data = ticker_data.sort_values('end_date', ascending=False)

            # Initialize list to track which entries to keep
            entries_to_keep = []
            covered_ranges = []

            # Process each entry
            for idx, entry in ticker_data.iterrows():
                # Check if this entry overlaps with any entry we've decided to keep
                is_overlapping = False
                for start, end in covered_ranges:
                    if entry['start_date'] <= end and entry['end_date'] >= start:
                        is_overlapping = True
                        break

                # If not overlapping, add to entries to keep
                if not is_overlapping:
                    entries_to_keep.append(idx)
                    covered_ranges.append((entry['start_date'], entry['end_date']))

            # Add the indices to keep
            rows_to_keep.extend(entries_to_keep)

        # Keep single-entry tickers and the selected entries for multi-entry tickers
        single_entry_mask = ~filtered_df['ticker'].isin(multi_entry_tickers)
        multi_entry_mask = filtered_df.index.isin(rows_to_keep)

        filtered_df = filtered_df[single_entry_mask | multi_entry_mask]
        print(f"After handling overlapping date ranges: {len(filtered_df)} rows")

    # Add back required symbols if any
    if required_symbols:
        print(f"Adding required symbols: {required_symbols}")
        filtered_df = pd.concat([filtered_df, required_rows]).drop_duplicates()
        print(f"After adding required symbols: {len(filtered_df)} rows")

    # Drop and recreate the symbols table without primary key constraint
    symbols_table = config['tiingo']['symbols_table']
    print(f"Dropping and recreating {symbols_table} table...")

    con.execute(f"DROP TABLE IF EXISTS {symbols_table}")
    con.execute(f"""
    CREATE TABLE {symbols_table} (
        symbol VARCHAR, 
        exchange VARCHAR, 
        asset_type VARCHAR, 
        price_currency VARCHAR, 
        start_date DATE, 
        end_date DATE
    )
    """)

    # Insert the data
    col = get_sql_column_provider('tiingo')
    con.execute(f"""
    INSERT INTO {symbols_table} BY NAME
    (SELECT {col('symbol')},
            {col('exchange')},
            {col('asset_type')},
            {col('price_currency')},
            start_date,
            end_date
      FROM filtered_df)""")

    print(f"Successfully inserted {len(filtered_df)} rows into {symbols_table}")

# Now, fix assets misclassified as stocks, but are really ETFs

# Use DuckDB to read contents of /data/mallard/etfs_equity.csv into a dataframe
# This is from eftdb.com. Free signup and go to leveraged equities and export to CSV. It gives you all equities in the
# file, ~3000. This is ~1700 less than Tiingo, but good for extra labeling.
eftdb_equities = duckdb.read_csv('/data/mallard/etfs_equity.csv').df()

with duckdb.connect(db_file) as con:
    max_end_date = con.execute(f"""
            SELECT MAX(end_date) 
            FROM {symbols_table}
            """).fetchone()[0]

with duckdb.connect(db_file) as con:
    tiingo_active_symbols = con.execute(f"""
    SELECT * FROM {symbols_table}
    WHERE price_currency = 'USD' and end_date = '{max_end_date}'
    """).df()

tiingo_stocks = tiingo_active_symbols[tiingo_active_symbols['asset_type'] == 'Stock']
misclassified_etfs = tiingo_stocks[tiingo_stocks['symbol'].isin(eftdb_equities['Symbol'])]
# Format the symbols as a comma-separated list of quoted values
symbols_list = ", ".join(f"'{symbol}'" for symbol in misclassified_etfs['symbol'])

with duckdb.connect(db_file) as con:
    # Update tiingo_symbols to set the asset_type to ETF if the symbol is in misclassified_etfs['symbol']
    missed_etfs = con.execute(f"""
    UPDATE {symbols_table}
    SET asset_type = 'ETF'
    WHERE price_currency = 'USD' AND end_date = '{max_end_date}' AND symbol IN ({symbols_list}) 
    """)

print(
    "supported_tickers are loaded. Please verify with the examples/verification.ipynb notebook. Then run fundamentals.py")

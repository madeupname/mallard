# Script to build a starting universe of tickers from Tiingo's supported_tickers.csv file
# Applies filters to data per config.ini.
# NOTE: drops/creates the table every time to ensure it contains exactly what is specified in config file.
import configparser
import logging
import os
import requests
import zipfile

import duckdb

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
    tickers = con.read_csv(supported_tickers_csv).filter('ticker IS NOT NULL')
    print("Extracted supported_tickers.csv")

    # Filter symbols per config
    ticker_requirements = config['DEFAULT']['ticker_requirements'].split(",")
    msg = f"Filtering tickers with requirements: {ticker_requirements}"
    print(msg)
    logger.info(msg)

    if ticker_requirements:
        if 'exchange' in ticker_requirements:
            tickers = tickers.filter("exchange IS NOT NULL")
        if 'stock' in ticker_requirements:
            tickers = tickers.filter("assetType = 'Stock'")
        if 'usd' in ticker_requirements:
            tickers = tickers.filter("priceCurrency = 'USD'")
        if 'dates' in ticker_requirements:
            tickers = tickers.filter("startDate IS NOT NULL AND endDate IS NOT NULL")
        if '6days' in ticker_requirements:
            tickers = tickers.filter("endDate - startDate > 5")
        if 'nocolon' in ticker_requirements:
            tickers = tickers.filter("ticker NOT LIKE '%:%'")
    # If there are multiple rows in tickers with the same value for ticker, we only want to keep the ones with the most
    # recent endDate column.
    # Create a query on the filtered tickers relation get distinct tickers with the most recent endDate
    latest_tickers = con.sql("SELECT DISTINCT ON(ticker) * FROM tickers ORDER BY ticker, endDate DESC")
    symbols_table = config['tiingo']['symbols_table']
    col = get_sql_column_provider('tiingo')
    # Truncate symbols_table
    con.execute(f"DELETE FROM {symbols_table}")
    con.execute(f"""
    INSERT INTO {symbols_table} BY NAME
    (SELECT {col('symbol')},
            {col('exchange')},
            {col('asset_type')},
            {col('price_currency')},
            {col('start_date')},
            {col('end_date')}
      FROM latest_tickers)""")
print(
    "supported_tickers are loaded. Please verify with the examples/verification.ipynb notebook. Then run fundamentals.py")
# Next step in filtering process is limiting to symbols with fundamental data: fundamentals.py

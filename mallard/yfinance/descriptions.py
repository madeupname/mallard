import configparser
import os
import time
import traceback

import duckdb
import yfinance as yf
from duckdb import IOException

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']

# Select vendor_symbol_id from fundamentals meta table where is_active = true
with duckdb.connect(db_file) as con:
    query = f"""
    SELECT vendor_symbol_id, symbol
    FROM {config['tiingo']['fundamentals_meta_table']}
    WHERE is_active = true AND vendor_symbol_id NOT IN (
        SELECT vendor_symbol_id
        FROM yf_info
    )
    """
    vendor_symbol_ids = con.execute(query).fetchall()


# Create yf_company_description table: vendor_symbol_id, symbol, description
with duckdb.connect(db_file) as con:
    query = f"""
    CREATE TABLE IF NOT EXISTS yf_info (
        vendor_symbol_id VARCHAR PRIMARY KEY,
        symbol VARCHAR,
        description VARCHAR)
    """
    con.execute(query)

print(f"Fetching descriptions for {len(vendor_symbol_ids)} symbols.")

for vendor_symbol_id, symbol in vendor_symbol_ids:
    try:
        description = yf.Ticker(symbol).info['longBusinessSummary']
        with duckdb.connect(db_file) as con:
            query = f"""
            INSERT OR REPLACE INTO yf_info
            VALUES (?, ?, ?)
            """
            con.execute(query, (vendor_symbol_id, symbol, description))
            # Sleep for 3 seconds to avoid rate limiting
            time.sleep(3)
    except IOException as e:
        print(f"Can't connect to database, exiting.")
        print(e)
        traceback.print_exc()
        break
    except Exception as e:
        print(f"Failed to get description for {symbol} / {vendor_symbol_id}")
        print(e)

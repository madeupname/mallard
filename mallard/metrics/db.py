import configparser
import os

import duckdb

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

# Get the path to the database file
db_file = config['DEFAULT']['db_file']
metrics = config['tiingo']['metrics'].split(",")
fundamental_metrics_table = config['tiingo']['fundamental_metrics_table']

with duckdb.connect(db_file) as con:
    # Create daily_metrics table with primary key on vendor_symbol_id and date
    print("Creating daily_metrics table if it doesn't exist.")
    con.execute("""
    CREATE TABLE IF NOT EXISTS daily_metrics (
        vendor_symbol_id VARCHAR,
        symbol VARCHAR,
        date DATE,                
        PRIMARY KEY (vendor_symbol_id, date))    
    """)
    # Create fundamental_metrics table with primary key on vendor_symbol_id and date
    # This is structured like the Tiingo reported and fundamentals tables.
    print("Creating fundamental_metrics table if it doesn't exist.")
    con.execute(f"""
    CREATE OR REPLACE TABLE {fundamental_metrics_table} (
        vendor_symbol_id VARCHAR,
        symbol VARCHAR,
        date DATE,
        year INT,
        quarter INT,
        PRIMARY KEY (vendor_symbol_id, date, year, quarter))
    """)
    # Get the directory this file is in.
    if 'adtval' in metrics:
        print("Creating columns in daily metrics for avg. daily trading value if they don't exist.")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS avg_daily_trading_value DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS has_min_trading_value DOUBLE")
        dir = os.path.dirname(os.path.realpath(__file__))
        con.execute(f"""CREATE OR REPLACE TABLE inflation AS FROM '{os.path.join(dir, "inflation.csv")}'""")
    if 'macd' in metrics:
        print("Creating columns in daily metrics for MACD if they don't exist.")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_signal DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_hist DOUBLE")
    if 'roic' in metrics:
        print("Creating columns in fundamental metrics for ROIC if they don't exist.")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS ebit_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS taxExp_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS nopat DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS roic DOUBLE")

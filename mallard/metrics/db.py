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
fundamentals_amended_distinct_table = config['tiingo']['fundamentals_amended_distinct_table']

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
    CREATE TABLE IF NOT EXISTS {fundamental_metrics_table} (
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
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS adtval DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS has_min_trading_value DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS has_min_adtval DOUBLE")

        dir = os.path.dirname(os.path.realpath(__file__))
        con.execute(f"""CREATE OR REPLACE TABLE inflation AS FROM '{os.path.join(dir, "inflation.csv")}'""")
    if 'macd' in metrics:
        print("Creating columns in daily metrics for MACD (including weekly) if they don't exist.")
        query = """
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ema_12 DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ema_26 DOUBLE;
                
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_signal DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_hist DOUBLE;
        
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_open DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_signal_open DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_hist_open DOUBLE;
        
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS weekly_macd DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS weekly_macd_signal DOUBLE;
        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS weekly_macd_hist DOUBLE;
        """
        con.execute(query)
    if 'roic' in metrics:
        print("Creating columns in fundamental metrics for ROIC if they don't exist.")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS ebit_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS taxExp_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS nopat DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS roic DOUBLE")
    if 'margins' in metrics or 'rank' in metrics:
        print("Creating columns in fundamental metrics for margins if they don't exist.")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS grossProfit_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS opinc_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS netinc_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS revenue_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS gross_margin_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS operating_margin_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS net_margin_ttm DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS freeCashFlow_ttm DOUBLE")
    if 'rank' in metrics:
        print("Creating columns in daily metrics for rank if they don't exist.")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS grossProfit_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS opinc_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS netinc_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS revenue_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS gross_margin_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS operating_margin_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS net_margin_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS freeCashFlow_ttm_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS pe_ratio_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS ebitda_ttm DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS enterprise_multiple DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS enterprise_multiple_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS currentRatio_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS debt_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS debtEquity_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS piotroskiFScore_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS roe_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS grossProfit_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS opinc_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS netinc_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS revenue_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS gross_margin_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS operating_margin_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS net_margin_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS freeCashFlow_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS currentRatio_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS debt_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS debtEquity_trend_slope_pct_rank DOUBLE")
        con.execute(f"ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS roe_trend_slope_pct_rank DOUBLE")

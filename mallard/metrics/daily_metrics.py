import configparser
import os

import duckdb
import polars as pl
import talib

from mallard.tiingo.eod import logger

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']
fundamentals_reported_table = config['tiingo']['fundamentals_reported_table']


def update_macd(vendor_symbol_id: str, duckdb_con: duckdb.DuckDBPyConnection, start_date='1990-06-01', end_date=None):
    """
    Updates the daily_metrics table with MACD indicators from prices in the tiingo_eod table.
    Since DuckDB cannot compute EMAs directly, TA-Lib is used to calculate the MACD columns.
    This is meant to be run in a multithreaded processes, hence the DB connection requirement.

    Args:
    vendor_symbol_id (str): The vendor symbol ID to calculate MACD for.
    duckdb_con (duckdb.DuckDBPyConnection): A connection object to the DuckDB database.
    start_date (str): Start date for the MACD calculation.
    end_date (str): End date for the MACD calculation; if None, uses the latest available date.
    """
    if duckdb_con is None:
        logger.error("update_macd: database connection is not provided.")
        return 'fail'

    with duckdb_con.cursor() as con:
        # Fetch the price data
        sql_query = f"""
        SELECT date, adj_close
        FROM {eod_table}
        WHERE vendor_symbol_id = '{vendor_symbol_id}' 
            AND date >= '{start_date}' 
            {f"AND date <= '{end_date}'" if end_date else ""}
        ORDER BY date ASC;
        """
        price_data = con.execute(sql_query).pl()

        # Check if data is sufficient
        if price_data.is_empty():
            logger.info(f"update_macd: No data available for params: {vendor_symbol_id}, {start_date}, {end_date}.")
            return 'skip'

        # Calculate MACD using TA-Lib
        macd, macdsignal, macdhist = talib.MACD(price_data['adj_close'].to_numpy(),
                                                fastperiod=12, slowperiod=26, signalperiod=9)

        # Prepare DataFrame to insert results
        results = pl.DataFrame({
            'vendor_symbol_id': vendor_symbol_id,
            'date': price_data['date'],
            'macd': macd,
            'macd_signal': macdsignal,
            'macd_hist': macdhist
        })

        # Clean data (remove NaNs due to MACD calculation starting period)
        results = results.filter(
            (pl.col('macd').is_not_nan()) &
            (pl.col('macd_signal').is_not_nan()) &
            (pl.col('macd_hist').is_not_nan())
        )

        # Insert/update the daily_metrics table
        con.execute("""
            INSERT OR REPLACE INTO daily_metrics (vendor_symbol_id, date, macd, macd_signal, macd_hist)
            FROM results
        """)


def avg_daily_trading_value(duckdb_con, value=20000000.0, start_date: str = '1990-06-01'):
    """Updates the avg_daily_trading_value column of the daily_metrics table.
    Average of the close * volume of the previous 5 trading days, so it can be used the day of without lookahead bias.
    If new data is added to the EOD table, it only calculates the missing values.
    Requires a DuckDB connection, but NOT meant to be run in threadpool executor."""
    with duckdb_con.cursor() as con:
        # Insert or update avg_daily_trading_value in daily_metrics
        con.execute(f"""
        INSERT INTO daily_metrics (vendor_symbol_id, symbol, date, avg_daily_trading_value)
        SELECT
            vendor_symbol_id,
            symbol,
            date,
            AVG(volume * close) OVER (
                PARTITION BY vendor_symbol_id
                ORDER BY date
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_daily_trading_value
        FROM
            {eod_table}
        WHERE
            date > (SELECT COALESCE(MAX(date) - INTERVAL '5 DAY', '{start_date}') FROM daily_metrics WHERE vendor_symbol_id = {eod_table}.vendor_symbol_id)
        ON CONFLICT DO UPDATE SET avg_daily_trading_value = EXCLUDED.avg_daily_trading_value
        """)
    with duckdb.connect(db_file) as con:
        inflation_df = con.execute("SELECT * FROM inflation ORDER BY year").pl()
        daily_metrics_df = con.execute("SELECT vendor_symbol_id, date, avg_daily_trading_value FROM daily_metrics").pl()

    # Calculate cumulative inflation multiplier
    inflation_df = inflation_df.with_columns(
        (1 + inflation_df['inflation_rate'] / 100).cum_prod().alias('cumulative_inflation')
    )

    # Normalize to the latest year's inflation (make latest year's multiplier = 1)
    latest_multiplier = \
        inflation_df.filter(pl.col('year') == inflation_df['year'].max()).select(
            pl.col('cumulative_inflation')).to_numpy()[0]
    inflation_df = inflation_df.with_columns(
        (pl.col('cumulative_inflation') / latest_multiplier).alias('adjusted_multiplier'),
        pl.col('year').cast(pl.Int32)  # Ensure 'year' is cast to i32 to match daily_metrics_df
    )

    # Ensure 'year' column in daily_metrics_df is i32, and extract year from date
    daily_metrics_df = daily_metrics_df.with_columns(
        pl.col('date').dt.year().cast(pl.Int32).alias('year')  # Casting 'year' extracted from 'date' to i32
    )

    # Merge adjusted_multiplier based on year
    result_df = daily_metrics_df.join(
        inflation_df.select(['year', 'adjusted_multiplier']),
        on='year',
        how='left'
    )

    # Calculate whether avg_daily_trading_value is above the inflation-adjusted threshold
    result_df = result_df.with_columns(
        (pl.col('avg_daily_trading_value') >= value * pl.col('adjusted_multiplier')).alias('has_min_trading_value')
    )

    # Insert has_min_trading_value into daily_metrics
    with duckdb_con.cursor() as con:
        con.execute(f"""
        INSERT OR REPLACE INTO daily_metrics (vendor_symbol_id, date, has_min_trading_value)
        FROM result_df
        """)

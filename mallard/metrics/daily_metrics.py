import configparser
import os
import traceback
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import talib
from legitindicators import roofing_filter

from mallard.metrics.bollinger import bollinger_sma, bollinger_ema
from mallard.tiingo.tiingo_util import logger

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']
fundamentals_reported_table = config['tiingo']['fundamentals_reported_table']
fundamentals_daily_table = config['tiingo']['fundamentals_daily_table']
fundamentals_meta_table = config['tiingo']['fundamentals_meta_table']


def initialize_daily_metrics():
    """For each row in eod_table, create a row in daily_metrics with vendor_symbol_id and date if it doesn't already
    exist"""
    with duckdb.connect(db_file) as con:
        con.execute(f"""
        INSERT OR IGNORE INTO daily_metrics (vendor_symbol_id, symbol, date)
        SELECT vendor_symbol_id, symbol, date
        FROM {eod_table}
        """)


def bulk_past_returns_calculation_extended(duckdb_con, start_date='2000-01-01'):
    """
    Calculate past returns and basic daily returns using window functions - each return period starts as soon as sufficient history exists.
    Extended to include return_0, return_close_close, and spy_return_0.
    """
    try:
        with duckdb_con.cursor() as con:
            print("Calculating past returns and basic daily returns using optimized window functions...")
            
            # Single query using window functions - calculate each return when sufficient history exists
            optimized_returns_query = f"""
            WITH stocks_with_history AS (
                SELECT 
                    vendor_symbol_id,
                    MIN(date) as first_date
                FROM tiingo_eod
                WHERE date >= '{start_date}'
                GROUP BY vendor_symbol_id
                HAVING COUNT(*) >= 2  -- Need at least 2 days for return_close_close (shortest requirement)
            ),
            price_data_with_lags AS (
                SELECT 
                    te.vendor_symbol_id,
                    te.date,
                    te.adj_close,
                    te.open,
                    te.close,
                    te.symbol,
                    -- Use window functions to get lagged prices
                    LAG(te.adj_close, 1) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1d_ago,
                    LAG(te.adj_close, 5) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1w_ago_approx,
                    LAG(te.adj_close, 21) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1m_ago_approx,
                    LAG(te.adj_close, 126) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_6m_ago_approx,
                    LAG(te.adj_close, 252) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1y_ago_approx,
                    -- Count how many trading days we have so far for this stock
                    ROW_NUMBER() OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS trading_day_number
                FROM tiingo_eod te
                JOIN stocks_with_history swh ON te.vendor_symbol_id = swh.vendor_symbol_id
                WHERE te.date >= swh.first_date
                  AND te.date >= '{start_date}'
            ),
            missing_returns_with_prices AS (
                SELECT 
                    dm.vendor_symbol_id,
                    dm.date,
                    pdwl.adj_close as current_price,
                    pdwl.open,
                    pdwl.close,
                    pdwl.symbol,
                    pdwl.price_1d_ago,
                    pdwl.price_1w_ago_approx,
                    pdwl.price_1m_ago_approx,
                    pdwl.price_6m_ago_approx,
                    pdwl.price_1y_ago_approx,
                    pdwl.trading_day_number
                FROM daily_metrics dm
                JOIN price_data_with_lags pdwl ON dm.vendor_symbol_id = pdwl.vendor_symbol_id 
                    AND dm.date = pdwl.date
                WHERE dm.date >= '{start_date}'
                  AND (dm.return_1w IS NULL OR dm.return_1m IS NULL OR 
                       dm.return_6m IS NULL OR dm.return_1y IS NULL OR
                       dm.return_0 IS NULL OR dm.return_close_close IS NULL OR dm.spy_return_0 IS NULL)
            ),
            calculated_returns AS (
                SELECT 
                    vendor_symbol_id,
                    date,
                    symbol,
                    -- Calculate basic daily returns
                    CASE 
                        WHEN open IS NOT NULL AND close IS NOT NULL AND open > 0 
                        THEN (close - open) / open 
                        ELSE NULL 
                    END AS return_0,
                    -- Calculate close-to-close return (same as 1-day return)
                    CASE 
                        WHEN trading_day_number >= 2 AND price_1d_ago IS NOT NULL AND price_1d_ago > 0 
                        THEN (current_price / price_1d_ago) - 1 
                        ELSE NULL 
                    END AS return_close_close,
                    -- Calculate 1w return if we have at least 6 trading days
                    CASE 
                        WHEN trading_day_number >= 6 AND price_1w_ago_approx IS NOT NULL AND price_1w_ago_approx > 0 
                        THEN (current_price / price_1w_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_1w,
                    -- Calculate 1m return if we have at least 22 trading days
                    CASE 
                        WHEN trading_day_number >= 22 AND price_1m_ago_approx IS NOT NULL AND price_1m_ago_approx > 0 
                        THEN (current_price / price_1m_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_1m,
                    -- Calculate 6m return if we have at least 127 trading days
                    CASE 
                        WHEN trading_day_number >= 127 AND price_6m_ago_approx IS NOT NULL AND price_6m_ago_approx > 0 
                        THEN (current_price / price_6m_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_6m,
                    -- Calculate 1y return if we have at least 253 trading days
                    CASE 
                        WHEN trading_day_number >= 253 AND price_1y_ago_approx IS NOT NULL AND price_1y_ago_approx > 0 
                        THEN (current_price / price_1y_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_1y
                FROM missing_returns_with_prices
            ),
            spy_returns AS (
                SELECT 
                    date,
                    return_0 as spy_return_0
                FROM calculated_returns 
                WHERE symbol = 'SPY'
            )
            UPDATE daily_metrics
            SET 
                return_1w = COALESCE(cr.return_1w, daily_metrics.return_1w),
                return_1m = COALESCE(cr.return_1m, daily_metrics.return_1m),
                return_6m = COALESCE(cr.return_6m, daily_metrics.return_6m),
                return_1y = COALESCE(cr.return_1y, daily_metrics.return_1y),
                return_0 = COALESCE(cr.return_0, daily_metrics.return_0),
                return_close_close = COALESCE(cr.return_close_close, daily_metrics.return_close_close),
                spy_return_0 = COALESCE(sr.spy_return_0, daily_metrics.spy_return_0)
            FROM calculated_returns cr
            LEFT JOIN spy_returns sr ON cr.date = sr.date
            WHERE daily_metrics.vendor_symbol_id = cr.vendor_symbol_id 
              AND daily_metrics.date = cr.date
            """
            
            print("Executing optimized returns calculation with basic daily returns...")
            con.execute(optimized_returns_query)
            
            # Count what we updated
            count_query = f"""
            SELECT COUNT(DISTINCT vendor_symbol_id) as updated_stocks
            FROM daily_metrics 
            WHERE date >= '{start_date}' 
              AND (return_1w IS NOT NULL OR return_1m IS NOT NULL OR 
                   return_6m IS NOT NULL OR return_1y IS NOT NULL OR
                   return_0 IS NOT NULL OR return_close_close IS NOT NULL OR spy_return_0 IS NOT NULL)
            """
            updated_stocks = con.execute(count_query).fetchone()[0]
            
            print(f"Successfully updated past returns and basic daily returns for {updated_stocks:,} stocks")
            return {'count_success': updated_stocks, 'count_skip': 0, 'count_fail': 0}
            
    except Exception as e:
        print(f"Error in optimized past returns calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_past_returns_calculation(duckdb_con, start_date='2000-01-01'):
    """
    Calculate past returns using window functions - each return period starts as soon as sufficient history exists.
    """
    try:
        with duckdb_con.cursor() as con:
            print("Calculating past returns using optimized window functions...")
            
            # Single query using window functions - calculate each return when sufficient history exists
            optimized_returns_query = f"""
            WITH stocks_with_history AS (
                SELECT 
                    vendor_symbol_id,
                    MIN(date) as first_date
                FROM tiingo_eod
                WHERE date >= '{start_date}'
                GROUP BY vendor_symbol_id
                HAVING COUNT(*) >= 5  -- Need at least 5 days for 1w returns
            ),
            price_data_with_lags AS (
                SELECT 
                    te.vendor_symbol_id,
                    te.date,
                    te.adj_close,
                    -- Use window functions to get lagged prices
                    LAG(te.adj_close, 5) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1w_ago_approx,
                    LAG(te.adj_close, 21) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1m_ago_approx,
                    LAG(te.adj_close, 126) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_6m_ago_approx,
                    LAG(te.adj_close, 252) OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS price_1y_ago_approx,
                    -- Count how many trading days we have so far for this stock
                    ROW_NUMBER() OVER (PARTITION BY te.vendor_symbol_id ORDER BY te.date) AS trading_day_number
                FROM tiingo_eod te
                JOIN stocks_with_history swh ON te.vendor_symbol_id = swh.vendor_symbol_id
                WHERE te.date >= swh.first_date
                  AND te.date >= '{start_date}'
            ),
            missing_returns_with_prices AS (
                SELECT 
                    dm.vendor_symbol_id,
                    dm.date,
                    pdwl.adj_close as current_price,
                    pdwl.price_1w_ago_approx,
                    pdwl.price_1m_ago_approx,
                    pdwl.price_6m_ago_approx,
                    pdwl.price_1y_ago_approx,
                    pdwl.trading_day_number
                FROM daily_metrics dm
                JOIN price_data_with_lags pdwl ON dm.vendor_symbol_id = pdwl.vendor_symbol_id 
                    AND dm.date = pdwl.date
                WHERE dm.date >= '{start_date}'
                  AND (dm.return_1w IS NULL OR dm.return_1m IS NULL OR 
                       dm.return_6m IS NULL OR dm.return_1y IS NULL)
            ),
            calculated_returns AS (
                SELECT 
                    vendor_symbol_id,
                    date,
                    -- Calculate 1w return if we have at least 6 trading days
                    CASE 
                        WHEN trading_day_number >= 6 AND price_1w_ago_approx IS NOT NULL AND price_1w_ago_approx > 0 
                        THEN (current_price / price_1w_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_1w,
                    -- Calculate 1m return if we have at least 22 trading days
                    CASE 
                        WHEN trading_day_number >= 22 AND price_1m_ago_approx IS NOT NULL AND price_1m_ago_approx > 0 
                        THEN (current_price / price_1m_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_1m,
                    -- Calculate 6m return if we have at least 127 trading days
                    CASE 
                        WHEN trading_day_number >= 127 AND price_6m_ago_approx IS NOT NULL AND price_6m_ago_approx > 0 
                        THEN (current_price / price_6m_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_6m,
                    -- Calculate 1y return if we have at least 253 trading days
                    CASE 
                        WHEN trading_day_number >= 253 AND price_1y_ago_approx IS NOT NULL AND price_1y_ago_approx > 0 
                        THEN (current_price / price_1y_ago_approx) - 1 
                        ELSE NULL 
                    END AS return_1y
                FROM missing_returns_with_prices
            )
            UPDATE daily_metrics
            SET 
                return_1w = COALESCE(cr.return_1w, daily_metrics.return_1w),
                return_1m = COALESCE(cr.return_1m, daily_metrics.return_1m),
                return_6m = COALESCE(cr.return_6m, daily_metrics.return_6m),
                return_1y = COALESCE(cr.return_1y, daily_metrics.return_1y)
            FROM calculated_returns cr
            WHERE daily_metrics.vendor_symbol_id = cr.vendor_symbol_id 
              AND daily_metrics.date = cr.date
            """
            
            print("Executing optimized returns calculation with proper timing...")
            con.execute(optimized_returns_query)
            
            # Count what we updated
            count_query = f"""
            SELECT COUNT(DISTINCT vendor_symbol_id) as updated_stocks
            FROM daily_metrics 
            WHERE date >= '{start_date}' 
              AND (return_1w IS NOT NULL OR return_1m IS NOT NULL OR 
                   return_6m IS NOT NULL OR return_1y IS NOT NULL)
            """
            updated_stocks = con.execute(count_query).fetchone()[0]
            
            print(f"Successfully updated past returns for {updated_stocks:,} stocks with proper timing")
            return {'count_success': updated_stocks, 'count_skip': 0, 'count_fail': 0}
            
    except Exception as e:
        print(f"Error in optimized past returns calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def process_past_returns_batch(con, symbol_ids, batch_start_date, batch_end_date):
    """
    Process a batch of symbols for past returns calculations.
    
    Args:
        con: Database connection cursor
        symbol_ids: List of vendor_symbol_ids to process
        batch_start_date: Earliest date in this batch
        batch_end_date: Latest date in this batch
        
    Returns:
        dict with symbols processed
    """
    try:
        print(f"  Processing batch of {len(symbol_ids)} symbols from {batch_start_date} to {batch_end_date}")
        
        # Convert symbol_ids to SQL-safe format
        symbol_list = ','.join([f"'{sid}'" for sid in symbol_ids])
        
        past_returns_query = f"""
        WITH PriceData AS (
            SELECT 
                te.vendor_symbol_id,
                te.date,
                te.adj_close
            FROM tiingo_eod te
            WHERE te.vendor_symbol_id IN ({symbol_list})
              -- Get extra lookback data (1 year + buffer)
              AND te.date >= DATE '{batch_start_date}' - INTERVAL '400 days'
              AND te.date <= DATE '{batch_end_date}'
            ORDER BY te.vendor_symbol_id, te.date
        ),
        
        ReturnsCalculation AS (
            SELECT 
                p1.vendor_symbol_id,
                p1.date,
                p1.adj_close AS current_price,
                
                -- Find closest trading day for each lookback period
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '7 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_1w_ago,
                
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '30 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_1m_ago,
                
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '182 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_6m_ago,
                
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '365 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_1y_ago
                
            FROM PriceData p1
            WHERE p1.date BETWEEN DATE '{batch_start_date}' AND DATE '{batch_end_date}'
              AND p1.vendor_symbol_id IN ({symbol_list})
        ),
        
        FinalReturns AS (
            SELECT 
                vendor_symbol_id,
                date,
                CASE 
                    WHEN price_1w_ago IS NOT NULL AND price_1w_ago > 0 
                    THEN (current_price / price_1w_ago) - 1 
                    ELSE NULL 
                END AS return_1w,
                CASE 
                    WHEN price_1m_ago IS NOT NULL AND price_1m_ago > 0 
                    THEN (current_price / price_1m_ago) - 1 
                    ELSE NULL 
                END AS return_1m,
                CASE 
                    WHEN price_6m_ago IS NOT NULL AND price_6m_ago > 0 
                    THEN (current_price / price_6m_ago) - 1 
                    ELSE NULL 
                END AS return_6m,
                CASE 
                    WHEN price_1y_ago IS NOT NULL AND price_1y_ago > 0 
                    THEN (current_price / price_1y_ago) - 1 
                    ELSE NULL 
                END AS return_1y
            FROM ReturnsCalculation
        )
        
        UPDATE daily_metrics
        SET 
            return_1w = COALESCE(fr.return_1w, daily_metrics.return_1w),
            return_1m = COALESCE(fr.return_1m, daily_metrics.return_1m),
            return_6m = COALESCE(fr.return_6m, daily_metrics.return_6m),
            return_1y = COALESCE(fr.return_1y, daily_metrics.return_1y)
        FROM FinalReturns fr
        WHERE daily_metrics.vendor_symbol_id = fr.vendor_symbol_id 
          AND daily_metrics.date = fr.date
          AND daily_metrics.vendor_symbol_id IN ({symbol_list})
        """
        
        # Execute the query
        con.execute(past_returns_query)
        
        return {'symbols': set(symbol_ids)}
        
    except Exception as e:
        print(f"Error processing batch {batch_start_date} to {batch_end_date} for {len(symbol_ids)} symbols: {e}")
        return {'symbols': set()}


def process_past_returns_chunk_for_symbols(con, chunk_start, chunk_end, symbol_ids):
    """
    Process a single chunk of past returns calculations for specific symbols.
    
    Args:
        con: Database connection cursor
        chunk_start: Start date for this chunk
        chunk_end: End date for this chunk
        symbol_ids: List of vendor_symbol_ids to process
        
    Returns:
        dict with symbols processed
    """
    try:
        # Convert symbol_ids to SQL-safe format
        symbol_list = ','.join([f"'{sid}'" for sid in symbol_ids])
        
        past_returns_query = f"""
        WITH PriceData AS (
            SELECT 
                te.vendor_symbol_id,
                te.date,
                te.adj_close
            FROM tiingo_eod te
            WHERE te.vendor_symbol_id IN ({symbol_list})
            -- Need extra lookback for the longest period (1 year + buffer)
              AND te.date >= DATE '{chunk_start}' - INTERVAL '400 days'
              AND te.date <= DATE '{chunk_end}'
            ORDER BY te.vendor_symbol_id, te.date
        ),
        
        ReturnsCalculation AS (
            SELECT 
                p1.vendor_symbol_id,
                p1.date,
                p1.adj_close AS current_price,
                
                -- Find closest trading day for each lookback period
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '7 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_1w_ago,
                
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '30 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_1m_ago,
                
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '182 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_6m_ago,
                
                (SELECT p2.adj_close 
                 FROM PriceData p2 
                 WHERE p2.vendor_symbol_id = p1.vendor_symbol_id 
                   AND p2.date <= p1.date - INTERVAL '365 days'
                 ORDER BY p2.date DESC 
                 LIMIT 1) AS price_1y_ago
                
            FROM PriceData p1
            WHERE p1.date BETWEEN DATE '{chunk_start}' AND DATE '{chunk_end}'
              AND p1.vendor_symbol_id IN ({symbol_list})
        ),
        
        FinalReturns AS (
            SELECT 
                vendor_symbol_id,
                date,
                CASE 
                    WHEN price_1w_ago IS NOT NULL AND price_1w_ago > 0 
                    THEN (current_price / price_1w_ago) - 1 
                    ELSE NULL 
                END AS return_1w,
                CASE 
                    WHEN price_1m_ago IS NOT NULL AND price_1m_ago > 0 
                    THEN (current_price / price_1m_ago) - 1 
                    ELSE NULL 
                END AS return_1m,
                CASE 
                    WHEN price_6m_ago IS NOT NULL AND price_6m_ago > 0 
                    THEN (current_price / price_6m_ago) - 1 
                    ELSE NULL 
                END AS return_6m,
                CASE 
                    WHEN price_1y_ago IS NOT NULL AND price_1y_ago > 0 
                    THEN (current_price / price_1y_ago) - 1 
                    ELSE NULL 
                END AS return_1y
            FROM ReturnsCalculation
        )
        
        UPDATE daily_metrics
        SET 
            return_1w = fr.return_1w,
            return_1m = fr.return_1m,
            return_6m = fr.return_6m,
            return_1y = fr.return_1y
        FROM FinalReturns fr
        WHERE daily_metrics.vendor_symbol_id = fr.vendor_symbol_id 
          AND daily_metrics.date = fr.date
          AND daily_metrics.vendor_symbol_id IN ({symbol_list})
          AND (daily_metrics.return_1w IS NULL OR daily_metrics.return_1m IS NULL OR 
               daily_metrics.return_6m IS NULL OR daily_metrics.return_1y IS NULL)
        """
        
        # Execute the query
        con.execute(past_returns_query)
        
        return {'symbols': set(symbol_ids)}
        
    except Exception as e:
        print(f"Error processing chunk {chunk_start} to {chunk_end} for {len(symbol_ids)} symbols: {e}")
        return {'symbols': set()}


def calculate_period_return(vendor_symbol_id, column_name, days, con, start_date):
    """
    Calculates returns for a specific lookback period.
    
    Args:
        vendor_symbol_id: Symbol identifier
        column_name: Column to update (e.g., 'return_1w')
        days: Calendar days to look back
        con: Database connection cursor
        start_date: Starting date for calculations when no previous data exists
        
    Returns:
        str: 'success' or 'skip'
    """
    # Check for existing data
    last_date_query = f"""
        SELECT MAX(date)
        FROM daily_metrics
        WHERE vendor_symbol_id = ?
          AND {column_name} IS NOT NULL
    """
    last_date = con.execute(last_date_query, [vendor_symbol_id]).fetchone()[0]
    
    # If we have previous calculations, check if there's any new data
    if last_date is not None:
        # Check if there are any new data points
        new_data_check_query = """
            SELECT EXISTS(
                SELECT 1
                FROM tiingo_eod
                WHERE vendor_symbol_id = ? 
                AND date > ?
            )
        """
        has_new_data = con.execute(new_data_check_query, [vendor_symbol_id, last_date]).fetchone()[0]

        if not has_new_data:
            # No new data, skip calculation
            return 'skip'
            
        # Only update from the last_date forward
        date_filter = f"AND date > DATE '{last_date}'"
    else:
        # No previous calculations, use start_date to limit the initial range
        date_filter = f"AND date >= DATE '{start_date}'"
    
    # Count the number of NULL values before the update
    count_nulls_before_query = f"""
        SELECT COUNT(*) 
        FROM daily_metrics 
        WHERE vendor_symbol_id = '{vendor_symbol_id}' 
        AND {column_name} IS NULL
        {date_filter}
    """
    nulls_before = con.execute(count_nulls_before_query).fetchone()[0]


    # Calculate returns using SQL to find the closest trading day before the lookback date
    # Using DuckDB 1.0 compatible date arithmetic
    update_query = f"""
    WITH dates AS (
        SELECT 
            date,
            adj_close
        FROM tiingo_eod
        WHERE vendor_symbol_id = '{vendor_symbol_id}'
        ORDER BY date
    ),
    
    lookback_dates AS (
        SELECT 
            d1.date AS current_date,
            d1.adj_close AS current_price,
            (
                SELECT MAX(d2.date) 
                FROM dates d2 
                WHERE d2.date <= (d1.date - INTERVAL '{days} days')
            ) AS prior_date
        FROM dates d1
    ),
    
    return_calc AS (
        SELECT 
            ld.current_date,
            ld.current_price,
            d.adj_close AS prior_price,
            (ld.current_price / NULLIF(d.adj_close, 0)) - 1 AS period_return
        FROM lookback_dates ld
        LEFT JOIN dates d ON ld.prior_date = d.date
        WHERE ld.prior_date IS NOT NULL  -- Only include where we found a valid prior date
    )
    
    UPDATE daily_metrics dm
    SET {column_name} = rc.period_return
    FROM return_calc rc
    WHERE 
        dm.vendor_symbol_id = '{vendor_symbol_id}'
        AND dm.date = rc.current_date
        {date_filter}
    """

    con.execute(update_query)

    # Count the number of NULL values after the update
    count_nulls_after_query = f"""
        SELECT COUNT(*) 
        FROM daily_metrics 
        WHERE vendor_symbol_id = '{vendor_symbol_id}' 
        AND {column_name} IS NULL
        {date_filter}
    """
    nulls_after = con.execute(count_nulls_after_query).fetchone()[0]

    # If fewer NULL values exist after the update, then rows were updated
    rows_updated = nulls_before - nulls_after

    if rows_updated <= 0:
        return 'skip'

    return 'success'


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    """Calculate rolling percentile rank of the current value within its window"""
    return series.rolling(window, min_periods=window//2).apply(
        lambda x: (x.rank().iloc[-1] - 1) / (len(x) - 1) if len(x) > 1 else np.nan,
        raw=False
    )

def bulk_regime_calculation(duckdb_con, start_date):
    """
    Calculate TLT/HYG regime metrics using proven pandas logic.
    Extracted from bulk_daily_return_calculation to separate concerns.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs regime calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (tlt_hyg_ratio IS NULL OR log_return_tlt_hyg IS NULL OR tlt_hyg_ratio_zscore_3m IS NULL OR
                   vol_tlt_1m IS NULL OR vol_hyg_1m IS NULL OR zscore_return_tlt_3m IS NULL OR zscore_return_hyg_3m IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs regime calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the rolling calculations (63 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=260)  # Same as original
            msg = f"Loading data from {lookback_date} for regime calculations"
            print(msg)
            logger.info(msg)

            # Load price data for TLT and HYG (copying exact query structure)
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.close,
                te.adj_close
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
              AND dm.symbol IN ('TLT', 'HYG')
            ORDER BY dm.symbol, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Extract TLT and HYG data for comprehensive metrics calculation (exact copy)
            tlt_data = df[df['symbol'] == 'TLT'][['date', 'close', 'adj_close']].copy()
            hyg_data = df[df['symbol'] == 'HYG'][['date', 'close', 'adj_close']].copy()
            
            # Calculate TLT/HYG metrics if both are available (exact copy)
            if not tlt_data.empty and not hyg_data.empty:
                # Calculate daily returns for TLT and HYG
                tlt_data['tlt_return'] = tlt_data['adj_close'].pct_change()
                hyg_data['hyg_return'] = hyg_data['adj_close'].pct_change()
                
                # Merge TLT and HYG data
                tlt_hyg_data = tlt_data.merge(hyg_data, on='date', how='inner', suffixes=('_tlt', '_hyg'))
                
                # Calculate TLT/HYG ratio
                tlt_hyg_data['tlt_hyg_ratio'] = tlt_hyg_data['close_tlt'] / tlt_hyg_data['close_hyg']
                
                # Calculate log return of the ratio
                tlt_hyg_data['log_return_tlt_hyg'] = np.log(
                    tlt_hyg_data['tlt_hyg_ratio'] / tlt_hyg_data['tlt_hyg_ratio'].shift(1)
                )
                
                # Calculate 3-month z-score of ratio (63 trading days)
                tlt_hyg_data['tlt_hyg_ratio_mean_3m'] = tlt_hyg_data['tlt_hyg_ratio'].rolling(63, min_periods=30).mean()
                tlt_hyg_data['tlt_hyg_ratio_std_3m'] = tlt_hyg_data['tlt_hyg_ratio'].rolling(63, min_periods=30).std()
                tlt_hyg_data['tlt_hyg_ratio_zscore_3m'] = np.where(
                    tlt_hyg_data['tlt_hyg_ratio_std_3m'] > 0,
                    (tlt_hyg_data['tlt_hyg_ratio'] - tlt_hyg_data['tlt_hyg_ratio_mean_3m']) / tlt_hyg_data['tlt_hyg_ratio_std_3m'],
                    np.nan
                )
                
                # Calculate 1-month volatility for TLT and HYG (21 trading days)
                tlt_hyg_data['vol_tlt_1m'] = tlt_hyg_data['tlt_return'].rolling(21, min_periods=10).std()
                tlt_hyg_data['vol_hyg_1m'] = tlt_hyg_data['hyg_return'].rolling(21, min_periods=10).std()
                
                # Calculate 3-month z-scores of TLT and HYG returns (63 trading days)
                tlt_hyg_data['tlt_return_mean_3m'] = tlt_hyg_data['tlt_return'].rolling(63, min_periods=30).mean()
                tlt_hyg_data['tlt_return_std_3m'] = tlt_hyg_data['tlt_return'].rolling(63, min_periods=30).std()
                tlt_hyg_data['zscore_return_tlt_3m'] = np.where(
                    tlt_hyg_data['tlt_return_std_3m'] > 0,
                    (tlt_hyg_data['tlt_return'] - tlt_hyg_data['tlt_return_mean_3m']) / tlt_hyg_data['tlt_return_std_3m'],
                    np.nan
                )
                
                tlt_hyg_data['hyg_return_mean_3m'] = tlt_hyg_data['hyg_return'].rolling(63, min_periods=30).mean()
                tlt_hyg_data['hyg_return_std_3m'] = tlt_hyg_data['hyg_return'].rolling(63, min_periods=30).std()
                tlt_hyg_data['zscore_return_hyg_3m'] = np.where(
                    tlt_hyg_data['hyg_return_std_3m'] > 0,
                    (tlt_hyg_data['hyg_return'] - tlt_hyg_data['hyg_return_mean_3m']) / tlt_hyg_data['hyg_return_std_3m'],
                    np.nan
                )
                
                # Keep only necessary columns for merging
                tlt_hyg_metrics = tlt_hyg_data[[
                    'date', 'tlt_hyg_ratio', 'log_return_tlt_hyg', 'tlt_hyg_ratio_zscore_3m',
                    'vol_tlt_1m', 'vol_hyg_1m', 'zscore_return_tlt_3m', 'zscore_return_hyg_3m'
                ]]
                
                # Filter to only update rows we need (those with dates >= earliest_date)
                update_metrics = tlt_hyg_metrics[tlt_hyg_metrics['date'] >= pd.to_datetime(earliest_date)].copy()
                
                if not update_metrics.empty:
                    print(f"Updating {len(update_metrics)} regime metric records")
                    
                    # Update database directly from DataFrame
                    columns_to_update = [
                        'tlt_hyg_ratio', 'log_return_tlt_hyg', 'tlt_hyg_ratio_zscore_3m',
                        'vol_tlt_1m', 'vol_hyg_1m', 'zscore_return_tlt_3m', 'zscore_return_hyg_3m'
                    ]
                    
                    # Update directly from DataFrame (broadcast to all symbols)
                    update_clause = ", ".join([f"{col} = update_metrics.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM update_metrics
                    WHERE daily_metrics.date = update_metrics.date
                    """
                    con.execute(update_query)
                    
                    # Count updated symbols
                    count_query = f"""
                    SELECT COUNT(DISTINCT vendor_symbol_id) FROM daily_metrics
                    WHERE date >= '{earliest_date}' 
                      AND tlt_hyg_ratio IS NOT NULL
                    """
                    updated_symbols = con.execute(count_query).fetchone()[0]
                    
                    msg = f"Successfully updated regime metrics for {updated_symbols} symbols"
                    print(msg)
                    logger.info(msg)
                    return {'count_success': updated_symbols, 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                # If TLT or HYG data is missing, log warning
                missing_symbols = []
                if tlt_data.empty:
                    missing_symbols.append('TLT')
                if hyg_data.empty:
                    missing_symbols.append('HYG')
                msg = f"WARNING: {', '.join(missing_symbols)} data not found. Regime metrics will be missing."
                print(msg)
                logger.warning(msg)
                return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}
                
    except Exception as e:
        msg = f"Error in bulk regime calculation: {e}"
        print(msg)
        logger.error(msg)
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_daily_return_calculation(duckdb_con, start_date):
    """
    Calculate daily return metrics for all stocks using vectorized operations.
    TLT/HYG regime metrics are now handled by bulk_regime_calculation().
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs daily return calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (return_0 IS NULL OR return_0_zscore_1m IS NULL OR return_close_close IS NULL OR 
                   spy_return_0 IS NULL OR spy_return_0_pctile_1y IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs daily return calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the rolling calculations (63 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=260)  # Increased for SPY percentile calculation
            msg = f"Loading data from {lookback_date} for daily return calculations"
            print(msg)
            logger.info(msg)

            # Load price data for all symbols including SPY
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.open,
                te.close,
                te.adj_close
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate intraday return (open to close)
            df['return_0'] = (df['close'] - df['open']) / df['open']
            
            # Calculate closing return (close to previous close)
            df['return_close_close'] = df.groupby('vsid')['adj_close'].pct_change()
            
            # Calculate the z-score of return_0 over 21 days (1 month)
            df['return_0_zscore_1m'] = df.groupby('vsid')['return_0'].transform(
                lambda x: (x - x.rolling(21, min_periods=10).mean()) / x.rolling(21, min_periods=10).std())
            
            # Extract SPY data and calculate SPY return percentiles
            spy_data = df[df['symbol'] == 'SPY'].copy()
            
            if not spy_data.empty:
                # Save SPY return_0 with the name spy_return_0
                spy_data['spy_return_0'] = spy_data['return_0']
                
                # Use the fixed rolling_percentile function
                spy_data['spy_return_0_pctile_1y'] = rolling_percentile(spy_data['return_0'], 252)
                
                # Keep only necessary columns from SPY data for merging
                spy_metrics = spy_data[['date', 'spy_return_0', 'spy_return_0_pctile_1y']]

                # Merge SPY metrics back to main dataframe
                df = df.merge(spy_metrics, on='date', how='left')
                
            else:
                # If SPY data is missing, add an empty column
                df['spy_return_0'] = np.nan
                df['spy_return_0_pctile_1y'] = np.nan
                msg = "WARNING: SPY data not found. SPY percentile metrics will be missing."
                print(msg)
                logger.warning(msg)
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update - now only individual stock and SPY metrics
                columns_to_update = [
                    'return_0', 'return_0_zscore_1m', 'return_close_close', 'spy_return_0', 'spy_return_0_pctile_1y'
                ]
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['return_0'], how='all')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} daily return records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        msg = f"Error in bulk daily return calculation: {e}"
        print(msg)
        logger.error(msg)
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def _process_daily_return_chunk(con, symbol_ids, earliest_date, start_date):
    """
    Process a chunk of symbols for daily return calculations.
    
    Args:
        con: Database connection cursor
        symbol_ids: List of vendor_symbol_ids to process
        earliest_date: Earliest date that needs calculation
        start_date: Original start date parameter
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        # Convert symbol_ids to SQL-safe format
        symbol_list = ','.join([f"'{sid}'" for sid in symbol_ids])
        
        # Need extra lookback period for the rolling calculations (63 days + buffer)
        lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=260)
        
        print(f"  Loading data from {lookback_date} for {len(symbol_ids)} symbols")

        # Load price data for this chunk of symbols including SPY, TLT, and HYG
        query = f"""
        SELECT 
            dm.vendor_symbol_id AS vsid,
            dm.symbol,
            dm.date,
            te.open,
            te.close,
            te.adj_close
        FROM daily_metrics dm
        JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
        WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
          AND (dm.vendor_symbol_id IN ({symbol_list}) OR dm.symbol IN ('SPY', 'TLT', 'HYG'))
        ORDER BY dm.vendor_symbol_id, dm.date
        """
        df = con.execute(query).df()
        
        if df.empty:
            return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
        
        # Convert dates to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate intraday return (open to close)
        df['return_0'] = (df['close'] - df['open']) / df['open']
        
        # Calculate closing return (close to previous close)
        df['return_close_close'] = df.groupby('vsid')['adj_close'].pct_change()
        
        # Calculate the z-score of return_0 over 21 days (1 month)
        df['return_0_zscore_1m'] = df.groupby('vsid')['return_0'].transform(
            lambda x: (x - x.rolling(21, min_periods=10).mean()) / x.rolling(21, min_periods=10).std())
        
        # Extract SPY data and calculate SPY return percentiles
        spy_data = df[df['symbol'] == 'SPY'].copy()
        
        if not spy_data.empty:
            # Save SPY return_0 with the name spy_return_0
            spy_data['spy_return_0'] = spy_data['return_0']
            
            # Use the fixed rolling_percentile function
            spy_data['spy_return_0_pctile_1y'] = rolling_percentile(spy_data['return_0'], 252)
            
            # Keep only necessary columns from SPY data for merging
            spy_metrics = spy_data[['date', 'spy_return_0', 'spy_return_0_pctile_1y']]

            # Merge SPY metrics back to main dataframe
            df = df.merge(spy_metrics, on='date', how='left')
            
        else:
            # If SPY data is missing, add an empty column
            df['spy_return_0'] = np.nan
            df['spy_return_0_pctile_1y'] = np.nan
            print("  WARNING: SPY data not found for this chunk. SPY percentile metrics will be missing.")

        # Extract TLT and HYG data for comprehensive metrics calculation
        tlt_data = df[df['symbol'] == 'TLT'][['date', 'close', 'adj_close']].copy()
        hyg_data = df[df['symbol'] == 'HYG'][['date', 'close', 'adj_close']].copy()
        
        # Calculate TLT and HYG metrics if both are available
        if not tlt_data.empty and not hyg_data.empty:
            # Calculate daily returns for TLT and HYG
            tlt_data['tlt_return'] = tlt_data['adj_close'].pct_change()
            hyg_data['hyg_return'] = hyg_data['adj_close'].pct_change()
            
            # Merge TLT and HYG data
            tlt_hyg_data = tlt_data.merge(hyg_data, on='date', how='inner', suffixes=('_tlt', '_hyg'))
            
            # Calculate TLT/HYG ratio
            tlt_hyg_data['tlt_hyg_ratio'] = tlt_hyg_data['close_tlt'] / tlt_hyg_data['close_hyg']
            
            # Calculate log return of the ratio
            tlt_hyg_data['log_return_tlt_hyg'] = np.log(
                tlt_hyg_data['tlt_hyg_ratio'] / tlt_hyg_data['tlt_hyg_ratio'].shift(1)
            )
            
            # Calculate 3-month z-score of ratio (63 trading days)
            tlt_hyg_data['tlt_hyg_ratio_mean_3m'] = tlt_hyg_data['tlt_hyg_ratio'].rolling(63, min_periods=30).mean()
            tlt_hyg_data['tlt_hyg_ratio_std_3m'] = tlt_hyg_data['tlt_hyg_ratio'].rolling(63, min_periods=30).std()
            tlt_hyg_data['tlt_hyg_ratio_zscore_3m'] = np.where(
                tlt_hyg_data['tlt_hyg_ratio_std_3m'] > 0,
                (tlt_hyg_data['tlt_hyg_ratio'] - tlt_hyg_data['tlt_hyg_ratio_mean_3m']) / tlt_hyg_data['tlt_hyg_ratio_std_3m'],
                np.nan
            )
            
            # Calculate 1-month volatility for TLT and HYG (21 trading days)
            tlt_hyg_data['vol_tlt_1m'] = tlt_hyg_data['tlt_return'].rolling(21, min_periods=10).std()
            tlt_hyg_data['vol_hyg_1m'] = tlt_hyg_data['hyg_return'].rolling(21, min_periods=10).std()
            
            # Calculate 3-month z-scores of TLT and HYG returns (63 trading days)
            tlt_hyg_data['tlt_return_mean_3m'] = tlt_hyg_data['tlt_return'].rolling(63, min_periods=30).mean()
            tlt_hyg_data['tlt_return_std_3m'] = tlt_hyg_data['tlt_return'].rolling(63, min_periods=30).std()
            tlt_hyg_data['zscore_return_tlt_3m'] = np.where(
                tlt_hyg_data['tlt_return_std_3m'] > 0,
                (tlt_hyg_data['tlt_return'] - tlt_hyg_data['tlt_return_mean_3m']) / tlt_hyg_data['tlt_return_std_3m'],
                np.nan
            )
            
            tlt_hyg_data['hyg_return_mean_3m'] = tlt_hyg_data['hyg_return'].rolling(63, min_periods=30).mean()
            tlt_hyg_data['hyg_return_std_3m'] = tlt_hyg_data['hyg_return'].rolling(63, min_periods=30).std()
            tlt_hyg_data['zscore_return_hyg_3m'] = np.where(
                tlt_hyg_data['hyg_return_std_3m'] > 0,
                (tlt_hyg_data['hyg_return'] - tlt_hyg_data['hyg_return_mean_3m']) / tlt_hyg_data['hyg_return_std_3m'],
                np.nan
            )
            
            # Keep only necessary columns for merging
            tlt_hyg_metrics = tlt_hyg_data[[
                'date', 'tlt_hyg_ratio', 'log_return_tlt_hyg', 'tlt_hyg_ratio_zscore_3m',
                'vol_tlt_1m', 'vol_hyg_1m', 'zscore_return_tlt_3m', 'zscore_return_hyg_3m'
            ]]
            
            # Merge TLT/HYG metrics back to main dataframe
            df = df.merge(tlt_hyg_metrics, on='date', how='left')
        else:
            # If TLT or HYG data is missing, add empty columns
            tlt_hyg_columns = [
                'tlt_hyg_ratio', 'log_return_tlt_hyg', 'tlt_hyg_ratio_zscore_3m',
                'vol_tlt_1m', 'vol_hyg_1m', 'zscore_return_tlt_3m', 'zscore_return_hyg_3m'
            ]
            for col in tlt_hyg_columns:
                df[col] = np.nan
            
            missing_symbols = []
            if tlt_data.empty:
                missing_symbols.append('TLT')
            if hyg_data.empty:
                missing_symbols.append('HYG')
            print(f"  WARNING: {', '.join(missing_symbols)} data not found for this chunk. TLT/HYG metrics will be missing.")
        
        # Filter to only the symbols we're processing (not SPY, TLT, HYG) and dates >= earliest_date
        update_df = df[
            (df['vsid'].isin(symbol_ids)) & 
            (df['date'] >= pd.to_datetime(earliest_date))
        ].copy()
        
        # Update database directly from DataFrame
        if not update_df.empty:
            # Prepare columns for update - now including all TLT/HYG metrics
            columns_to_update = [
                'return_0', 'return_0_zscore_1m', 'return_close_close', 'spy_return_0', 'spy_return_0_pctile_1y',
                'tlt_hyg_ratio', 'log_return_tlt_hyg', 'tlt_hyg_ratio_zscore_3m',
                'vol_tlt_1m', 'vol_hyg_1m', 'zscore_return_tlt_3m', 'zscore_return_hyg_3m'
            ]
            
            # Create DataFrame for update
            update_cols = ['vsid', 'date'] + columns_to_update
            temp_df = update_df[update_cols].dropna(subset=['return_0'], how='all')
            
            if not temp_df.empty:
                print(f"  Updating {len(temp_df)} daily return records for {len(temp_df['vsid'].unique())} symbols")
                
                # Update directly from DataFrame
                update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                update_query = f"""
                UPDATE daily_metrics
                SET {update_clause}
                FROM temp_df
                WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                  AND daily_metrics.date = temp_df.date
                """
                con.execute(update_query)
                
                return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
        else:
            return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
    except Exception as e:
        print(f"  Error processing chunk: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_streak_calculation(duckdb_con, start_date):
    """
    Calculate winning/losing streaks using SQL with proper incremental logic.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs streak calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (streak_0 IS NULL OR streak_close_close IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                msg = "No data needs streak calculations"
                print(msg)
                logger.info(msg)
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            msg = f"Processing streaks from {earliest_date} using step-by-step SQL approach"
            print(msg)
            logger.info(msg)
            
            # Step-by-step approach to avoid nested window functions
            streak_query = f"""
            WITH DirectionData AS (
                SELECT 
                    vendor_symbol_id,
                    date,
                    return_0,
                    return_close_close,
                    CASE 
                        WHEN return_0 > 0.0001 THEN 1
                        WHEN return_0 < -0.0001 THEN -1
                        ELSE 0
                    END AS direction_0,
                    CASE 
                        WHEN return_close_close > 0.0001 THEN 1
                        WHEN return_close_close < -0.0001 THEN -1
                        ELSE 0
                    END AS direction_close_close
                FROM daily_metrics
                WHERE date >= '{earliest_date}'
                ORDER BY vendor_symbol_id, date
            ),
            
            -- Add previous direction for comparison
            DirectionWithLag AS (
                SELECT *,
                    LAG(direction_0) OVER (PARTITION BY vendor_symbol_id ORDER BY date) AS prev_direction_0,
                    LAG(direction_close_close) OVER (PARTITION BY vendor_symbol_id ORDER BY date) AS prev_direction_close_close
                FROM DirectionData
            ),
            
            -- Create change indicators
            DirectionChanges AS (
                SELECT *,
                    CASE WHEN prev_direction_0 != direction_0 OR prev_direction_0 IS NULL THEN 1 ELSE 0 END AS change_0,
                    CASE WHEN prev_direction_close_close != direction_close_close OR prev_direction_close_close IS NULL THEN 1 ELSE 0 END AS change_close_close
                FROM DirectionWithLag
            ),
            
            -- Create streak groups using cumulative sum
            StreakGroups AS (
                SELECT *,
                    SUM(change_0) OVER (PARTITION BY vendor_symbol_id ORDER BY date) AS streak_group_0,
                    SUM(change_close_close) OVER (PARTITION BY vendor_symbol_id ORDER BY date) AS streak_group_close_close
                FROM DirectionChanges
            ),
            
            -- Calculate streak lengths within each group
            StreakLengths AS (
                SELECT 
                    vendor_symbol_id,
                    date,
                    direction_0,
                    direction_close_close,
                    CASE 
                        WHEN direction_0 = 0 THEN 0
                        ELSE direction_0 * ROW_NUMBER() OVER (
                            PARTITION BY vendor_symbol_id, streak_group_0 
                            ORDER BY date
                        )
                    END AS new_streak_0,
                    CASE 
                        WHEN direction_close_close = 0 THEN 0
                        ELSE direction_close_close * ROW_NUMBER() OVER (
                            PARTITION BY vendor_symbol_id, streak_group_close_close 
                            ORDER BY date
                        )
                    END AS new_streak_close_close
                FROM StreakGroups
            )
            
            UPDATE daily_metrics
            SET 
                streak_0 = sl.new_streak_0,
                streak_close_close = sl.new_streak_close_close
            FROM StreakLengths sl
            WHERE daily_metrics.vendor_symbol_id = sl.vendor_symbol_id 
              AND daily_metrics.date = sl.date
              AND (daily_metrics.streak_0 IS NULL OR daily_metrics.streak_close_close IS NULL)
            """
            
            # Execute the query
            con.execute(streak_query)
            
            # Get count of updated symbols
            updated_count_query = f"""
            SELECT COUNT(DISTINCT vendor_symbol_id) FROM daily_metrics
            WHERE date >= '{earliest_date}' 
              AND streak_0 IS NOT NULL 
              AND streak_close_close IS NOT NULL
            """
            updated_symbols = con.execute(updated_count_query).fetchone()[0]
            
            msg = f"Successfully processed streaks for {updated_symbols} symbols"
            print(msg)
            logger.info(msg)
            return {'count_success': updated_symbols, 'count_skip': 0, 'count_fail': 0}
            
    except Exception as e:
        msg = f"Error in bulk streak calculation: {e}"
        print(msg)
        logger.error(msg)
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_volatility_calculation(duckdb_con, start_date):
    """
    Calculate volatility metrics for all stocks using vectorized operations.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs volatility calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (volatility_1m IS NULL OR volatility_3m IS NULL OR volatility_1m_zscore_3m IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs volatility calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Get all price data since the earliest missing date (minus lookback period)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=97)
            print(f"Loading data from {lookback_date} for volatility calculations")
            
            price_query = f"""
            SELECT t.vendor_symbol_id AS vsid, t.symbol, t.date, t.adj_close
            FROM tiingo_eod t
            WHERE t.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY t.vendor_symbol_id, t.date
            """
            df = con.execute(price_query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate log returns - safely handling zeros and negative values
            df['log_return'] = np.nan
            mask = (df['adj_close'] > 0) & (df.groupby('vsid')['adj_close'].shift(1) > 0)
            df.loc[mask, 'log_return'] = np.log(
                df.loc[mask, 'adj_close'] / df.groupby('vsid')['adj_close'].shift(1).loc[mask]
            )
            
            # Calculate volatility metrics
            df['volatility_1m'] = df.groupby('vsid')['log_return'].transform(
                lambda x: x.rolling(window=21, min_periods=10).std()
            )
            
            df['volatility_3m'] = df.groupby('vsid')['log_return'].transform(
                lambda x: x.rolling(window=63, min_periods=30).std()
            )
            
            # Calculate z-score of 1m volatility over 3m period
            df['volatility_1m_zscore_3m'] = df.groupby('vsid')['volatility_1m'].transform(
                lambda x: (x - x.rolling(window=63, min_periods=30).mean()) / x.rolling(window=63, min_periods=30).std()
            )
            
            # Get SPY's volatility metrics exactly as in your code
            spy_metrics = df[df['symbol'] == 'SPY'][['date', 'volatility_1m', 'volatility_3m', 'log_return']]
            spy_metrics = spy_metrics.rename(columns={
                'volatility_1m': 'spy_volatility_1m',
                'volatility_3m': 'spy_volatility_3m',
                'log_return': 'spy_log_return'
            })
            
            # Merge SPY metrics back to main dataframe based on date
            df = df.merge(spy_metrics, on='date', how='left')
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update
                columns_to_update = [
                    'log_return', 'volatility_1m', 'volatility_3m', 'volatility_1m_zscore_3m',
                    'spy_volatility_1m', 'spy_volatility_3m', 'spy_log_return'
                ]
                
                # Filter out rows with all NaN in volatility columns
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['volatility_1m', 'volatility_3m'], how='all')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} volatility records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk volatility calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_trading_val_calculation(duckdb_con, start_date):
    """
    Calculate trading value metrics for all stocks using vectorized operations.
    
    Args:
        duckdb_con: DuckDB connection
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:            
            # Find the earliest date that needs trading val calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (trading_val IS NULL OR trading_val_adtval IS NULL OR trading_val_zscore_3m IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs trading value calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Get all price data since the earliest missing date (minus lookback period)
            # Need 3 months of data for z-score calculation
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=97)
            print(f"Loading data from {lookback_date} for trading value calculations")
            
            price_query = f"""
            SELECT 
                t.vendor_symbol_id AS vsid, 
                t.symbol, 
                t.date, 
                t.close, 
                t.volume,
                dm.adtval
            FROM tiingo_eod t
            LEFT JOIN daily_metrics dm ON t.vendor_symbol_id = dm.vendor_symbol_id AND t.date = dm.date
            WHERE t.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY t.vendor_symbol_id, t.date
            """
            df = con.execute(price_query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate trading value metrics
            df['trading_val'] = df['close'] * df['volume']
            
            # Calculate trading_val_adtval (handling potential division by zero)
            df['trading_val_adtval'] = np.where(
                df['adtval'] > 0,
                df['trading_val'] / df['adtval'],
                np.nan
            )
            
            # Calculate trading_val_zscore_3m
            # Define window size for 3 months (approximately 63 trading days)
            window_size = 63
            
            # Use transform with rolling to calculate z-score for each group
            df['trading_val_mean'] = df.groupby('vsid')['trading_val'].transform(
                lambda x: x.rolling(window=window_size, min_periods=30).mean()
            )
            
            df['trading_val_std'] = df.groupby('vsid')['trading_val'].transform(
                lambda x: x.rolling(window=window_size, min_periods=30).std()
            )
            
            # Calculate z-score with protection against division by zero
            df['trading_val_zscore_3m'] = np.where(
                df['trading_val_std'] > 0,
                (df['trading_val'] - df['trading_val_mean']) / df['trading_val_std'],
                np.nan
            )
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update
                columns_to_update = ['trading_val', 'trading_val_adtval', 'trading_val_zscore_3m']
                
                # Create DataFrame for update with only needed columns
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['trading_val'], how='all')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} trading value records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk trading value calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_beta_calculation(duckdb_con, start_date):
    """
    Calculate beta metrics for all stocks using vectorized operations.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs beta calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (beta_3m IS NULL OR beta_3m_zscore_3m IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs beta calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the rolling calculations (60 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=130)
            print(f"Loading data from {lookback_date} for beta calculations")
            
            # We need log_return and spy_log_return for beta calculations
            # If these are already calculated by volatility module, use them
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                dm.log_return,
                dm.spy_log_return
            FROM daily_metrics dm
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate rolling 3-month covariance of stock and SPY log returns
            df["cov_log_3m"] = df.groupby('vsid').apply(
                lambda g: g['log_return'].rolling(63, min_periods=30).cov(g['spy_log_return'])
            ).reset_index(level=0, drop=True)
            
            # Calculate rolling 3-month variance of SPY log returns
            df["var_spy_log_3m"] = df.groupby('vsid').apply(
                lambda g: g['spy_log_return'].rolling(63, min_periods=30).var()
            ).reset_index(level=0, drop=True)
            
            # Calculate beta (both columns as in your notebook code)
            df["beta_3m"] = np.where(
                df["var_spy_log_3m"] > 0,  # Avoid division by zero
                df["cov_log_3m"] / df["var_spy_log_3m"],
                np.nan
            )
            
            # Calculate 3-month rolling z-score of beta
            df["beta_3m_zscore_3m"] = df.groupby("vsid")["beta_3m"].transform(
                lambda x: (x - x.rolling(63, min_periods=30).mean()) / x.rolling(63, min_periods=30).std()
            )
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update - include all 5 columns
                columns_to_update = [
                    'beta_3m', 'beta_3m_zscore_3m'
                ]
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols]
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} beta records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk beta calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_vxx_calculation(duckdb_con, start_date):
    """
    Calculate VXX volatility metrics for all stocks using vectorized operations.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs VXX calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (vxx_close_zscore_1m IS NULL OR vxx_close_pctile_1y IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs VXX calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the rolling calculations (252 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=280)
            print(f"Loading data from {lookback_date} for VXX calculations")
            
            # Load price data for all symbols and VXX
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.adj_close
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.symbol, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Extract VXX data and calculate metrics
            vxx_data = df[df['symbol'] == 'VXX'].copy()
            
            if not vxx_data.empty:
                # Calculate VXX adj_close z-score over 21d
                vxx_data['vxx_close_zscore_1m'] = vxx_data['adj_close'].rolling(window=21, min_periods=10).apply(
                    lambda x: (x[-1] - x.mean()) / x.std() if len(x) > 1 else np.nan, 
                    raw=True
                )
                
                # Calculate percentile rank of VXX adj_close over a 252 day window
                vxx_data['vxx_close_pctile_1y'] = rolling_percentile(vxx_data['adj_close'], 252)
                
                # Keep only necessary columns for merging
                vxx_metrics = vxx_data[['date', 'vxx_close_zscore_1m', 'vxx_close_pctile_1y']]
                
                # Merge VXX metrics back to main dataframe
                df = df.merge(vxx_metrics, on='date', how='left')
            else:
                # If VXX data is missing, add empty columns
                df['vxx_close_zscore_1m'] = np.nan
                df['vxx_close_pctile_1y'] = np.nan
                print("WARNING: VXX data not found. VXX metrics will be missing.")
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update
                columns_to_update = [
                    'vxx_close_zscore_1m', 'vxx_close_pctile_1y'
                ]
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols]
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} VXX metric records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk VXX calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_atr_calculation(duckdb_con, start_date):
    """
    Calculate average true range (ATR) for all stocks using vectorized operations.
    
    NOTE: These are intermediate calculations required for calculating gap stress. They should not be used as features
    in the model. Gap stress is not calculated here because it relies on overnight return, which requires data from the
    morning of.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs ATR calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE atr_14 IS NULL AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs ATR calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the ATR calculation (14 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=30)
            print(f"Loading data from {lookback_date} for ATR calculations")
            
            # Load price data for all symbols
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.high,
                te.low,
                te.adj_close
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Step 1: Calculate True Range
            df["true_range"] = df.groupby("vsid").apply(
                lambda g: pd.concat([
                    g["high"] - g["low"],
                    (g["high"] - g["adj_close"].shift(1)).abs(),
                    (g["low"] - g["adj_close"].shift(1)).abs()
                ], axis=1).max(axis=1)
            ).reset_index(level=0, drop=True)
            
            # Step 2: Calculate ATR (14-day average)
            df["atr_14"] = df.groupby("vsid")["true_range"].transform(
                lambda x: x.rolling(14, min_periods=7).mean()
            )
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update
                columns_to_update = ['atr_14']
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['atr_14'], how='all')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} ATR records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk ATR calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_skew_calculation(duckdb_con, start_date):
    """
    Calculate skewness and kurtosis metrics for all stocks using vectorized operations.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs skew/kurt calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (skew_1m IS NULL OR kurt_1m IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs skew/kurtosis calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the rolling calculations (21 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=30)
            print(f"Loading data from {lookback_date} for skew/kurtosis calculations")
            
            # Load price data including log_return
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                dm.log_return
            FROM daily_metrics dm
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
              AND dm.log_return IS NOT NULL
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Clip outliers to avoid contaminating stats
            df["log_return_clipped"] = df["log_return"].clip(lower=-0.5, upper=0.5)
            
            # Calculate rolling skew and kurtosis with 21-day window
            df["skew_1m"] = df.groupby("vsid")["log_return_clipped"].transform(
                lambda x: x.rolling(21, min_periods=10).skew()
            )
            
            df["kurt_1m"] = df.groupby("vsid")["log_return_clipped"].transform(
                lambda x: x.rolling(21, min_periods=10).kurt()
            )
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update
                columns_to_update = [
                    'skew_1m', 'kurt_1m'
                ]
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['skew_1m', 'kurt_1m'], how='any')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} skew/kurtosis records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk skew calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_distance_calculation(duckdb_con, start_date):
    """
    Calculate distance from 52-week high and low metrics for all stocks.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs distance calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (dist_from_high_1y IS NULL OR dist_from_low_1y IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs distance calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Need extra lookback period for the rolling calculations (252 days + buffer)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=280)
            print(f"Loading data from {lookback_date} for distance from high/low calculations")
            
            # Load price data
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.adj_close
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate 252-day (1 year) rolling high and low
            df["high_1y"] = df.groupby("vsid")["adj_close"].transform(
                lambda x: x.rolling(252, min_periods=126).max()
            )
            
            df["low_1y"] = df.groupby("vsid")["adj_close"].transform(
                lambda x: x.rolling(252, min_periods=126).min()
            )
            
            # Calculate distance from high and low in percentage terms
            df["dist_from_high_1y"] = (df["adj_close"] / df["high_1y"]) - 1
            df["dist_from_low_1y"] = (df["adj_close"] / df["low_1y"]) - 1
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update - only store the distance metrics
                columns_to_update = [
                    'dist_from_high_1y', 'dist_from_low_1y'
                ]
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=columns_to_update, how='any')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} distance records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk distance calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def bulk_ema_calculation(duckdb_con, start_date):
    """
    Calculate EMA metrics for all stocks using vectorized operations.
    
    Args:
        duckdb_con: DuckDB connection
        start_date: Start date for processing
        
    Returns:
        dict with counts of success, skip, and fail
    """
    try:
        with duckdb_con.cursor() as con:
            # Find the earliest date that needs EMA calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (ema_9 IS NULL OR ema_50 IS NULL OR ema_100 IS NULL)
              AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs EMA calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Use the longest EMA period (100) as the minimum lookback
            # This ensures we have enough data to calculate EMA_100 properly
            lookback_days = 100  # Just need 100 days for EMA_100, not 300
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=lookback_days)
            print(f"Loading data from {lookback_date} for EMA calculations")
            
            # Load price data for all symbols
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.adj_close
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate EMAs using pandas groupby
            # pandas ewm() will start calculating immediately but we'll mask early values
            df['ema_9_raw'] = df.groupby('vsid')['adj_close'].transform(
                lambda x: x.ewm(span=9, adjust=False).mean()
            )
            df['ema_50_raw'] = df.groupby('vsid')['adj_close'].transform(
                lambda x: x.ewm(span=50, adjust=False).mean()
            )
            df['ema_100_raw'] = df.groupby('vsid')['adj_close'].transform(
                lambda x: x.ewm(span=100, adjust=False).mean()
            )
            
            # Create row numbers within each symbol group to determine when we have enough data
            df['row_num'] = df.groupby('vsid').cumcount() + 1
            
            # Only keep EMA values where we have sufficient data points
            # EMA_9 needs at least 9 data points, EMA_50 needs 50, EMA_100 needs 100
            df['ema_9'] = np.where(df['row_num'] >= 9, df['ema_9_raw'], np.nan)
            df['ema_50'] = np.where(df['row_num'] >= 50, df['ema_50_raw'], np.nan)
            df['ema_100'] = np.where(df['row_num'] >= 100, df['ema_100_raw'], np.nan)
            
            # Clean up temporary columns
            df = df.drop(columns=['ema_9_raw', 'ema_50_raw', 'ema_100_raw', 'row_num'])
            
            # Keep only rows we need to update (those with dates >= earliest_date)
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            # Update database directly from DataFrame
            if not update_df.empty:
                # Prepare columns for update
                columns_to_update = ['ema_9', 'ema_50', 'ema_100']
                
                # Create DataFrame for update
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].copy()
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} EMA records for {len(temp_df['vsid'].unique())} symbols")
                    
                    # Update directly from DataFrame
                    update_clause = ", ".join([f"{col} = temp_df.{col}" for col in columns_to_update])
                    update_query = f"""
                    UPDATE daily_metrics
                    SET {update_clause}
                    FROM temp_df
                    WHERE daily_metrics.vendor_symbol_id = temp_df.vsid 
                      AND daily_metrics.date = temp_df.date
                    """
                    con.execute(update_query)
                    
                    return {'count_success': len(temp_df['vsid'].unique()), 'count_skip': 0, 'count_fail': 0}
                else:
                    return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            else:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk EMA calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def compute_ema_live_open(df):
    """
    Compute EMA values using current open price for live trading simulation.
    This simulates what a trader would see at market open.
    
    Args:
        df: DataFrame with columns: vsid, date, adj_close, adj_open, ema_9, ema_50, ema_100
        
    Returns:
        DataFrame with additional columns: ema_9_open, ema_50_open, ema_100_open
    """
    try:
        # Get the latest two rows per symbol (current and previous day)
        latest_two_rows = df.groupby('vsid').tail(2)
        
        # Find symbols that have exactly 2 rows and required data
        symbol_counts = latest_two_rows.groupby('vsid').size()
        valid_symbols = symbol_counts[symbol_counts == 2].index.tolist()
        
        if not valid_symbols:
            print("No symbols have sufficient data for EMA live calculation")
            return df
        
        # Filter to valid symbols and get current day (latest) data
        current_data = latest_two_rows.groupby('vsid').tail(1)
        current_data = current_data[current_data['vsid'].isin(valid_symbols)]
        
        # Check for required columns
        required_cols = ['adj_open', 'ema_9', 'ema_50', 'ema_100']
        missing_cols = [col for col in required_cols if col not in current_data.columns]
        if missing_cols:
            print(f"Missing required columns for EMA live calculation: {missing_cols}")
            return df
        
        # Filter out rows with missing data
        valid_mask = current_data[required_cols].notna().all(axis=1)
        current_data = current_data[valid_mask]
        
        if current_data.empty:
            print("No valid data for EMA live calculation")
            return df
        
        # Calculate live EMAs using current open price
        # EMA formula: EMA_today = (Price_today * multiplier) + (EMA_yesterday * (1 - multiplier))
        # where multiplier = 2 / (period + 1)
        
        multiplier_9 = 2 / (9 + 1)
        multiplier_50 = 2 / (50 + 1)
        multiplier_100 = 2 / (100 + 1)
        
        current_data = current_data.copy()
        current_data['ema_9_open'] = (current_data['adj_open'] * multiplier_9) + (current_data['ema_9'] * (1 - multiplier_9))
        current_data['ema_50_open'] = (current_data['adj_open'] * multiplier_50) + (current_data['ema_50'] * (1 - multiplier_50))
        current_data['ema_100_open'] = (current_data['adj_open'] * multiplier_100) + (current_data['ema_100'] * (1 - multiplier_100))
        
        # Merge results back to main dataframe
        df = df.merge(
            current_data[['vsid', 'date', 'ema_9_open', 'ema_50_open', 'ema_100_open']], 
            on=['vsid', 'date'], 
            how='left', 
            suffixes=('', '_new')
        )
        
        # Update columns where new values exist
        for col in ['ema_9_open', 'ema_50_open', 'ema_100_open']:
            new_col = f"{col}_new"
            if new_col in df.columns:
                df[col] = df[new_col].fillna(df[col])
                df = df.drop(columns=[new_col])
        
        print(f"Calculated live EMA for {len(current_data)} symbols")
        return df
        
    except Exception as e:
        print(f"Error in EMA live calculation: {e}")
        import traceback
        traceback.print_exc()
        return df



def update_ema(vendor_symbol_id: str, duckdb_con: duckdb.DuckDBPyConnection, start_date='1990-06-01',
               end_date=None):
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
        price_data = con.execute(sql_query).df()

        # Check if data is sufficient
        if price_data.shape[0] == 0:
            print(f"update_ema: No data available for params: {vendor_symbol_id}, {start_date}, {end_date}.")
            return 'skip'

        # Calculate EMAs using TA lib
        ema_9 = talib.EMA(price_data['adj_close'].to_numpy(), timeperiod=9)
        ema_50 = talib.EMA(price_data['adj_close'].to_numpy(), timeperiod=50)
        ema_100 = talib.EMA(price_data['adj_close'].to_numpy(), timeperiod=100)
        # Prepare DataFrame to insert results
        results = pd.DataFrame({
            'vendor_symbol_id': vendor_symbol_id,
            'date': price_data['date'],
            'ema_9': ema_9,
            'ema_50': ema_50,
            'ema_100': ema_100
        })
        # Replace NaN values with None for database compatibility
        results = results.where(pd.notnull(results), None)

        # Insert/update the daily_metrics table via UPDATE statement
        query = """
                UPDATE daily_metrics
                SET ema_9   = results.ema_9,
                    ema_50  = results.ema_50,
                    ema_100 = results.ema_100 FROM results
                WHERE daily_metrics.vendor_symbol_id = results.vendor_symbol_id
                  AND daily_metrics.date = results.date;"""
        con.execute(query)
    return 'success'


def avg_daily_trading_value(value=20000000.0, start_date: str = '1990-06-01'):
    """Updates the avg_daily_trading_value and adtval columns of the daily_metrics table.
    avg_daily_trading_value: Average of close * volume of the previous 5 trading days
    adtval: Average of close * volume of the current day and previous 4 trading days
    Both values are inflation adjusted."""
    with duckdb.connect(db_file) as con:
        # Update both trading value metrics in daily_metrics
        con.execute(f"""
        UPDATE daily_metrics
        SET 
            avg_daily_trading_value = sub.avg_value,
            adtval = sub.current_avg_value
        FROM (
            SELECT
                e.vendor_symbol_id,
                e.date,
                AVG(e.volume * e.close) OVER (
                    PARTITION BY e.vendor_symbol_id
                    ORDER BY e.date
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ) AS avg_value,
                AVG(e.volume * e.close) OVER (
                    PARTITION BY e.vendor_symbol_id
                    ORDER BY e.date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS current_avg_value
            FROM tiingo_eod e
        ) sub
        WHERE daily_metrics.vendor_symbol_id = sub.vendor_symbol_id
              AND daily_metrics.date = sub.date;
        """)
        inflation_df = con.execute("SELECT * FROM inflation ORDER BY year").pl()
        daily_metrics_df = con.execute("""
                                       SELECT vendor_symbol_id, date, avg_daily_trading_value, adtval
                                       FROM daily_metrics
                                       """).pl()

    # Calculate cumulative inflation multiplier
    inflation_df = inflation_df.with_columns(
        (1 + inflation_df['inflation_rate'] / 100).cum_prod().alias('cumulative_inflation')
    )

    # Normalize to the latest year's inflation (make latest year's multiplier = 1)
    latest_multiplier = inflation_df.filter(pl.col('year') == inflation_df['year'].max()).select(
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

    # Create both flags in a single operation
    result_df = result_df.with_columns([
        (pl.col('avg_daily_trading_value') >= value * pl.col('adjusted_multiplier')).alias('has_min_trading_value'),
        (pl.col('adtval') >= value * pl.col('adjusted_multiplier')).alias('has_min_adtval')
    ])

    # Now both columns exist and can be selected
    result_df = result_df.select(['vendor_symbol_id', 'date', 'has_min_trading_value', 'has_min_adtval'])

    with duckdb.connect(db_file) as con:
        con.execute("""
                    UPDATE daily_metrics
                    SET has_min_trading_value = result_df.has_min_trading_value,
                        has_min_adtval        = result_df.has_min_adtval FROM result_df
                    WHERE daily_metrics.vendor_symbol_id = result_df.vendor_symbol_id
                      AND daily_metrics.date = result_df.date
                    """)


def pe_rank(date='2005-01-01'):
    msg = "Calculating P/E ratio percentile ranks..."
    print(msg)
    logger.info(msg)
    with duckdb.connect(db_file) as con:
        # P/E Ratio
        # Get the latest date from
        query = f"""
            WITH RelevantMetrics AS (
                SELECT
                    fd.vendor_symbol_id,
                    fd.symbol,
                    m.name,
                    fd.date AS trading_date,
                    CASE WHEN fd.pe_ratio < 0 THEN 'Infinity' ELSE fd.pe_ratio END as pe_ratio,
                    m.industry
                FROM tiingo_fundamentals_daily fd
                LEFT JOIN tiingo_fundamentals_meta m
                ON fd.vendor_symbol_id = m.vendor_symbol_id
                WHERE fd.date > '{date}'
            ),
            PercentileRanks AS (
                SELECT
                    vendor_symbol_id,
                    trading_date,
                    PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY pe_ratio DESC) * 100 AS pe_ratio_pct_rank
                FROM RelevantMetrics
            )
            UPDATE daily_metrics
            SET pe_ratio_pct_rank = PercentileRanks.pe_ratio_pct_rank
            FROM PercentileRanks
            WHERE daily_metrics.vendor_symbol_id = PercentileRanks.vendor_symbol_id
                  AND daily_metrics.date = PercentileRanks.trading_date;"""
        try:
            logger.info("pe_rank: Updating P/E ratio rank.")
            con.execute(query)
            msg = "pe_rank: Done updating P/E ratio rank."
            print(msg)
            logger.info(msg)
        except Exception as e:
            msg = f"pe_rank: Error updating P/E ratio rank: {e}"
            print(msg)
            logger.error(msg)
            return 'fail'


def enterprise_multiple_rank(date='2005-01-01'):
    msg = "Calculating enterprise multiple percentile ranks..."
    print(msg)
    logger.info(msg)
    with duckdb.connect(db_file) as con:
        # Enterprise Multiple
        query = f"""
            WITH RelevantMetrics AS (
                SELECT
                    fd.vendor_symbol_id,
                    fd.symbol,
                    fd.date AS trading_date,
                    CASE WHEN fd.enterprise_val > 0 AND f.ebitda_ttm > 0 THEN fd.enterprise_val / f.ebitda_ttm ELSE 'Infinity' END AS enterprise_multiple,
                    f.date AS metrics_date,
                    m.industry
                FROM tiingo_fundamentals_daily fd
                LEFT JOIN tiingo_fundamentals_meta m
                ON fd.vendor_symbol_id = m.vendor_symbol_id
                LEFT JOIN LATERAL (
                    SELECT date, ebitda_ttm
                    FROM tiingo_fundamental_metrics fm
                    WHERE fd.date > '{date}' AND fm.vendor_symbol_id = fd.vendor_symbol_id
                      AND date < fd.date  -- Most recent metrics before the trading day
                      AND date > fd.date - INTERVAL 100 DAY
                    ORDER BY fm.date DESC
                    LIMIT 1
                ) f ON true
                WHERE fd.date > '{date}'
            ),
            PercentileRanks AS (
                SELECT
                    vendor_symbol_id,
                    trading_date,
                    enterprise_multiple,
                    PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY enterprise_multiple DESC) * 100 AS enterprise_multiple_pct_rank
                FROM RelevantMetrics
                WHERE trading_date > '{date}'
            )
            UPDATE daily_metrics
            SET
                enterprise_multiple = pr.enterprise_multiple,
                enterprise_multiple_pct_rank = pr.enterprise_multiple_pct_rank
            FROM
                (SELECT * FROM PercentileRanks) AS pr
            WHERE
                daily_metrics.vendor_symbol_id = pr.vendor_symbol_id AND
                daily_metrics.date = pr.trading_date;
            """
        try:
            logger.info("enterprise_multiple_rank: Updating enterprise multiple rank.")
            con.execute(query)
            logger.info("enterprise_multiple_rank: Done updating enterprise multiple rank.")
        except Exception as e:
            msg = f"enterprise_multiple_rank: Error updating enterprise multiple rank: {e}"
            print(msg)
            logger.error(msg)
            return 'fail'


def create_relevant_metrics_fm(duckdb_con, date='2005-01-01'):
    """Creates a temporary table from tiingo_fundamental_metrics, used when creating rankings.
    However, it has all the metrics at a daily level, meaning as of that date, for stocks that are active on that day.
    Temp table seems to have better performance than a CTE."""
    msg = f"Creating temporary table RelevantMetrics."
    print(msg)
    logger.info(msg)
    # Get the latest date from
    query = f"""
CREATE  TEMP TABLE RelevantMetrics AS  SELECT
    e.vendor_symbol_id,
    e.symbol,
    e.date AS trading_date,
    f.date AS metrics_date,
    grossProfit_ttm,
    opinc_ttm,
    netinc_ttm,
    revenue_ttm,
    gross_margin_ttm,
    operating_margin_ttm,
    net_margin_ttm,
    freeCashFlow_ttm,
    grossProfit_trend_slope,
    opinc_trend_slope,
    netinc_trend_slope,
    revenue_trend_slope,
    gross_margin_trend_slope,
    operating_margin_trend_slope,
    net_margin_trend_slope,
    freeCashFlow_trend_slope,
    currentRatio_trend_slope,
    debt_trend_slope,
    debtEquity_trend_slope,
    roe_trend_slope,
    m.industry
FROM tiingo_eod e
LEFT JOIN tiingo_fundamentals_meta m
ON e.vendor_symbol_id = m.vendor_symbol_id
LEFT JOIN LATERAL (
    SELECT "date", grossProfit_ttm, opinc_ttm, netinc_ttm, revenue_ttm, gross_margin_ttm, operating_margin_ttm,
        net_margin_ttm, freeCashFlow_ttm,
        grossProfit_trend_slope, opinc_trend_slope, netinc_trend_slope,
        revenue_trend_slope, gross_margin_trend_slope, operating_margin_trend_slope, net_margin_trend_slope,
        freeCashFlow_trend_slope, currentRatio_trend_slope, debt_trend_slope, debtEquity_trend_slope, roe_trend_slope
    FROM tiingo_fundamental_metrics fm
    WHERE e.date > '{date}' AND fm.vendor_symbol_id = e.vendor_symbol_id
      AND "date" < e.date  -- Most recent metrics before the trading day
      AND "date" > e.date - INTERVAL 100 DAY
    ORDER BY fm.date DESC
    LIMIT 1
) f ON true
WHERE e.date > '{date}';
    """
    duckdb_con.execute(query)


def create_relevant_metrics_fad(duckdb_con, date='2005-01-01'):
    """Creates a temporary table from tiingo_fundamentals_amended_distinct, used when creating rankings.
    Temp table seems to have better performance than a CTE."""
    msg = f"Creating temporary table RelevantMetrics."
    print(msg)
    logger.info(msg)
    # Get the latest date from
    query = f"""
CREATE TEMP TABLE RelevantMetrics AS SELECT
    e.vendor_symbol_id,
    e.symbol,
    e.date AS trading_date,
    f.date AS metrics_date,
    currentRatio,
    debt,
    debtEquity,
    piotroskiFScore,
    roe,
    m.industry
FROM tiingo_eod e
LEFT JOIN tiingo_fundamentals_meta m
ON e.vendor_symbol_id = m.vendor_symbol_id
LEFT JOIN LATERAL (
    SELECT "date", currentRatio, debt, debtEquity, piotroskiFScore, roe
    -- It doesn't matter if it's an annual or quarterly report since this is from the balance sheet and overview. 
    FROM tiingo_fundamentals_amended_distinct fad
    WHERE e.date > '{date}' AND fad.vendor_symbol_id = e.vendor_symbol_id
      AND date < e.date  -- Most recent metrics before the trading day
      -- 120 day recency eliminates stocks that should've beeen marked inactive, or stopped reporting.
      AND date > e.date - INTERVAL 120 DAY 
    ORDER BY fad.date DESC
    LIMIT 1
) f ON true
WHERE e.date > '{date}';
    """
    duckdb_con.execute(query)


def rank_metric(duckdb_con, metric, by_industry=False, order="ASC"):
    """Create the rank for an individual metric, using RelevantMetrics temp table."""
    msg = f"Creating percentile rank for {metric}"
    print(msg)
    logger.info(msg)
    industry = ", industry" if by_industry else ""
    query = f"""
    UPDATE daily_metrics
    SET
        {metric}_pct_rank = pr.{metric}_pct_rank
    FROM 
        (SELECT
            vendor_symbol_id,
            symbol,
            trading_date,
            CASE
                WHEN {metric} IS NOT NULL THEN 
                    PERCENT_RANK() OVER (
                        PARTITION BY trading_date{industry} 
                        ORDER BY {metric} {order}
                        NULLS LAST
                    ) * 100
                ELSE NULL
            END AS {metric}_pct_rank
        FROM RelevantMetrics
        WHERE {metric} IS NOT NULL) as pr
    WHERE
        daily_metrics.vendor_symbol_id = pr.vendor_symbol_id AND
        daily_metrics.date = pr.trading_date;
    """
    duckdb_con.execute(query)


@DeprecationWarning
def profit_rank(date='2005-01-01'):
    with duckdb.connect(db_file) as con:
        msg = "Calculating profit metrics percentile ranks..."
        print(msg)
        logger.info(msg)
        query = f"""
        WITH RelevantMetrics AS (
            SELECT
                e.vendor_symbol_id,
                e.symbol,
                e.date AS trading_date,
                f.date AS metrics_date,
                f.grossProfit_ttm,
                f.opinc_ttm,
                f.netinc_ttm,
                f.revenue_ttm,
                f.gross_margin_ttm,
                f.operating_margin_ttm,
                f.net_margin_ttm,
                f.freeCashFlow_ttm,
                f.grossProfit_trend_slope,
                f.opinc_trend_slope,
                f.netinc_trend_slope,
                f.revenue_trend_slope,
                f.gross_margin_trend_slope,
                f.operating_margin_trend_slope,
                f.net_margin_trend_slope,
                f.freeCashFlow_trend_slope,
                f.currentRatio_trend_slope,
                f.debt_trend_slope,
                f.debtEquity_trend_slope,
                f.roe_trend_slope,
                m.industry
            FROM tiingo_eod e
            LEFT JOIN tiingo_fundamentals_meta m
            ON e.vendor_symbol_id = m.vendor_symbol_id            
            LEFT JOIN LATERAL (
                SELECT date, grossProfit_ttm, opinc_ttm, netinc_ttm, revenue_ttm, gross_margin_ttm, operating_margin_ttm, 
                    net_margin_ttm, freeCashFlow_ttm, 
                    grossProfit_trend_slope, opinc_trend_slope, netinc_trend_slope,
                    revenue_trend_slope, gross_margin_trend_slope, operating_margin_trend_slope, net_margin_trend_slope,
                    freeCashFlow_trend_slope, currentRatio_trend_slope, debt_trend_slope, debtEquity_trend_slope, roe_trend_slope
                FROM tiingo_fundamental_metrics fm
                WHERE e.date > '{date}' AND fm.vendor_symbol_id = e.vendor_symbol_id
                  AND date < e.date  -- Most recent metrics before the trading day
                  AND date > e.date - INTERVAL 100 DAY
                ORDER BY fm.date DESC
                LIMIT 1
            ) f ON true
        ),
        PercentileRanks AS (
            SELECT
                vendor_symbol_id,
                symbol,
                trading_date,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(grossProfit_ttm, '-Infinity')) * 100 AS grossProfit_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(opinc_ttm, '-Infinity')) * 100 AS opinc_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(netinc_ttm, '-Infinity')) * 100 AS netinc_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(revenue_ttm, '-Infinity')) * 100 AS revenue_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY COALESCE(gross_margin_ttm, '-Infinity')) * 100 AS gross_margin_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY COALESCE(operating_margin_ttm, '-Infinity')) * 100 AS operating_margin_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(net_margin_ttm, '-Infinity')) * 100 AS net_margin_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(freeCashFlow_ttm, '-Infinity')) * 100 AS freeCashFlow_ttm_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(grossProfit_trend_slope, '-Infinity')) * 100 AS grossProfit_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(opinc_trend_slope, '-Infinity')) * 100 AS opinc_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(netinc_trend_slope, '-Infinity')) * 100 AS netinc_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(revenue_trend_slope, '-Infinity')) * 100 AS revenue_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY COALESCE(gross_margin_trend_slope, '-Infinity')) * 100 AS gross_margin_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY COALESCE(operating_margin_trend_slope, '-Infinity')) * 100 AS operating_margin_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(net_margin_trend_slope, '-Infinity')) * 100 AS net_margin_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(freeCashFlow_trend_slope, '-Infinity')) * 100 AS freeCashFlow_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(currentRatio_trend_slope, '-Infinity')) * 100 AS currentRatio_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(debt_trend_slope, 'Infinity') DESC) * 100 AS debt_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(debtEquity_trend_slope, 'Infinity') DESC) * 100 AS debtEquity_trend_slope_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY COALESCE(roe_trend_slope, '-Infinity')) * 100 AS roe_trend_slope_pct_rank
            FROM RelevantMetrics
        )
        UPDATE daily_metrics
            SET 
                grossProfit_ttm_pct_rank = pr.grossProfit_ttm_pct_rank,
                opinc_ttm_pct_rank = pr.opinc_ttm_pct_rank,
                netinc_ttm_pct_rank = pr.netinc_ttm_pct_rank,
                revenue_ttm_pct_rank = pr.revenue_ttm_pct_rank,
                gross_margin_ttm_pct_rank = pr.gross_margin_ttm_pct_rank,
                operating_margin_ttm_pct_rank = pr.operating_margin_ttm_pct_rank,
                net_margin_ttm_pct_rank = pr.net_margin_ttm_pct_rank,
                freeCashFlow_ttm_pct_rank = pr.freeCashFlow_ttm_pct_rank,
                grossProfit_trend_slope_pct_rank = pr.grossProfit_trend_slope_pct_rank,
                opinc_trend_slope_pct_rank = pr.opinc_trend_slope_pct_rank,
                netinc_trend_slope_pct_rank = pr.netinc_trend_slope_pct_rank,
                revenue_trend_slope_pct_rank = pr.revenue_trend_slope_pct_rank,
                gross_margin_trend_slope_pct_rank = pr.gross_margin_trend_slope_pct_rank,
                operating_margin_trend_slope_pct_rank = pr.operating_margin_trend_slope_pct_rank,
                net_margin_trend_slope_pct_rank = pr.net_margin_trend_slope_pct_rank,
                freeCashFlow_trend_slope_pct_rank = pr.freeCashFlow_trend_slope_pct_rank,
                currentRatio_trend_slope_pct_rank = pr.currentRatio_trend_slope_pct_rank,
                debt_trend_slope_pct_rank = pr.debt_trend_slope_pct_rank,
                debtEquity_trend_slope_pct_rank = pr.debtEquity_trend_slope_pct_rank,
                roe_trend_slope_pct_rank = pr.roe_trend_slope_pct_rank
            FROM
                (SELECT * FROM PercentileRanks) AS pr
            WHERE
                daily_metrics.vendor_symbol_id = pr.vendor_symbol_id AND
                daily_metrics.date = pr.trading_date;
        """
        try:
            con.execute(query)
            msg = "Profit metrics percentile ranks calculated."
            print(msg)
            logger.info(msg)
        except Exception as e:
            msg = f"Error calculating profit metrics percentile ranks: {e}"
            print(msg)
            logger.error(msg)
            return 'fail'


@DeprecationWarning
def balance_sheet_rank(date='2005-01-01'):
    """Calculate percentile ranking of metrics related to the balance sheet."""
    with duckdb.connect(db_file) as con:
        msg = "Calculating balance sheet metrics percentile ranks..."
        print(msg)
        logger.info(msg)
        query = f"""
        WITH RelevantMetrics AS (
            SELECT
                e.vendor_symbol_id,
                e.symbol,
                e.date AS trading_date,
                f.date AS metrics_date,
                    CASE WHEN f.currentRatio IS NULL THEN '-Infinity' ELSE f.currentRatio END AS currentRatio,
                CASE WHEN f.debt IS NULL THEN 'Infinity' ELSE f.debt END AS debt,
                CASE WHEN f.debtEquity IS NULL THEN 'Infinity' ELSE f.debtEquity END AS debtEquity,
                CASE WHEN f.piotroskiFScore IS NULL THEN '-Infinity' ELSE f.piotroskiFScore END AS piotroskiFScore,
                CASE WHEN f.roe IS NULL THEN '-Infinity' ELSE f.roe END AS roe,
                m.industry
            FROM tiingo_eod e
            LEFT JOIN tiingo_fundamentals_meta m
            ON e.vendor_symbol_id = m.vendor_symbol_id            
            LEFT JOIN LATERAL (
                SELECT "date", currentRatio, debt, debtEquity, piotroskiFScore, roe
                -- It doesn't matter if it's an annual or quarterly report since this is from the balance sheet and overview. 
                FROM tiingo_fundamentals_amended_distinct fad
                WHERE e.date > '{date}' AND fad.vendor_symbol_id = e.vendor_symbol_id
                  AND date < e.date  -- Most recent metrics before the trading day
                  -- 120 day recency eliminates stocks that should've beeen marked inactive, or stopped reporting.
                  AND date > e.date - INTERVAL 120 DAY 
                ORDER BY fad.date DESC
                LIMIT 1
            ) f ON true
        ),
        PercentileRanks AS (
            SELECT
                vendor_symbol_id,
                symbol,
                trading_date,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY currentRatio) * 100 AS currentRatio_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY debt DESC) * 100 AS debt_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY debtEquity DESC) * 100 AS debtEquity_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date ORDER BY piotroskiFScore) * 100 AS piotroskiFScore_pct_rank,
                PERCENT_RANK() OVER (PARTITION BY trading_date, industry ORDER BY roe) * 100 AS roe_pct_rank
            FROM RelevantMetrics
        )
        UPDATE daily_metrics
            SET 
                currentRatio_pct_rank = pr.currentRatio_pct_rank,
                debt_pct_rank = pr.debt_pct_rank,
                debtEquity_pct_rank = pr.debtEquity_pct_rank,
                piotroskiFScore_pct_rank = pr.piotroskiFScore_pct_rank,
                roe_pct_rank = pr.roe_pct_rank
            FROM
                (SELECT * FROM PercentileRanks) AS pr
            WHERE
                daily_metrics.vendor_symbol_id = pr.vendor_symbol_id AND
                daily_metrics.date = pr.trading_date;
            """
        try:
            con.execute(query)
            msg = "balance sheet metrics percentile ranks calculated."
            print(msg)
            logger.info(msg)
        except Exception as e:
            msg = f"Error calculating balance sheet metrics percentile ranks: {e}"
            print(msg)
            logger.error(msg)
            return 'fail'


def rank(date='2024-07-01'):
    """Calculate percentile ranking of many different metrics. This can take several minutes.
    As a reminder, your DB files will be inaccessible by other processes while the job runs, even for read-only."""
    # Note: I tried running in a threadpool but it seems memory I/O is the main choke point for some of these very large
    # queries.
    enterprise_multiple_rank(date)
    pe_rank(date)


def create_consolidated_metrics(date='2005-01-01', chunk_size_days=183):
    msg = "Creating consolidated_metrics table."
    print(msg)
    logger.info(msg)

    with duckdb.connect(db_file) as con:
        # Get schema information from source tables
        daily_metrics_schema = con.execute("DESCRIBE daily_metrics").fetchall()
        fundamentals_daily_schema = con.execute(f"DESCRIBE {fundamentals_daily_table}").fetchall()
        fundamentals_meta_schema = con.execute(f"DESCRIBE {fundamentals_meta_table}").fetchall()

        # Build CREATE TABLE statement dynamically
        # Start with all columns from daily_metrics
        create_columns = [f"{col[0]} {col[1]}" for col in daily_metrics_schema]

        # Add needed columns from fundamentals_daily
        additional_columns = ['pe_ratio', 'enterprise_val', 'pb_ratio']
        for col in fundamentals_daily_schema:
            if col[0] in additional_columns:
                create_columns.append(f"{col[0]} {col[1]}")

        # Add needed columns from fundamentals_meta
        meta_columns = ['name', 'industry']
        for col in fundamentals_meta_schema:
            if col[0] in meta_columns:
                create_columns.append(f"{col[0]} {col[1]}")

        create_table_query = f"""
        CREATE OR REPLACE TABLE consolidated_metrics (
            {','.join(create_columns)}
        );
        """

        # Create table with dynamically built schema
        con.execute(create_table_query)

        # Rest of the chunked processing logic remains the same
        date_range = con.execute(f"""
            SELECT MIN(date), MAX(date) 
            FROM daily_metrics 
            WHERE date >= '{date}'
        """).fetchone()

        start_date = date_range[0]
        end_date = date_range[1]

        current_date = start_date
        while current_date <= end_date:
            chunk_end = current_date + pd.Timedelta(days=chunk_size_days)

            msg = f"Processing chunk {current_date} to {chunk_end}"
            print(msg)
            logger.info(msg)

            con.execute("""
            INSERT INTO consolidated_metrics 
            SELECT dm.*,
                   fdm.pe_ratio,
                   fdm.enterprise_val,
                   fdm.pb_ratio,
                   m.name,
                   m.industry
            FROM daily_metrics dm
            JOIN """ + fundamentals_daily_table + """ fdm
             ON dm.vendor_symbol_id = fdm.vendor_symbol_id AND dm.date = fdm.date
            JOIN """ + fundamentals_meta_table + """ m
             ON dm.vendor_symbol_id = m.vendor_symbol_id
            WHERE dm.date >= ? AND dm.date < ?
            ORDER BY dm.date;
            """, [current_date, chunk_end])

            current_date = chunk_end


def create_max_returns(date='2005-01-01'):
    """This is specific to strategies that use fundamentals as an indicator.
    Calculates table with max possible returns for a given trading day until 2 weeks before the next SEC filing.
    That means a 2-week moratorium on trading while investors wait for the next earnings report.
    Uses max adjusted high for this."""
    msg = "Creating max_returns table."
    print(msg)
    logger.info(msg)
    query = f"""
    CREATE TEMP TABLE ValidTradingPeriods AS
    WITH FilingDates AS (
        SELECT
            vendor_symbol_id,
            MIN("date") AS filing_date,
            MIN("date") - 14 AS exclusion_start_date  -- Calculate the start of the exclusion period
        FROM tiingo_fundamentals_reported
        WHERE "date" > '2005-01-01'
        GROUP BY vendor_symbol_id, "year", quarter  -- Use provided year and quarter fields
    ),
    FilingDatesWithNext AS (
        SELECT
            vendor_symbol_id,
            filing_date,
            exclusion_start_date,
            LEAD(exclusion_start_date) OVER (PARTITION BY vendor_symbol_id ORDER BY filing_date) AS next_exclusion_start_date
        FROM FilingDates
    ),
    ValidTradingDates AS (
        SELECT
            vendor_symbol_id,
            "date" AS trading_date
        FROM daily_metrics
        WHERE has_min_trading_value = 1 AND "date" IN (SELECT "date" FROM tiingo_fundamentals_reported WHERE "date" > '2005-01-01') --AND "date" > '2005-01-01'
    )
    SELECT
        vtd.vendor_symbol_id,
        vtd.trading_date,
        (
            SELECT MIN(fd.exclusion_start_date)
            FROM FilingDatesWithNext fd
            WHERE fd.vendor_symbol_id = vtd.vendor_symbol_id AND fd.exclusion_start_date > vtd.trading_date
        ) AS next_exclusion_start_date
    FROM ValidTradingDates vtd
    WHERE NOT EXISTS (
        SELECT 1
        FROM FilingDates fd
        WHERE vtd.vendor_symbol_id = fd.vendor_symbol_id
          AND vtd.trading_date >= fd.exclusion_start_date
          AND vtd.trading_date < fd.filing_date
    )
    ORDER BY vtd.vendor_symbol_id, vtd.trading_date;
    
    -- Assuming 'ValidTradingPeriods' has vendor_symbol_id, trading_date, next_exclusion_start_date
    CREATE OR REPLACE TABLE max_returns AS
    WITH TradingPrices AS (
        SELECT
            vtp.vendor_symbol_id,
            vtp.trading_date,
            vtp.next_exclusion_start_date,
            eod.adj_close AS trading_day_close
        FROM ValidTradingPeriods vtp
        JOIN tiingo_eod eod
        ON vtp.vendor_symbol_id = eod.vendor_symbol_id AND vtp.trading_date = eod.date
    ),
    MaxHighs AS (
        SELECT
            eod.vendor_symbol_id,
            MAX(eod.adj_high) AS max_adj_high,
            vtp.trading_date,
            vtp.next_exclusion_start_date
        FROM ValidTradingPeriods vtp
        JOIN tiingo_eod eod
        ON eod.vendor_symbol_id = vtp.vendor_symbol_id
           AND eod.date > vtp.trading_date
           AND eod.date < vtp.next_exclusion_start_date
        GROUP BY eod.vendor_symbol_id, vtp.trading_date, vtp.next_exclusion_start_date
    )
    
    SELECT
        tp.vendor_symbol_id,
        tp.trading_date,
        tp.trading_day_close,
        mh.max_adj_high,
        (mh.max_adj_high - tp.trading_day_close) / tp.trading_day_close AS max_return_percentage
    FROM TradingPrices tp
    JOIN MaxHighs mh
    ON tp.vendor_symbol_id = mh.vendor_symbol_id
       AND tp.trading_date = mh.trading_date
       AND tp.next_exclusion_start_date = mh.next_exclusion_start_date
    ORDER BY tp.vendor_symbol_id, tp.trading_date;"""

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import talib

from mallard.tiingo.tiingo_util import config, logger

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']


def calculate_rsi_core(df, period=14, price_col='adj_close', group_col=None):
    """
    Core RSI calculation logic shared between batch and live calculations.
    
    Args:
        df: DataFrame with price data
        period: RSI period (default 14)
        price_col: Column name for prices ('adj_close')
        group_col: Column to group by (e.g., 'vsid' for batch, None for single series)
        include_open_calc: Whether to calculate open-based RSI (deprecated, kept for compatibility)
    
    Returns:
        DataFrame with RSI columns added
    """
    if group_col:
        # Use Wilder's smoothing consistently
        df['price_change'] = df.groupby(group_col)[price_col].diff()
        df['gain'] = np.where(df['price_change'] > 0, df['price_change'], 0)
        df['loss'] = np.where(df['price_change'] < 0, -df['price_change'], 0)
        
        # Wilder's smoothing: first value is SMA, then exponential with alpha=1/period
        def wilders_smooth(x):
            # Convert to numpy array to ensure 0-based indexing
            values = x.values
            result = np.full_like(values, np.nan)
            valid_mask = ~np.isnan(values)
            
            if valid_mask.sum() < period:
                # Return Series with original index
                return pd.Series(result, index=x.index)
            
            # First smoothed value is SMA of first 'period' values
            first_avg_idx = period - 1
            result[first_avg_idx] = values[valid_mask][:period].mean()
            
            # Subsequent values use Wilder's formula
            for i in range(first_avg_idx + 1, len(values)):
                if valid_mask[i]:
                    result[i] = (result[i-1] * (period - 1) + values[i]) / period
                else:
                    result[i] = result[i-1]  # carry forward
            
            # Return Series with original index
            return pd.Series(result, index=x.index)
                
        df['avg_gain'] = df.groupby(group_col)['gain'].transform(wilders_smooth)
        df['avg_loss'] = df.groupby(group_col)['loss'].transform(wilders_smooth)
        
        # Calculate RSI
        df['rs'] = df['avg_gain'] / df['avg_loss']
        df['rsi'] = 100 - (100 / (1 + df['rs']))
        
        # Handle division by zero (when avg_loss is 0, RSI should be 100)
        df['rsi'] = np.where(df['avg_loss'] == 0, 100, df['rsi'])
        df['rsi'] = np.clip(df['rsi'], 0, 100)
        
        # Clean up temporary columns
        temp_cols = ['price_change', 'gain', 'loss', 'rs']
        df = df.drop(columns=[col for col in temp_cols if col in df.columns])
        
    else:
        # Single series calculation (for live trading)
        df['price_change'] = df[price_col].diff()
        df['gain'] = np.where(df['price_change'] > 0, df['price_change'], 0)
        df['loss'] = np.where(df['price_change'] < 0, -df['price_change'], 0)
        
        df['avg_gain'] = df['gain'].ewm(alpha=1/period, adjust=False).mean()
        df['avg_loss'] = df['loss'].ewm(alpha=1/period, adjust=False).mean()
        
        df['rs'] = df['avg_gain'] / df['avg_loss']
        df['rsi'] = 100 - (100 / (1 + df['rs']))
        df['rsi'] = np.where(df['avg_loss'] == 0, 100, df['rsi'])
        df['rsi'] = np.clip(df['rsi'], 0, 100)
        
        # Clean up
        df = df.drop(columns=['price_change', 'gain', 'loss', 'rs'])
    
    return df


def bulk_rsi_calculation(duckdb_con, start_date, period=14):
    """
    Calculate RSI metrics for all stocks using vectorized operations.
    """
    try:
        # Calculate appropriate lookback
        lookback_days = max(period * 3, 50)  # At least 50 days, or 3x period
        
        with duckdb_con.cursor() as con:
            # Find earliest date needing RSI calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (rsi IS NULL) AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            print(f"DEBUG: earliest_date = {earliest_date}")
            
            if earliest_date is None:
                print("No data needs RSI calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Load data with sufficient lookback for RSI calculation (need ~3x period for EMA to stabilize)
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=lookback_days)
            print(f"Loading data from {lookback_date} for RSI calculations")
            
            # Single query to load all needed data
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
            print(f"DEBUG: Loaded {len(df)} rows from database")
            
            if df.empty:
                print("DEBUG: Empty dataframe after loading - SKIP")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            df['date'] = pd.to_datetime(df['date'])
            
            # Use shared core calculation logic
            df = calculate_rsi_core(df, period=14, price_col='adj_close', group_col='vsid')
            print(f"DEBUG: After RSI calculation, df shape: {df.shape}")
            
            # Filter to only update rows from earliest_date forward
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            print(f"DEBUG: After filtering to earliest_date, update_df shape: {update_df.shape}")
            
            if not update_df.empty:
                # Validate RSI values are in reasonable range
                invalid_rsi = (update_df['rsi'] < 0) | (update_df['rsi'] > 100)
                if invalid_rsi.any():
                    logger.warning(f"Found {invalid_rsi.sum()} invalid RSI values, clipping to [0,100]")
                    update_df['rsi'] = np.clip(update_df['rsi'], 0, 100)

                columns_to_update = [
                    'avg_gain', 'avg_loss', 'rsi'
                ]
                
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['rsi'], how='all')
                print(f"DEBUG: After dropna, temp_df shape: {temp_df.shape}")
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} RSI records for {len(temp_df['vsid'].unique())} symbols")
                    
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
                    print("DEBUG: temp_df is empty after dropna - SKIP")
            else:
                print("DEBUG: update_df is empty after filtering - SKIP")
            
            return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk RSI calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


@DeprecationWarning
def compute_rsi_incremental_with_state(df, vendor_symbol_id, last_date=None, con=None, period=14):
    """
    Compute RSI incrementally with state management.
    
    Args:
        df: DataFrame with date, adj_close, and adj_open columns
        vendor_symbol_id: Symbol identifier
        last_date: Last date for which we have RSI values
        con: Database connection
        period: RSI period (default 14)
        
    Returns:
        DataFrame with RSI values added
    """
    
    if con is None:
        raise ValueError("Database connection (con) must be provided.")
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Initialize arrays for calculations
    n = len(df)
    gains_arr = np.full(n, np.nan)
    losses_arr = np.full(n, np.nan)
    avg_gain_arr = np.full(n, np.nan)
    avg_loss_arr = np.full(n, np.nan)
    rsi_close_arr = np.full(n, np.nan)
    rsi_open_arr = np.full(n, np.nan)
    
    # Extract price arrays for faster access
    close_prices = df['adj_close'].values
    open_prices = df['adj_open'].values
    
    # Get previous state if available
    start_idx = 0
    row_num = 0
    
    
    if last_date is not None:
        # Ensure last_date is in the correct format for the database query
        if isinstance(last_date, pd.Timestamp):
            last_date_db = last_date.date()
        else:
            last_date_db = last_date
            
        last_state_query = """
        SELECT avg_gain_close, avg_loss_close
        FROM daily_metrics
        WHERE vendor_symbol_id = ? AND date = ?
        """
        result = con.execute(last_state_query, [vendor_symbol_id, last_date_db]).fetchone()
        
        if result and all(x is not None for x in result):
            avg_gain = result[0]
            avg_loss = result[1]
            
            # Filter to only keep new rows (plus one day overlap for continuity)
            # Convert last_date to pandas datetime for comparison with df['date']
            last_date_pd = pd.to_datetime(last_date)
            overlap_date = last_date_pd - pd.Timedelta(days=1)
            overlap_mask = df['date'] >= overlap_date
            
            if not any(overlap_mask):
                return df  # No new data
                
            # Get the subset of data we need
            df = df[overlap_mask].reset_index(drop=True)
            close_prices = df['adj_close'].values
            open_prices = df['adj_open'].values
            
            # Reinitialize arrays with new size
            n = len(df)
            gains_arr = np.full(n, np.nan)
            losses_arr = np.full(n, np.nan)
            avg_gain_arr = np.full(n, np.nan)
            avg_loss_arr = np.full(n, np.nan)
            rsi_close_arr = np.full(n, np.nan)
            rsi_open_arr = np.full(n, np.nan)
            
            # If we have the overlap row, set its values
            # Convert both to the same type for comparison
            if len(df) > 0:
                first_row_date = pd.to_datetime(df.iloc[0]['date']).date()
                last_date_date = last_date_pd.date() if isinstance(last_date_pd, pd.Timestamp) else last_date_pd
                
                if first_row_date == last_date_date:
                    avg_gain_arr[0] = avg_gain
                    avg_loss_arr[0] = avg_loss
                    
                    # Calculate RSI for the overlap row
                    if avg_loss == 0:
                        rsi_close_arr[0] = 100
                    else:
                        rs = avg_gain / avg_loss
                        rsi_close_arr[0] = 100 - (100 / (1 + rs))
                    
                    start_idx = 1
                    row_num = period + 1
                else:
                    start_idx = 0
                    row_num = period  # Assuming we're past warmup if we have previous state
            else:
                start_idx = 0
                row_num = period
        else:
            # No valid previous state, initialize from scratch
            start_idx = 0
            row_num = 0
            avg_gain = None
            avg_loss = None
    else:
        # No previous state, initialize from scratch
        start_idx = 0
        row_num = 0
        avg_gain = None
        avg_loss = None
    
    # If we're starting from scratch, we need to calculate initial averages
    if avg_gain is None or avg_loss is None:
        # Need at least period+1 data points to calculate first RSI
        if len(close_prices) <= period:
            return df  # Not enough data
        
        # Calculate price changes
        for i in range(1, period+1):
            if np.isnan(close_prices[i-1]) or np.isnan(close_prices[i]):
                continue
                
            change = close_prices[i] - close_prices[i-1]
            if change > 0:
                gains_arr[i] = change
                losses_arr[i] = 0
            else:
                gains_arr[i] = 0
                losses_arr[i] = abs(change)
        
        # Calculate initial averages
        avg_gain = np.nanmean(gains_arr[1:period+1])
        avg_loss = np.nanmean(losses_arr[1:period+1])
        
        # Store initial averages
        avg_gain_arr[period] = avg_gain
        avg_loss_arr[period] = avg_loss
        
        # Calculate initial RSI
        if avg_loss == 0:
            rsi_close_arr[period] = 100
        else:
            rs = avg_gain / avg_loss
            rsi_close_arr[period] = 100 - (100 / (1 + rs))
            
        
        start_idx = period + 1
        row_num = period + 1
    
    # Main calculation loop
    for i in range(start_idx, n):
        row_num += 1
        
        # Handle the case where i == 0 or either price is NaN
        if i == 0 or np.isnan(close_prices[i]) or np.isnan(close_prices[i-1]):
            # For i == 0, we can't calculate a change
            # For NaN values, we can't calculate a valid change
            # Carry forward previous values if available
            if i > 0 and not np.isnan(avg_gain_arr[i-1]):
                avg_gain_arr[i] = avg_gain_arr[i-1]
                avg_loss_arr[i] = avg_loss_arr[i-1]
                rsi_close_arr[i] = rsi_close_arr[i-1]
            continue
        
        # Calculate price change for close
        change = close_prices[i] - close_prices[i-1]
        
        # Determine gain or loss
        if change > 0:
            gain = change
            loss = 0
        else:
            gain = 0
            loss = abs(change)
        
        # Store raw gains and losses
        gains_arr[i] = gain
        losses_arr[i] = loss
        
        # Get previous averages
        prev_avg_gain = avg_gain_arr[i-1] if not np.isnan(avg_gain_arr[i-1]) else avg_gain
        prev_avg_loss = avg_loss_arr[i-1] if not np.isnan(avg_loss_arr[i-1]) else avg_loss
        
        # Calculate smoothed averages (similar to EMA calculation)
        avg_gain = (prev_avg_gain * (period - 1) + gain) / period
        avg_loss = (prev_avg_loss * (period - 1) + loss) / period
        
        # Store averages and RSI only if past warmup period
        if row_num > period:
            avg_gain_arr[i] = avg_gain
            avg_loss_arr[i] = avg_loss
            
            # Calculate RSI
            if avg_loss == 0:
                rsi_close_arr[i] = 100
            else:
                rs = avg_gain / avg_loss
                rsi_close_arr[i] = 100 - (100 / (1 + rs))
                rsi_close_arr[i] = np.clip(rsi_close_arr[i], 0, 100)  # Ensure value is in [0, 100]
            
            # Calculate open-based RSI if we have valid open price and previous state
            if not np.isnan(open_prices[i]) and i > 0 and not np.isnan(close_prices[i-1]):
                # Use previous day's avg_gain and avg_loss as the base
                open_change = open_prices[i] - close_prices[i-1]
                
                if open_change > 0:
                    open_gain = open_change
                    open_loss = 0
                else:
                    open_gain = 0
                    open_loss = abs(open_change)
                
                # Calculate open RSI using previous day's smoothed averages
                open_avg_gain = (prev_avg_gain * (period - 1) + open_gain) / period
                open_avg_loss = (prev_avg_loss * (period - 1) + open_loss) / period
                
                # Calculate open RSI
                if open_avg_loss == 0:
                    rsi_open_arr[i] = 100
                else:
                    open_rs = open_avg_gain / open_avg_loss
                    rsi_open_arr[i] = 100 - (100 / (1 + open_rs))
                    rsi_open_arr[i] = np.clip(rsi_open_arr[i], 0, 100)  # Ensure value is in [0, 100]
        else:
            # During warmup period, still store the avg_gain and avg_loss for continuity
            # but don't calculate RSI yet
            avg_gain_arr[i] = avg_gain
            avg_loss_arr[i] = avg_loss    
    
    # Assign results back to DataFrame
    df['avg_gain_close'] = avg_gain_arr
    df['avg_loss_close'] = avg_loss_arr
    df['rsi_close'] = rsi_close_arr
    df['rsi_open'] = rsi_open_arr
    
    return df


@DeprecationWarning
def update_rsi_batch(con, vendor_symbol_id, df):
    """
    Update RSI values in daily_metrics table using DuckDB's direct DataFrame update.
    
    Args:
        con: Database connection
        vendor_symbol_id: Symbol identifier
        df: DataFrame with calculated RSI values
    """
    # Filter out rows with no RSI values (during warmup period)
    df_valid = df.dropna(subset=['rsi_close', 'rsi_open']).copy()
    
    if df_valid.empty:
        return
    
    # Add vendor_symbol_id column if not present
    if 'vendor_symbol_id' not in df_valid.columns:
        df_valid['vendor_symbol_id'] = vendor_symbol_id
    
    # Execute the update using DuckDB's direct DataFrame update
    con.execute("""
        UPDATE daily_metrics
        SET 
            avg_gain_close = df_valid.avg_gain_close,
            avg_loss_close = df_valid.avg_loss_close,
            rsi_close = df_valid.rsi_close,
            rsi_open = df_valid.rsi_open
        FROM df_valid
        WHERE daily_metrics.vendor_symbol_id = df_valid.vendor_symbol_id
          AND daily_metrics.date = df_valid.date
    """)

@DeprecationWarning
def run_rsi_pipeline(vendor_symbol_id, duckdb_con, period=14):
    """
    Run the complete RSI calculation pipeline with proper state management.
    
    Args:
        vendor_symbol_id: Symbol identifier
        duckdb_con: DuckDB connection
        period: RSI period (default 14)
        
    Returns:
        str: 'success', 'skip', or 'fail'
    """
    try:
        with duckdb_con.cursor() as con:
            # Begin transaction
            con.execute("BEGIN TRANSACTION")
            
            # Check for existing RSI data - ensure both close and open RSI exist
            last_date_query = """
            SELECT MAX(date) 
            FROM daily_metrics
            WHERE vendor_symbol_id = ? AND rsi_close IS NOT NULL AND rsi_open IS NOT NULL
            """
            last_date = con.execute(last_date_query, [vendor_symbol_id]).fetchone()[0]
            
            # If we have previous calculations, check if there's any new data
            if last_date is not None:
                # Convert last_date to pandas datetime for consistent comparisons
                last_date_pd = pd.to_datetime(last_date)
                
                # Check if there are any new data points
                new_data_check_query = """
                SELECT EXISTS(
                    SELECT 1 
                    FROM tiingo_eod 
                    WHERE vendor_symbol_id = ? AND date > ?
                )
                """
                has_new_data = con.execute(new_data_check_query, [vendor_symbol_id, last_date]).fetchone()[0]
                
                if not has_new_data:
                    # No new data, skip calculation
                    con.execute("ROLLBACK")
                    return 'skip'
                
                # Get the last state for continuity
                last_state_query = """
                SELECT date, avg_gain_close, avg_loss_close
                FROM daily_metrics
                WHERE vendor_symbol_id = ? AND date = ? AND rsi_close IS NOT NULL AND rsi_open IS NOT NULL
                ORDER BY date DESC
                LIMIT 1
                """
                last_state = con.execute(last_state_query, [vendor_symbol_id, last_date]).fetchone()
                
                if last_state:
                    # Important: Load data starting from a few days BEFORE the last date
                    # This ensures we have enough context for the open-based RSI calculation
                    lookback_start = last_date_pd - pd.Timedelta(days=5)  # Add extra buffer
                    
                    query = """
                    SELECT date, adj_open, adj_close
                    FROM tiingo_eod
                    WHERE vendor_symbol_id = ? AND date >= ?
                    ORDER BY date
                    """
                    df = con.execute(query, [vendor_symbol_id, lookback_start]).df()
                    
                    # If somehow we got no rows (race condition?), skip
                    if len(df) == 0:
                        con.execute("ROLLBACK")
                        return 'skip'
                    
                    # Convert date to datetime
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Add the last state as the first row for continuity
                    last_row = pd.DataFrame({
                        'date': [pd.to_datetime(last_state[0])],
                        'adj_open': [np.nan],  # Use np.nan instead of None
                        'adj_close': [np.nan], # Use np.nan instead of None
                        'avg_gain_close': [last_state[1]],
                        'avg_loss_close': [last_state[2]]
                    })                    
                    
                    # Concatenate and sort
                    df = pd.concat([last_row, df], ignore_index=True)
                    df = df.sort_values('date').reset_index(drop=True)
                    
                    # Calculate RSI
                    df_rsi = compute_rsi_incremental_with_state(df, vendor_symbol_id, last_date_pd, con, period)
                    
                    # Remove the first row if it was the last state row we added
                    if len(df_rsi) > 1:
                        # Convert both to the same type for comparison
                        first_row_date = pd.to_datetime(df_rsi.iloc[0]['date']).date()
                        last_date_date = last_date_pd.date() if isinstance(last_date_pd, pd.Timestamp) else last_date_pd
                        
                        if first_row_date == last_date_date:
                            df_rsi = df_rsi.iloc[1:].reset_index(drop=True)
                    
                    # Filter to only include new dates (after last_date)
                    df_rsi = df_rsi[df_rsi['date'] > last_date_pd].reset_index(drop=True)
                    
                else:
                    # No valid last state, load all data
                    query = """
                    SELECT date, adj_open, adj_close
                    FROM tiingo_eod
                    WHERE vendor_symbol_id = ?
                    ORDER BY date
                    """
                    df = con.execute(query, [vendor_symbol_id]).df()
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Calculate RSI
                    df_rsi = compute_rsi_incremental_with_state(df, vendor_symbol_id, None, con, period)
            else:
                # No previous calculations, load all data
                query = """
                SELECT date, adj_open, adj_close
                FROM tiingo_eod
                WHERE vendor_symbol_id = ?
                ORDER BY date
                """
                df = con.execute(query, [vendor_symbol_id]).df()
                df['date'] = pd.to_datetime(df['date'])
                
                # Check if we have enough data for initial calculation
                if len(df) < period + 1:  # Need at least period+1 rows for initial calculation
                    con.execute("ROLLBACK")
                    return 'skip'
                
                # Calculate RSI
                df_rsi = compute_rsi_incremental_with_state(df, vendor_symbol_id, None, con, period)
            
            # If we have no rows to update (all filtered out), skip
            if df_rsi.empty or df_rsi.dropna(subset=['rsi_close', 'rsi_open']).empty:
                con.execute("ROLLBACK")
                return 'skip'
            
            # Update database
            update_rsi_batch(con, vendor_symbol_id, df_rsi)
            
            # Commit transaction
            con.execute("COMMIT")
            return 'success'
            
    except Exception as e:
        # Rollback on error
        try:
            con.execute("ROLLBACK")
        except:
            pass
        
        logger.error(f"run_rsi_pipeline failed for {vendor_symbol_id}: {e}")
        return 'fail'


@DeprecationWarning
def update_rsi(vendor_symbol_id: str, duckdb_con: duckdb.DuckDBPyConnection, start_date='1990-06-01',
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
        price_data = con.execute(sql_query).pl()

        # Check if data is sufficient
        if price_data.is_empty():
            print(f"update_rsi: No data available for params: {vendor_symbol_id}, {start_date}, {end_date}.")
            return 'skip'

        # Calculate RSI using TA lib
        rsi = talib.RSI(price_data['adj_close'].to_numpy(), timeperiod=14)
        # Prepare DataFrame to insert results
        results = pl.DataFrame({
            'vendor_symbol_id': vendor_symbol_id,
            'date': price_data['date'],
            'rsi': rsi
        })
        # Clean data (remove NaNs due to SMA calculation starting period)
        results = results.filter(
            pl.col("rsi").is_not_null()
        )
        # Insert/update the daily_metrics table via UPDATE statement
        query = """
                UPDATE daily_metrics
                SET rsi = results.rsi FROM results
                WHERE daily_metrics.vendor_symbol_id = results.vendor_symbol_id
                  AND daily_metrics.date = results.date;"""
        con.execute(query)
    return 'success'

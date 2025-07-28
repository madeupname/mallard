import duckdb
import numpy as np
import pandas as pd

from mallard.tiingo.tiingo_util import logger, config

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']


import duckdb
import numpy as np
import pandas as pd
from mallard.tiingo.tiingo_util import logger, config

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']

def calculate_ema_vectorized(prices, period, prev_ema=None):
    """
    Vectorized EMA calculation that can handle both single series and grouped data.
    
    Args:
        prices: Price series or DataFrame column
        period: EMA period
        prev_ema: Previous EMA value for continuation (optional)
    """
    alpha = 2 / (period + 1)
    
    if prev_ema is not None:
        # For incremental calculation
        return alpha * prices + (1 - alpha) * prev_ema
    else:
        # For full series calculation
        return prices.ewm(span=period, adjust=False).mean()


def calculate_ema_update(current_price, prev_ema, period):
    """Shared EMA calculation formula"""
    alpha = 2 / (period + 1)
    return alpha * current_price + (1 - alpha) * prev_ema

def calculate_macd_from_emas(ema_fast, ema_slow):
    """Shared MACD calculation formula"""
    return ema_fast - ema_slow

def calculate_signal_update(current_macd, prev_signal, period=9):
    """Shared MACD signal calculation formula"""
    alpha = 2 / (period + 1)
    return alpha * current_macd + (1 - alpha) * prev_signal


def calculate_macd_core(df, fastperiod=12, slowperiod=26, signalperiod=9, 
                       price_col='adj_close', group_col=None, include_open_calc=True):
    """
    Core MACD calculation logic shared between batch and live calculations.
    
    Args:
        df: DataFrame with price data
        fastperiod, slowperiod, signalperiod: MACD parameters
        price_col: Column name for prices ('adj_close' or 'adj_open')
        group_col: Column to group by (e.g., 'vsid' for batch, None for single series)
        include_open_calc: Whether to calculate open-based indicators
    
    Returns:
        DataFrame with MACD columns added
    """
    if group_col:
        # Bulk processing - vectorized for all symbols
        df['ema_fast'] = df.groupby(group_col)[price_col].transform(
            lambda x: x.ewm(span=fastperiod, adjust=False).mean()
        )
        df['ema_slow'] = df.groupby(group_col)[price_col].transform(
            lambda x: x.ewm(span=slowperiod, adjust=False).mean()
        )
        df['macd'] = df['ema_fast'] - df['ema_slow']
        df['macd_signal'] = df.groupby(group_col)['macd'].transform(
            lambda x: x.ewm(span=signalperiod, adjust=False).mean()
        )
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        if include_open_calc and 'adj_open' in df.columns:
            # Calculate open-based indicators vectorized
            df['prev_ema_fast'] = df.groupby(group_col)['ema_fast'].shift(1)
            df['prev_ema_slow'] = df.groupby(group_col)['ema_slow'].shift(1)
            df['prev_macd_signal'] = df.groupby(group_col)['macd_signal'].shift(1)
            
            alpha_fast = 2 / (fastperiod + 1)
            alpha_slow = 2 / (slowperiod + 1)
            alpha_signal = 2 / (signalperiod + 1)
            
            df['ema_fast_open'] = np.where(
                df['prev_ema_fast'].notna() & df['adj_open'].notna(),
                alpha_fast * df['adj_open'] + (1 - alpha_fast) * df['prev_ema_fast'],
                np.nan
            )
            
            df['ema_slow_open'] = np.where(
                df['prev_ema_slow'].notna() & df['adj_open'].notna(),
                alpha_slow * df['adj_open'] + (1 - alpha_slow) * df['prev_ema_slow'],
                np.nan
            )
            
            df['macd_open'] = df['ema_fast_open'] - df['ema_slow_open']
            
            df['macd_signal_open'] = np.where(
                df['prev_macd_signal'].notna() & df['macd_open'].notna(),
                alpha_signal * df['macd_open'] + (1 - alpha_signal) * df['prev_macd_signal'],
                np.nan
            )
            
            df['macd_hist_open'] = df['macd_open'] - df['macd_signal_open']
            
            # Clean up temporary columns
            df = df.drop(columns=['prev_ema_fast', 'prev_ema_slow', 'prev_macd_signal'])
    else:
        # Single series calculation (for live trading)
        df['ema_fast'] = df[price_col].ewm(span=fastperiod, adjust=False).mean()
        df['ema_slow'] = df[price_col].ewm(span=slowperiod, adjust=False).mean()
        df['macd'] = df['ema_fast'] - df['ema_slow']
        df['macd_signal'] = df['macd'].ewm(span=signalperiod, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
    
    return df


def bulk_macd_calculation(duckdb_con, start_date):
    """
    Calculate MACD metrics for all stocks using vectorized operations.
    Uses the shared calculate_macd_core function.
    """
    try:
        with duckdb_con.cursor() as con:
            # Find earliest date needing MACD calculation
            earliest_date_query = f"""
            SELECT MIN(date) FROM daily_metrics
            WHERE (macd IS NULL OR macd_open IS NULL) AND date >= '{start_date}'
            """
            earliest_date = con.execute(earliest_date_query).fetchone()[0]
            
            if earliest_date is None:
                print("No data needs MACD calculations")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            # Load data with sufficient lookback for EMA calculation
            lookback_date = pd.to_datetime(earliest_date) - pd.Timedelta(days=60)
            print(f"Loading data from {lookback_date} for MACD calculations")
            
            # Single query to load all needed data
            query = f"""
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.adj_close,
                te.adj_open
            FROM daily_metrics dm
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date >= '{lookback_date.strftime('%Y-%m-%d')}'
            ORDER BY dm.vendor_symbol_id, dm.date
            """
            df = con.execute(query).df()
            
            if df.empty:
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
            
            df['date'] = pd.to_datetime(df['date'])
            
            # Use shared core calculation logic
            df = calculate_macd_core(df, price_col='adj_close', group_col='vsid', include_open_calc=True)
            
            # Rename columns to match database schema
            df = df.rename(columns={
                'ema_fast': 'ema_12',
                'ema_slow': 'ema_26',
                'ema_fast_open': 'ema_12_open',
                'ema_slow_open': 'ema_26_open'
            })
            
            # Filter to only update rows from earliest_date forward
            update_df = df[df['date'] >= pd.to_datetime(earliest_date)].copy()
            
            if not update_df.empty:
                columns_to_update = [
                    'ema_12', 'ema_26', 'macd', 'macd_signal', 'macd_hist',
                    'ema_12_open', 'ema_26_open', 'macd_open', 'macd_signal_open', 'macd_hist_open'
                ]
                
                update_cols = ['vsid', 'date'] + columns_to_update
                temp_df = update_df[update_cols].dropna(subset=['macd'], how='all')
                
                if not temp_df.empty:
                    print(f"Updating {len(temp_df)} MACD records for {len(temp_df['vsid'].unique())} symbols")
                    
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
            
            return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}
                
    except Exception as e:
        print(f"Error in bulk MACD calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def compute_macd_live_open(df, fastperiod=12, slowperiod=26, signalperiod=9):
    """
    Optimized live MACD calculation for trading bot.
    Uses the shared calculate_macd_core function but optimized for speed.
    """
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure date is in datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Extract only the latest two rows for each symbol for efficiency
    latest_two_rows = df.sort_values(['vsid', 'date']) \
                        .groupby('vsid') \
                        .tail(2) \
                        .reset_index(drop=True)
    
    if len(latest_two_rows) < 2:
        return df
    
    # For live calculation, we only need the open-based calculation
    # using previous day's close-based EMA values with current open
    latest_dates = latest_two_rows.groupby('vsid')['date'].transform('max')
    latest_mask = (latest_two_rows['date'] == latest_dates)
    prev_mask = ~latest_mask
    
    prev_values = latest_two_rows[prev_mask].set_index('vsid')
    latest_rows = latest_two_rows[latest_mask].set_index('vsid')
    
    # Skip symbols that don't have required data
    valid_symbols = latest_rows.index.intersection(prev_values.index)
    valid_symbols = valid_symbols[~latest_rows.loc[valid_symbols, 'adj_open'].isna()]
    valid_symbols = valid_symbols[~prev_values.loc[valid_symbols, 'ema_12'].isna()]
    valid_symbols = valid_symbols[~prev_values.loc[valid_symbols, 'ema_26'].isna()]
    valid_symbols = valid_symbols[~prev_values.loc[valid_symbols, 'macd_signal'].isna()]
    
    if len(valid_symbols) == 0:
        return df
    
    # Use shared calculation logic for the mathematical operations
    alpha_fast = 2 / (fastperiod + 1)
    alpha_slow = 2 / (slowperiod + 1)
    alpha_signal = 2 / (signalperiod + 1)
    
    results = pd.DataFrame(index=valid_symbols)
    
    # Get values for calculations
    latest_open = latest_rows.loc[valid_symbols, 'adj_open']
    prev_ema_12 = prev_values.loc[valid_symbols, 'ema_12']
    prev_ema_26 = prev_values.loc[valid_symbols, 'ema_26']
    prev_macd_signal = prev_values.loc[valid_symbols, 'macd_signal']
    
    # Calculate using the same formulas as in calculate_macd_core
    results['ema_12_open'] = alpha_fast * latest_open + (1 - alpha_fast) * prev_ema_12
    results['ema_26_open'] = alpha_slow * latest_open + (1 - alpha_slow) * prev_ema_26
    results['macd_open'] = results['ema_12_open'] - results['ema_26_open']
    results['macd_signal_open'] = alpha_signal * results['macd_open'] + (1 - alpha_signal) * prev_macd_signal
    results['macd_hist_open'] = results['macd_open'] - results['macd_signal_open']
    
    # Add date column for merging
    results['date'] = latest_rows.loc[valid_symbols, 'date'].values
    results = results.reset_index()
    
    # Merge results back to the original dataframe
    df['_merge_key'] = df['vsid'].astype(str) + '_' + df['date'].dt.strftime('%Y-%m-%d')
    results['_merge_key'] = results['vsid'].astype(str) + '_' + results['date'].dt.strftime('%Y-%m-%d')
    
    # Update the original dataframe with calculated values
    for col in ['ema_12_open', 'ema_26_open', 'macd_open', 'macd_signal_open', 'macd_hist_open']:
        update_dict = dict(zip(results['_merge_key'], results[col]))
        mask = df['_merge_key'].isin(results['_merge_key'])
        df.loc[mask, col] = df.loc[mask, '_merge_key'].map(update_dict)
    
    # Remove temporary merge key
    df = df.drop(columns=['_merge_key'])
    
    return df


@DeprecationWarning
def compute_macd_incremental_with_state(df, vendor_symbol_id, last_date=None, con=None,
                                        fastperiod=12, slowperiod=26, signalperiod=9):
    """Optimized version using NumPy for faster calculations"""
    alpha_fast = 2 / (fastperiod + 1)
    alpha_slow = 2 / (slowperiod + 1)
    alpha_signal = 2 / (signalperiod + 1)
    warmup = slowperiod + signalperiod

    # Create a copy to avoid modifying the original
    df = df.copy()

    # Initialize arrays for calculations
    n = len(df)
    ema_fast_arr = np.full(n, np.nan)
    ema_slow_arr = np.full(n, np.nan)
    macd_arr = np.full(n, np.nan)
    signal_arr = np.full(n, np.nan)
    hist_arr = np.full(n, np.nan)

    # Open-based arrays
    ema_fast_open_arr = np.full(n, np.nan)
    ema_slow_open_arr = np.full(n, np.nan)
    macd_open_arr = np.full(n, np.nan)
    signal_open_arr = np.full(n, np.nan)
    hist_open_arr = np.full(n, np.nan)

    # Extract price arrays for faster access
    close_prices = df['adj_close'].values
    open_prices = df['adj_open'].values

    # Get previous state if available
    start_idx = 0
    row_num = 0

    if last_date is not None and con is not None:
        # Ensure last_date is in the correct format for the database query
        if isinstance(last_date, pd.Timestamp):
            last_date_db = last_date.date()
        else:
            last_date_db = last_date

        last_state_query = """
                           SELECT ema_12, ema_26, macd_signal
                           FROM daily_metrics
                           WHERE vendor_symbol_id = ? AND date = ? \
                           """
        result = con.execute(last_state_query, [vendor_symbol_id, last_date_db]).fetchone()

        if result and all(x is not None for x in result):
            ema_fast = result[0]
            ema_slow = result[1]
            signal = result[2]

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
            ema_fast_arr = np.full(n, np.nan)
            ema_slow_arr = np.full(n, np.nan)
            macd_arr = np.full(n, np.nan)
            signal_arr = np.full(n, np.nan)
            hist_arr = np.full(n, np.nan)
            ema_fast_open_arr = np.full(n, np.nan)
            ema_slow_open_arr = np.full(n, np.nan)
            macd_open_arr = np.full(n, np.nan)
            signal_open_arr = np.full(n, np.nan)
            hist_open_arr = np.full(n, np.nan)

            # If we have the overlap row, set its values
            # Convert both to the same type for comparison
            if len(df) > 0:
                first_row_date = pd.to_datetime(df.iloc[0]['date']).date()
                last_date_date = last_date_pd.date() if isinstance(last_date_pd, pd.Timestamp) else last_date_pd

                if first_row_date == last_date_date:
                    ema_fast_arr[0] = ema_fast
                    ema_slow_arr[0] = ema_slow
                    macd_arr[0] = ema_fast - ema_slow
                    signal_arr[0] = signal
                    hist_arr[0] = (ema_fast - ema_slow) - signal
                    start_idx = 1
                    row_num = warmup + 1
                else:
                    start_idx = 0
                    row_num = warmup  # Assuming we're past warmup if we have previous state
            else:
                start_idx = 0
                row_num = warmup
        else:
            # No valid previous state, initialize from scratch
            ema_fast = np.mean(close_prices[:min(fastperiod, len(close_prices))])
            ema_slow = np.mean(close_prices[:min(slowperiod, len(close_prices))])
            signal = ema_fast - ema_slow
    else:
        # No previous state, initialize from scratch
        ema_fast = np.mean(close_prices[:min(fastperiod, len(close_prices))])
        ema_slow = np.mean(close_prices[:min(slowperiod, len(close_prices))])
        signal = ema_fast - ema_slow

    # Main calculation loop - this is the bottleneck
    # We can't fully vectorize this due to the recursive nature of EMA calculation
    for i in range(start_idx, n):
        row_num += 1

        # Skip calculation if price is NaN
        if np.isnan(close_prices[i]):
            continue

        # EMA update using close (final)
        ema_fast = alpha_fast * close_prices[i] + (1 - alpha_fast) * ema_fast
        ema_slow = alpha_slow * close_prices[i] + (1 - alpha_slow) * ema_slow

        # Check for NaN before subtraction
        if not np.isnan(ema_fast) and not np.isnan(ema_slow):
            macd_val = ema_fast - ema_slow
            signal = alpha_signal * macd_val + (1 - alpha_signal) * signal
            hist = macd_val - signal

            # Store values if past warmup period
            if row_num > warmup:
                ema_fast_arr[i] = ema_fast
                ema_slow_arr[i] = ema_slow
                macd_arr[i] = macd_val
                signal_arr[i] = signal
                hist_arr[i] = hist
        else:
            # Handle NaN case if needed
            pass

        # Open-based indicators use previous day's EMA/Signal
        if i > 0 and not np.isnan(ema_fast_arr[i - 1]) and row_num > warmup:
            prev_ema_fast = ema_fast_arr[i - 1]
            prev_ema_slow = ema_slow_arr[i - 1]
            prev_signal = signal_arr[i - 1]

            ema_fast_open = alpha_fast * open_prices[i] + (1 - alpha_fast) * prev_ema_fast
            ema_slow_open = alpha_slow * open_prices[i] + (1 - alpha_slow) * prev_ema_slow
            macd_open = ema_fast_open - ema_slow_open
            signal_open = alpha_signal * macd_open + (1 - alpha_signal) * prev_signal
            hist_open = macd_open - signal_open

            ema_fast_open_arr[i] = ema_fast_open
            ema_slow_open_arr[i] = ema_slow_open
            macd_open_arr[i] = macd_open
            signal_open_arr[i] = signal_open
            hist_open_arr[i] = hist_open

    # Assign results back to DataFrame
    df['ema_fast'] = ema_fast_arr
    df['ema_slow'] = ema_slow_arr
    df['macd'] = macd_arr
    df['macd_signal'] = signal_arr
    df['macd_hist'] = hist_arr
    df['ema_fast_open'] = ema_fast_open_arr
    df['ema_slow_open'] = ema_slow_open_arr
    df['macd_open'] = macd_open_arr
    df['macd_signal_open'] = signal_open_arr
    df['macd_hist_open'] = hist_open_arr

    return df

@DeprecationWarning
def update_macd_batch(con, vendor_symbol_id, df):
    """
    Update MACD values in daily_metrics table using DuckDB's direct DataFrame update.
    
    Args:
        con: Database connection
        vendor_symbol_id: Symbol identifier
        df: DataFrame with calculated MACD values
    """
    # Filter out rows with no MACD values (during warmup period)
    df_valid = df.dropna(subset=['macd']).copy()

    if df_valid.empty:
        return

    # Add vendor_symbol_id column if not present
    if 'vendor_symbol_id' not in df_valid.columns:
        df_valid['vendor_symbol_id'] = vendor_symbol_id

    # Ensure column names match the database schema
    df_valid = df_valid.rename(columns={
        'ema_fast': 'ema_12',
        'ema_slow': 'ema_26',
        'macd_signal': 'macd_signal',
        'macd_hist': 'macd_hist',
        'macd_open': 'macd_open',
        'macd_signal_open': 'macd_signal_open',
        'macd_hist_open': 'macd_hist_open'
    })

    # Execute the update using DuckDB's direct DataFrame update
    con.execute("""
                UPDATE daily_metrics
                SET ema_12           = df_valid.ema_12,
                    ema_26           = df_valid.ema_26,
                    macd             = df_valid.macd,
                    macd_signal      = df_valid.macd_signal,
                    macd_hist        = df_valid.macd_hist,
                    macd_open        = df_valid.macd_open,
                    macd_signal_open = df_valid.macd_signal_open,
                    macd_hist_open   = df_valid.macd_hist_open FROM df_valid
                WHERE daily_metrics.vendor_symbol_id = df_valid.vendor_symbol_id
                  AND daily_metrics.date = df_valid.date
                """)


@DeprecationWarning  
def run_macd_pipeline(vendor_symbol_id, duckdb_con):
    """
    Run the complete MACD calculation pipeline with proper state management.
    
    Args:
        vendor_symbol_id: Symbol identifier
        duckdb_con: DuckDB connection
        
    Returns:
        str: 'success', 'skip', or 'fail'
    """
    try:
        with duckdb_con.cursor() as con:
            # Begin transaction
            con.execute("BEGIN TRANSACTION")

            # Check for existing MACD data
            last_date_query = """
                              SELECT MAX(date)
                              FROM daily_metrics
                              WHERE vendor_symbol_id = ?
                                AND macd IS NOT NULL
                              """
            last_date = con.execute(last_date_query, [vendor_symbol_id]).fetchone()[0]

            # If we have previous calculations, check if there's any new data
            if last_date is not None:
                # Convert last_date to pandas datetime for consistent comparisons
                last_date_pd = pd.to_datetime(last_date)

                # Check if there are any new data points
                new_data_check_query = """
                                       SELECT EXISTS(SELECT 1
                                                     FROM tiingo_eod
                                                     WHERE vendor_symbol_id = ? AND date > ?)
                                       """
                has_new_data = con.execute(new_data_check_query, [vendor_symbol_id, last_date]).fetchone()[0]

                if not has_new_data:
                    # No new data, skip calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # Get the last state for continuity
                last_state_query = """
                                   SELECT date, ema_12, ema_26, macd_signal
                                   FROM daily_metrics
                                   WHERE vendor_symbol_id = ? AND date = ? AND macd IS NOT NULL
                                   """
                last_state = con.execute(last_state_query, [vendor_symbol_id, last_date]).fetchone()

                if last_state:
                    # Only load new data since the last calculation
                    query = """
                            SELECT date, adj_open, adj_close
                            FROM tiingo_eod
                            WHERE vendor_symbol_id = ? AND date > ?
                            ORDER BY date
                            """
                    df = con.execute(query, [vendor_symbol_id, last_date]).df()

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
                        'adj_close': [np.nan],  # Use np.nan instead of None
                        'ema_fast': [last_state[1]],
                        'ema_slow': [last_state[2]],
                        'macd': [last_state[1] - last_state[2]],
                        'macd_signal': [last_state[3]],
                        'macd_hist': [last_state[1] - last_state[2] - last_state[3]]
                    })

                    # Concatenate and sort
                    df = pd.concat([last_row, df], ignore_index=True)
                    df = df.sort_values('date').reset_index(drop=True)
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
            else:
                # No previous calculations, load all data
                query = """
                        SELECT date, adj_open, adj_close
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ?
                        ORDER BY date \
                        """
                df = con.execute(query, [vendor_symbol_id]).df()
                df['date'] = pd.to_datetime(df['date'])

                # Check if we have enough data for initial calculation
                if len(df) < 35:  # Need at least 35 rows for initial calculation
                    con.execute("ROLLBACK")
                    return 'skip'

            # Calculate MACD
            df_macd = compute_macd_incremental_with_state(df, vendor_symbol_id,
                                                          last_date_pd if last_date is not None else None, con)

            # Remove the first row if it was the last state row we added
            if last_date is not None and len(df_macd) > 1:
                # Convert both to the same type for comparison
                first_row_date = pd.to_datetime(df_macd.iloc[0]['date']).date()
                last_date_date = last_date_pd.date()

                if first_row_date == last_date_date:
                    df_macd = df_macd.iloc[1:].reset_index(drop=True)

            # If we have no rows to update (all filtered out), skip
            if df_macd.empty or df_macd.dropna(subset=['macd']).empty:
                con.execute("ROLLBACK")
                return 'skip'

            # Update database
            update_macd_batch(con, vendor_symbol_id, df_macd)

            # Commit transaction
            con.execute("COMMIT")
            return 'success'

    except Exception as e:
        # Rollback on error
        try:
            con.execute("ROLLBACK")
        except:
            pass

        logger.error(f"run_macd_pipeline failed for {vendor_symbol_id}: {e}")
        return 'fail'


@DeprecationWarning
def update_macd(vendor_symbol_id: str, duckdb_con: duckdb.DuckDBPyConnection, start_date='1990-06-01', end_date=None,
                weekly=False):
    """
    TODO DELETE THIS LEGACY FUNCTION
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
        raise ValueError("Database connection is not provided.")

    try:
        with duckdb_con.cursor() as con:
            # Fetch the price data
            if weekly:
                sql_query = f"""
                SELECT
                    week_start_date AS "date",
                    MAX(weekly_close) AS adj_close
                FROM (
                    SELECT
                        DATE_TRUNC('week', "date") AS week_start_date,
                        LAST_VALUE(adj_close) OVER (
                            PARTITION BY DATE_TRUNC('week', "date")
                            ORDER BY date
                            RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                        ) AS weekly_close
                    FROM {eod_table}
                    WHERE vendor_symbol_id = '{vendor_symbol_id}'
                        AND date >= '{start_date}'
                ) sub
                GROUP BY week_start_date
                ORDER BY week_start_date ASC;
                """
            else:
                sql_query = f"""
                SELECT "date", adj_close
                FROM {eod_table}
                WHERE vendor_symbol_id = '{vendor_symbol_id}' 
                    AND "date" >= '{start_date}' 
                    {f"AND date <= '{end_date}'" if end_date else ""}
                ORDER BY date ASC;
                """
            price_data = con.execute(sql_query).df()

            # MACD calculation requires at least 34 data points, despite a 26-day EMA being used.
            if price_data.shape[0] < 34:
                logger.info(f"update_macd: No data available for params: {vendor_symbol_id}, {start_date}, {end_date}.")
                return 'skip'

            # Calculate MACD using TA-Lib
            macd, macdsignal, macdhist = talib.MACD(price_data['adj_close'].to_numpy(),
                                                    fastperiod=12, slowperiod=26, signalperiod=9)

            # Prepare Pandas DataFrame to insert results
            results = pd.DataFrame({
                'vendor_symbol_id': vendor_symbol_id,
                'date': price_data['date'],
                'macd': macd,
                'macd_signal': macdsignal,
                'macd_hist': macdhist
            })

            # Clean data (remove NaNs due to MACD calculation starting period)
            results = results.dropna(subset=['macd', 'macd_signal', 'macd_hist'])
            results.reset_index(drop=True, inplace=True)

            if weekly:
                # Forward fill the MACD values
                # Expand results to include every date in the range from 'week_start_date' to the date before the next 'week_start_date'

                # Create a date range that covers all dates you need
                dates_range = pd.date_range(start=results['date'].min(), end=datetime.now().date(), freq='D')

                # Create a DataFrame from this range
                expanded_dates = pd.DataFrame({'date': dates_range})

                # Merge with results on 'date'
                expanded_results = pd.merge(expanded_dates, results, on='date', how='left')

                # Forward fill the MACD values
                expanded_results.ffill(inplace=True)

                con.execute("""
                            UPDATE daily_metrics
                            SET weekly_macd        = expanded_results.macd,
                                weekly_macd_signal = expanded_results.macd_signal,
                                weekly_macd_hist   = expanded_results.macd_hist FROM expanded_results
                            WHERE daily_metrics.vendor_symbol_id = expanded_results.vendor_symbol_id
                              AND daily_metrics.date = expanded_results.date;
                            """)
            else:
                # Update the daily_metrics table
                con.execute("""
                            UPDATE daily_metrics
                            SET macd        = results.macd,
                                macd_signal = results.macd_signal,
                                macd_hist   = results.macd_hist FROM results
                            WHERE daily_metrics.vendor_symbol_id = results.vendor_symbol_id
                              AND daily_metrics.date = results.date;
                            """)
        return 'success'
    except Exception as e:
        logger.error(f"update_macd: Error updating MACD for {vendor_symbol_id}: {e}")
        return 'fail'

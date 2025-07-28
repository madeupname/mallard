import duckdb
import numpy as np
import pandas as pd
from numba import njit
from mallard.tiingo.tiingo_util import logger, config

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']


def bulk_bollinger_ema_calculation(duckdb_con, start_date, period=20, multiplier=2.0):
    """
    Calculate Bollinger Bands EMA metrics incrementally for stocks that are missing them.
    Focuses on recent missing dates rather than historical gaps.
    """
    try:
        # Calculate appropriate lookback - need extra for EMA to stabilize
        lookback_days = max(period * 5, 100)  # At least 100 days, or 5x period for EMA stability

        with duckdb_con.cursor() as con:
            # Focus on recent missing dates (last 60 days) to avoid historical gaps
            recent_cutoff_date = f"CURRENT_DATE - INTERVAL '60 days'"

            # Find stocks missing Bollinger EMA in recent dates
            missing_analysis_query = f"""
            WITH recent_missing_bollinger AS (
                SELECT 
                    dm.vendor_symbol_id,
                    MIN(dm.date) as first_recent_missing_date,
                    COUNT(*) as missing_count
                FROM daily_metrics dm
                JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
                WHERE dm.date >= {recent_cutoff_date}  -- Only look at recent dates
                  AND (dm.ema_20_close IS NULL OR dm.bollinger_upper_ema_close IS NULL OR 
                       dm.bollinger_lower_ema_close IS NULL OR dm.ema_20_open IS NULL OR
                       dm.bollinger_upper_ema_open IS NULL OR dm.bollinger_lower_ema_open IS NULL)
                  AND te.adj_close IS NOT NULL  -- Should have Bollinger (has price data)
                GROUP BY dm.vendor_symbol_id
            ),
            stocks_with_sufficient_history AS (
                SELECT 
                    rmb.vendor_symbol_id,
                    rmb.first_recent_missing_date,
                    rmb.missing_count,
                    -- Count history before the recent missing date (should be plenty)
                    (SELECT COUNT(*) 
                     FROM tiingo_eod te2 
                     WHERE te2.vendor_symbol_id = rmb.vendor_symbol_id 
                       AND te2.date < rmb.first_recent_missing_date) as prior_history_days
                FROM recent_missing_bollinger rmb
                WHERE (SELECT COUNT(*) 
                       FROM tiingo_eod te2 
                       WHERE te2.vendor_symbol_id = rmb.vendor_symbol_id 
                         AND te2.date < rmb.first_recent_missing_date) >= {lookback_days - 1}
            )
            SELECT 
                COUNT(*) as stocks_needing_update,
                SUM(missing_count) as total_missing_dates,
                MIN(first_recent_missing_date) as earliest_missing_date,
                MAX(first_recent_missing_date) as latest_missing_date
            FROM stocks_with_sufficient_history
            """

            result = con.execute(missing_analysis_query).fetchone()
            stocks_needing_update, total_missing, earliest_date, latest_date = result if result else (0, 0, None, None)

            if stocks_needing_update == 0 or earliest_date is None:
                print("All stocks have up-to-date Bollinger EMA bands (recent incremental check)")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

            print(f"Found {stocks_needing_update:,} stocks with {total_missing:,} missing Bollinger EMA dates (recent)")
            print(f"Recent missing dates range: {earliest_date} to {latest_date}")

            # Load data for incremental calculation - from recent missing date back with lookback
            data_loading_query = f"""
            WITH recent_missing_bollinger AS (
                SELECT 
                    dm.vendor_symbol_id,
                    MIN(dm.date) as first_recent_missing_date
                FROM daily_metrics dm
                JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
                WHERE dm.date >= {recent_cutoff_date}
                  AND (dm.ema_20_close IS NULL OR dm.bollinger_upper_ema_close IS NULL OR 
                       dm.bollinger_lower_ema_close IS NULL OR dm.ema_20_open IS NULL OR
                       dm.bollinger_upper_ema_open IS NULL OR dm.bollinger_lower_ema_open IS NULL)
                  AND te.adj_close IS NOT NULL
                GROUP BY dm.vendor_symbol_id
                HAVING (SELECT COUNT(*) 
                        FROM tiingo_eod te2 
                        WHERE te2.vendor_symbol_id = dm.vendor_symbol_id 
                          AND te2.date < MIN(dm.date)) >= {lookback_days - 1}
            ),
            stock_date_ranges AS (
                SELECT 
                    rmb.vendor_symbol_id,
                    rmb.first_recent_missing_date,
                    rmb.first_recent_missing_date - INTERVAL '{lookback_days} days' as load_start_date,
                    (SELECT MAX(te.date) 
                     FROM tiingo_eod te 
                     WHERE te.vendor_symbol_id = rmb.vendor_symbol_id) as load_end_date
                FROM recent_missing_bollinger rmb
            )
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.adj_close,
                te.adj_open,
                sdr.first_recent_missing_date
            FROM stock_date_ranges sdr
            JOIN daily_metrics dm ON sdr.vendor_symbol_id = dm.vendor_symbol_id
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            WHERE dm.date BETWEEN sdr.load_start_date AND sdr.load_end_date
            ORDER BY dm.vendor_symbol_id, dm.date
            """

            df = con.execute(data_loading_query).df()

            if df.empty:
                print("No data loaded for recent incremental Bollinger EMA calculation")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

            unique_stocks = len(df['vsid'].unique())
            total_rows = len(df)
            print(f"Loading {total_rows:,} rows for {unique_stocks:,} stocks (recent incremental approach)")

            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            df['first_recent_missing_date'] = pd.to_datetime(df['first_recent_missing_date'])

            # Apply Bollinger EMA calculation to each symbol group
            print(f"Calculating Bollinger EMA bands incrementally for {unique_stocks:,} symbols...")

            def calculate_bollinger_ema_incremental_group(group_df, period_val, multiplier_val):
                """Apply Bollinger EMA calculation and only keep rows from first_recent_missing_date forward"""
                try:
                    # Calculate Bollinger EMA for the entire loaded range
                    result_df = compute_bollinger_ema_incremental_with_open(
                        group_df, period=period_val, multiplier=multiplier_val
                    )

                    # Only return rows from the first recent missing date forward
                    first_missing = group_df['first_recent_missing_date'].iloc[0]
                    update_rows = result_df[result_df['date'] >= first_missing].copy()

                    return update_rows
                except Exception as e:
                    print(f"Error calculating Bollinger EMA for symbol {group_df.iloc[0]['vsid']}: {e}")
                    return pd.DataFrame()

            update_dfs = []
            processed_count = 0
            successful_count = 0

            print(f"Starting to process {unique_stocks:,} symbols (DEBUG MODE - will stop after finding issue)...")

            for vsid, group in df.groupby('vsid'):
                processed_count += 1

                try:
                    result = calculate_bollinger_ema_incremental_group(group, period, multiplier)

                    if not result.empty:
                        # Check if result has valid data
                        valid_rows = result.dropna(subset=['ema_20_close'], how='all')
                        if not valid_rows.empty:
                            update_dfs.append(result)
                            successful_count += 1

                    # Progress indicator every 1000 symbols
                    if processed_count % 1000 == 0:
                        print(
                            f"Progress: {processed_count:,}/{unique_stocks:,} processed, {successful_count:,} successful")

                except Exception as e:
                    print(f"ERROR processing symbol {vsid}: {e}")
                    raise  # Continue with next symbol

            print(f"Completed: {processed_count:,} symbols processed, {successful_count:,} successful")

            if successful_count == 0:
                print("ERROR: No symbols were successfully processed. Check the EMA calculation logic.")
                return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}
            # END REPLACEMENT SECTION
            #             
            if not update_dfs:
                print("No recent Bollinger EMA bands calculated")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

            # Combine all update DataFrames
            df_to_update = pd.concat(update_dfs, ignore_index=True)

            # Update database
            columns_to_update = [
                'ema_20_close', 'bollinger_upper_ema_close', 'bollinger_lower_ema_close',
                'ema_20_open', 'bollinger_upper_ema_open', 'bollinger_lower_ema_open'
            ]
            update_cols = ['vsid', 'date'] + columns_to_update

            temp_df = df_to_update[update_cols].dropna(subset=['ema_20_close'], how='all')

            if not temp_df.empty:
                print(
                    f"Updating {len(temp_df):,} recent Bollinger EMA records for {len(temp_df['vsid'].unique()):,} symbols")

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
                print("No valid recent incremental Bollinger EMA bands calculated")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

    except Exception as e:
        print(f"Error in recent incremental bulk Bollinger EMA calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def compute_bollinger_ema_live_open(df, period=20, multiplier=2.0):
    """
    Calculate Bollinger EMA open values for the latest date per symbol.
    Uses the most recent close-based EMA and volatility with today's open price.
    
    Expects df to have columns: vsid, date, adj_open, ema_20_close, 
    bollinger_upper_ema_close, bollinger_lower_ema_close
    """
    if df.empty:
        return df

    # Create a copy to avoid modifying the original
    df = df.copy()

    # Ensure date is in datetime format
    df['date'] = pd.to_datetime(df['date'])

    # Sort by symbol and date to ensure proper ordering
    df = df.sort_values(['vsid', 'date']).reset_index(drop=True)

    # For each symbol, we need the last two rows (yesterday and today)
    # But we only calculate for the very last row (today)

    # Get the latest date for each symbol
    latest_dates = df.groupby('vsid')['date'].transform('max')

    # Get the second-to-latest date for each symbol (for previous values)
    second_latest_dates = df.groupby('vsid')['date'].transform(
        lambda x: x.sort_values().iloc[-2] if len(x) >= 2 else pd.NaT)

    # Create masks
    is_latest = (df['date'] == latest_dates)
    is_second_latest = (df['date'] == second_latest_dates)

    # Get today's rows (where we want to calculate open-based values)
    today_rows = df[is_latest].copy()

    # Get yesterday's rows (for previous close-based values)
    yesterday_rows = df[is_second_latest].copy()

    if today_rows.empty or yesterday_rows.empty:
        return df

    # Merge to get yesterday's values alongside today's
    yesterday_values = yesterday_rows[
        ['vsid', 'ema_20_close', 'bollinger_upper_ema_close', 'bollinger_lower_ema_close']].copy()
    yesterday_values.columns = ['vsid', 'prev_ema_20_close', 'prev_bollinger_upper_ema_close',
                                'prev_bollinger_lower_ema_close']

    # Merge today's data with yesterday's values
    calculation_df = today_rows.merge(yesterday_values, on='vsid', how='left')

    # Filter to only symbols that have all required data
    valid_mask = (
            calculation_df['adj_open'].notna() &
            calculation_df['prev_ema_20_close'].notna() &
            calculation_df['prev_bollinger_upper_ema_close'].notna() &
            calculation_df['prev_bollinger_lower_ema_close'].notna()
    )

    valid_df = calculation_df[valid_mask].copy()

    if valid_df.empty:
        print("No valid symbols found for Bollinger EMA open calculation")
        return df

    # Calculate EMA for open using previous day's close-based EMA
    alpha = 2 / (period + 1)
    valid_df['ema_20_open'] = alpha * valid_df['adj_open'] + (1 - alpha) * valid_df['prev_ema_20_close']

    # Extract previous volatility from yesterday's bands
    valid_df['prev_std'] = (valid_df['prev_bollinger_upper_ema_close'] - valid_df['prev_bollinger_lower_ema_close']) / (
                2 * multiplier)

    # Calculate Bollinger bands for open using yesterday's volatility
    valid_df['bollinger_upper_ema_open'] = valid_df['ema_20_open'] + multiplier * valid_df['prev_std']
    valid_df['bollinger_lower_ema_open'] = valid_df['ema_20_open'] - multiplier * valid_df['prev_std']

    # Merge results back into original dataframe
    result_cols = ['vsid', 'date', 'ema_20_open', 'bollinger_upper_ema_open', 'bollinger_lower_ema_open']
    results = valid_df[result_cols].copy()

    # Update the original dataframe
    df = df.merge(results, on=['vsid', 'date'], how='left', suffixes=('', '_new'))

    # Update columns where new values exist
    for col in ['ema_20_open', 'bollinger_upper_ema_open', 'bollinger_lower_ema_open']:
        new_col = f"{col}_new"
        if new_col in df.columns:
            df[col] = df[new_col].fillna(df[col] if col in df.columns else np.nan)
            df = df.drop(columns=[new_col])

    print(f"Calculated Bollinger EMA open values for {len(valid_df)} symbols")

    return df


def compute_bollinger_ema_live_open_bad(df, period=20, multiplier=2.0):
    """
    Optimized live Bollinger EMA calculation for trading bot. This uses previous Bollinger EMA values based on the close
    price, plus the morning's open as a partial day bar to get a sneak peek at the day's closing band values, without
    leaking the future. 
    Uses the shared compute_bollinger_ema_incremental_with_open mathematical approach but optimized for speed.
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

    # Identify the latest row for each symbol
    latest_dates = latest_two_rows.groupby('vsid')['date'].transform('max')
    latest_mask = (latest_two_rows['date'] == latest_dates)
    prev_mask = ~latest_mask

    # Extract previous values and latest open prices
    prev_values = latest_two_rows[prev_mask].set_index('vsid')
    latest_rows = latest_two_rows[latest_mask].set_index('vsid')

    # Skip symbols that don't have required data
    valid_symbols = latest_rows.index.intersection(prev_values.index)
    valid_symbols = valid_symbols[~latest_rows.loc[valid_symbols, 'adj_open'].isna()]
    valid_symbols = valid_symbols[~prev_values.loc[valid_symbols, 'ema_20_close'].isna()]

    if len(valid_symbols) == 0:
        return df

    # Calculate Bollinger EMA open values using the same logic as the core function
    results = pd.DataFrame(index=valid_symbols)

    # Get values for calculations
    latest_open = latest_rows.loc[valid_symbols, 'adj_open']
    prev_ema_20 = prev_values.loc[valid_symbols, 'ema_20_close']

    # Calculate EMA for open using previous day's close-based EMA
    alpha = 2 / (period + 1)
    ema_20_open = alpha * latest_open + (1 - alpha) * prev_ema_20

    # For the standard deviation, we need to use the incremental approach
    # This is simplified - in practice you'd want to maintain the variance state
    # For now, we'll use a simplified approach that assumes similar volatility
    prev_upper = prev_values.loc[valid_symbols, 'bollinger_upper_ema_close']
    prev_lower = prev_values.loc[valid_symbols, 'bollinger_lower_ema_close']
    prev_std = (prev_upper - prev_lower) / (2 * multiplier)

    # Calculate Bollinger bands for open
    bollinger_upper_ema_open = ema_20_open + multiplier * prev_std
    bollinger_lower_ema_open = ema_20_open - multiplier * prev_std

    # Store results
    results['ema_20_open'] = ema_20_open
    results['bollinger_upper_ema_open'] = bollinger_upper_ema_open
    results['bollinger_lower_ema_open'] = bollinger_lower_ema_open

    # Add date column for merging
    results['date'] = latest_rows.loc[valid_symbols, 'date'].values
    results = results.reset_index()

    # Use pandas merge for efficiency
    df = df.merge(
        results[['vsid', 'date', 'ema_20_open', 'bollinger_upper_ema_open', 'bollinger_lower_ema_open']],
        on=['vsid', 'date'],
        how='left',
        suffixes=('', '_new')
    )

    # Update columns where new values exist
    for col in ['ema_20_open', 'bollinger_upper_ema_open', 'bollinger_lower_ema_open']:
        new_col = f"{col}_new"
        if new_col in df.columns:
            df[col] = df[new_col].fillna(df[col])
            df = df.drop(columns=[new_col])

    return df


def compute_bollinger_ema_incremental_with_open(df, period=20, multiplier=2.0):
    """
    Compute Bollinger Bands using EMA (with numerically stable EWMSD).
    Includes close-based and open-based variants.
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])

    # --- CLOSE-based EMA version ---
    alpha = 2 / (period + 1)
    close_array = df['adj_close'].astype('float64').values

    # EMA via pandas (no precision issue)
    ema_20_close = pd.Series(close_array).ewm(span=period, adjust=False).mean()

    # DEFENSIVE: Try Numba function, fallback to pandas if it fails
    try:
        ewm_std_close = ewm_std_stable_numba(close_array, alpha)

        # Validate Numba results
        valid_std_count = np.isfinite(ewm_std_close).sum()
        if valid_std_count == 0 or valid_std_count < len(ewm_std_close) * 0.5:
            # Numba function returned garbage, use pandas fallback
            raise ValueError("Numba function returned invalid results")

    except Exception:
        # Numba failed or returned garbage - use pandas fallback
        ewm_std_close = pd.Series(close_array).ewm(span=period, adjust=False).std().values

    # DEFENSIVE: Ensure EMA assignment doesn't get corrupted
    try:
        df['ema_20_close'] = ema_20_close.values
        df['bollinger_upper_ema_close'] = ema_20_close.values + multiplier * ewm_std_close
        df['bollinger_lower_ema_close'] = ema_20_close.values - multiplier * ewm_std_close

        # Validate final results
        if df['ema_20_close'].notna().sum() == 0:
            raise ValueError("EMA assignment resulted in all NaN values")

    except Exception:
        # Assignment failed - rebuild DataFrame columns safely
        result_df = df.copy()
        result_df['ema_20_close'] = ema_20_close.values
        result_df['bollinger_upper_ema_close'] = ema_20_close.values + multiplier * ewm_std_close
        result_df['bollinger_lower_ema_close'] = ema_20_close.values - multiplier * ewm_std_close
        df = result_df

    # --- OPEN-based variant (unchanged) ---
    open_array = df['adj_open'].astype('float64').values
    ema_20_open = np.full(len(df), np.nan)
    upper_open = np.full(len(df), np.nan)
    lower_open = np.full(len(df), np.nan)

    if len(close_array) >= period:
        base_series = close_array[:period]
        base_mean = np.mean(base_series)
        base_var = np.var(base_series)
        ema_prev = base_mean
        var_prev = base_var

        for i in range(period, len(df)):
            open_today = open_array[i]
            if np.isnan(open_today):
                continue
            delta = open_today - ema_prev
            ema_today = ema_prev + alpha * delta
            delta2 = open_today - ema_today
            var_today = (1 - alpha) * (var_prev + alpha * delta * delta2)
            std_today = np.sqrt(var_today)
            ema_20_open[i] = ema_today
            upper_open[i] = ema_today + multiplier * std_today
            lower_open[i] = ema_today - multiplier * std_today
            ema_prev = ema_today
            var_prev = var_today

    df['ema_20_open'] = ema_20_open
    df['bollinger_upper_ema_open'] = upper_open
    df['bollinger_lower_ema_open'] = lower_open

    return df


def update_bollinger_ema_batch(con, vendor_symbol_id, df):
    """
    Update EMA-based Bollinger Bands values in daily_metrics table using DuckDB's direct DataFrame update.
    
    Args:
        con: Database connection
        vendor_symbol_id: Symbol identifier
        df: DataFrame with calculated EMA-based Bollinger Bands values
    """
    # Filter out rows with no Bollinger EMA values (during warmup period)
    df_valid = df.dropna(subset=['ema_20_close']).copy()

    if df_valid.empty:
        return

    # Add vendor_symbol_id column if not present
    if 'vendor_symbol_id' not in df_valid.columns:
        df_valid['vendor_symbol_id'] = vendor_symbol_id

    # Execute the update using DuckDB's direct DataFrame update
    con.execute("""
                UPDATE daily_metrics
                SET ema_20_close              = df_valid.ema_20_close,
                    bollinger_upper_ema_close = df_valid.bollinger_upper_ema_close,
                    bollinger_lower_ema_close = df_valid.bollinger_lower_ema_close,
                    ema_20_open               = df_valid.ema_20_open,
                    bollinger_upper_ema_open  = df_valid.bollinger_upper_ema_open,
                    bollinger_lower_ema_open  = df_valid.bollinger_lower_ema_open FROM df_valid
                WHERE daily_metrics.vendor_symbol_id = df_valid.vendor_symbol_id
                  AND daily_metrics.date = df_valid.date
                """)


def run_bollinger_ema_pipeline(vendor_symbol_id, duckdb_con, period=20, multiplier=2.0):
    """
    Run the complete EMA-based Bollinger Bands calculation pipeline.
    
    Args:
        vendor_symbol_id: Symbol identifier
        duckdb_con: DuckDB connection
        period: The window size, typically 20
        multiplier: Standard deviation multiplier, typically 2.0
        
    Returns:
        str: 'success', 'skip', or 'fail'
    """
    try:
        with duckdb_con.cursor() as con:
            # Begin transaction
            con.execute("BEGIN TRANSACTION")

            # Check for existing Bollinger EMA data
            last_date_query = """
                              SELECT MAX(date)
                              FROM daily_metrics
                              WHERE vendor_symbol_id = ?
                                AND ema_20_close IS NOT NULL \
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
                                                     WHERE vendor_symbol_id = ? AND date > ?) \
                                       """
                has_new_data = con.execute(new_data_check_query, [vendor_symbol_id, last_date]).fetchone()[0]

                if not has_new_data:
                    # No new data, skip calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # For Bollinger Bands, we need data for the lookback period plus the new dates
                # Get the start date for our query (period days before the first new date)
                start_date_query = """
                                   SELECT MIN(date)
                                   FROM tiingo_eod
                                   WHERE vendor_symbol_id = ? AND date > ? \
                                   """
                first_new_date = con.execute(start_date_query, [vendor_symbol_id, last_date]).fetchone()[0]

                if first_new_date is None:
                    # No new data, skip calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert to pandas datetime
                first_new_date_pd = pd.to_datetime(first_new_date)

                # Calculate start date for lookback period
                lookback_start_date = first_new_date_pd - pd.Timedelta(
                    days=period * 2)  # Extra buffer for weekends/holidays

                # Query for price data including lookback period
                query = """
                        SELECT date, adj_close, adj_open
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ? AND date >= ?
                        ORDER BY date \
                        """
                df = con.execute(query, [vendor_symbol_id, lookback_start_date]).df()

                # If somehow we got no rows, skip
                if len(df) == 0:
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])

                # Calculate Bollinger Bands with EMA
                df_bollinger = compute_bollinger_ema_incremental_with_open(df, period, multiplier)

                # Filter to only include new dates (after last_date)
                df_bollinger = df_bollinger[df_bollinger['date'] > last_date_pd].reset_index(drop=True)

            else:
                # No previous calculations, load all data
                query = """
                        SELECT date, adj_close, adj_open
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ?
                        ORDER BY date \
                        """
                df = con.execute(query, [vendor_symbol_id]).df()

                # If somehow we got no rows, skip
                if len(df) == 0:
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])

                # Check if we have enough data for initial calculation
                if len(df) < period:  # Need at least period rows for initial calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # Calculate Bollinger Bands with EMA
                df_bollinger = compute_bollinger_ema_incremental_with_open(df, period, multiplier)

            # If we have no rows to update (all filtered out), skip
            if df_bollinger.empty or df_bollinger.dropna(subset=['ema_20_close']).empty:
                con.execute("ROLLBACK")
                return 'skip'

            # Update database
            update_bollinger_ema_batch(con, vendor_symbol_id, df_bollinger)

            # Commit transaction
            con.execute("COMMIT")
            return 'success'

    except Exception as e:
        # Rollback on error
        try:
            con.execute("ROLLBACK")
        except:
            pass

        logger.error(f"run_bollinger_ema_pipeline failed for {vendor_symbol_id}: {e}")
        return 'fail'


def compute_bollinger_incremental_with_open(df, period=20, stddev_mult=2.0):
    """
    Compute Bollinger Bands using adjusted close and open prices.
    
    - `bollinger_upper_close`, `sma_20_close`, `bollinger_lower_close` use adjusted closes.
    - `bollinger_upper_open`, `sma_20_open`, `bollinger_lower_open` use previous 19 closes + today's open.
    
    Args:
        df (pd.DataFrame): Must include 'date', 'adj_close', and 'adj_open'.
        period (int): The window size, typically 20.
        stddev_mult (float): Standard deviation multiplier, typically 2.
        
    Returns:
        pd.DataFrame with the new columns added.
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])  # ensure clean dates

    # ---- CLOSE-BASED ----
    sma_20_close = df['adj_close'].rolling(window=period, min_periods=period).mean()
    std_close = df['adj_close'].rolling(window=period, min_periods=period).std(ddof=0)

    df['sma_20_close'] = sma_20_close
    df['bollinger_upper_close'] = sma_20_close + stddev_mult * std_close
    df['bollinger_lower_close'] = sma_20_close - stddev_mult * std_close

    # ---- OPEN-BASED ----
    # Initialize lists to store results
    sma_20_open_list = []  # Renamed to be explicit about type
    upper_open_list = []  # Renamed to be explicit about type
    lower_open_list = []  # Renamed to be explicit about type

    closes = df['adj_close'].values
    opens = df['adj_open'].values

    # Use np.errstate to suppress specific warnings
    with np.errstate(invalid='ignore', divide='ignore'):
        for i in range(len(df)):
            if i < period - 1:
                sma_20_open_list.append(np.nan)
                upper_open_list.append(np.nan)
                lower_open_list.append(np.nan)
                continue

            # Use previous 19 closes + today's open
            window = np.append(closes[i - period + 1:i], opens[i])

            # Skip if any NaN in window or if today's open is NaN
            if np.isnan(window).any() or np.isnan(opens[i]):
                sma_20_open_list.append(np.nan)
                upper_open_list.append(np.nan)
                lower_open_list.append(np.nan)
                continue

            # Calculate mean and standard deviation
            mean = np.mean(window)
            std = np.std(window, ddof=0)

            # Store values
            sma_20_open_list.append(mean)
            upper_open_list.append(mean + stddev_mult * std)
            lower_open_list.append(mean - stddev_mult * std)

    # Assign lists to DataFrame columns
    df['sma_20_open'] = sma_20_open_list
    df['bollinger_upper_open'] = upper_open_list
    df['bollinger_lower_open'] = lower_open_list

    return df


def update_bollinger_batch(con, vendor_symbol_id, df):
    """
    Update Bollinger Bands values in daily_metrics table using DuckDB's direct DataFrame update.
    
    Args:
        con: Database connection
        vendor_symbol_id: Symbol identifier
        df: DataFrame with calculated Bollinger Bands values
    """
    # Filter out rows with no Bollinger values (during warmup period)
    df_valid = df.dropna(subset=['sma_20_close']).copy()

    if df_valid.empty:
        return

    # Add vendor_symbol_id column if not present
    if 'vendor_symbol_id' not in df_valid.columns:
        df_valid['vendor_symbol_id'] = vendor_symbol_id

    # Execute the update using DuckDB's direct DataFrame update
    con.execute("""
                UPDATE daily_metrics
                SET sma_20_close          = df_valid.sma_20_close,
                    bollinger_upper_close = df_valid.bollinger_upper_close,
                    bollinger_lower_close = df_valid.bollinger_lower_close,
                    sma_20_open           = df_valid.sma_20_open,
                    bollinger_upper_open  = df_valid.bollinger_upper_open,
                    bollinger_lower_open  = df_valid.bollinger_lower_open FROM df_valid
                WHERE daily_metrics.vendor_symbol_id = df_valid.vendor_symbol_id
                  AND daily_metrics.date = df_valid.date
                """)


from numba import njit


@njit
def ewm_std_stable_numba(series, alpha):
    N = len(series)
    ewma_mean = np.zeros(N)
    ewma_var = np.zeros(N)
    ewma_mean[0] = series[0]
    for t in range(1, N):
        x = series[t]
        prev_mean = ewma_mean[t - 1]
        delta = x - prev_mean
        ewma_mean[t] = prev_mean + alpha * delta
        delta2 = x - ewma_mean[t]
        ewma_var[t] = (1 - alpha) * (ewma_var[t - 1] + alpha * delta * delta2)
    ewma_std = np.sqrt(ewma_var)
    return ewma_std


def run_bollinger_pipeline(vendor_symbol_id, duckdb_con, period=20, stddev_mult=2.0):
    """
    Run the complete Bollinger Bands calculation pipeline.
    
    Args:
        vendor_symbol_id: Symbol identifier
        duckdb_con: DuckDB connection
        period: The window size, typically 20
        stddev_mult: Standard deviation multiplier, typically 2.0
        
    Returns:
        str: 'success', 'skip', or 'fail'
    """
    try:
        with duckdb_con.cursor() as con:
            # Begin transaction
            con.execute("BEGIN TRANSACTION")

            # Check for existing Bollinger data
            last_date_query = """
                              SELECT MAX(date)
                              FROM daily_metrics
                              WHERE vendor_symbol_id = ?
                                AND sma_20_close IS NOT NULL \
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
                                                     WHERE vendor_symbol_id = ? AND date > ?) \
                                       """
                has_new_data = con.execute(new_data_check_query, [vendor_symbol_id, last_date]).fetchone()[0]

                if not has_new_data:
                    # No new data, skip calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # For Bollinger Bands, we need data for the lookback period plus the new dates
                # Get the start date for our query (period days before the first new date)
                start_date_query = """
                                   SELECT MIN(date)
                                   FROM tiingo_eod
                                   WHERE vendor_symbol_id = ? AND date > ? \
                                   """
                first_new_date = con.execute(start_date_query, [vendor_symbol_id, last_date]).fetchone()[0]

                if first_new_date is None:
                    # No new data, skip calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert to pandas datetime
                first_new_date_pd = pd.to_datetime(first_new_date)

                # Calculate start date for lookback period
                lookback_start_date = first_new_date_pd - pd.Timedelta(
                    days=period * 2)  # Extra buffer for weekends/holidays

                # Query for price data including lookback period
                query = """
                        SELECT date, adj_close, adj_open
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ? AND date >= ?
                        ORDER BY date \
                        """
                df = con.execute(query, [vendor_symbol_id, lookback_start_date]).df()

                # If somehow we got no rows, skip
                if len(df) == 0:
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])

                # Calculate Bollinger Bands
                df_bollinger = compute_bollinger_incremental_with_open(df, period, stddev_mult)

                # Filter to only include new dates (after last_date)
                df_bollinger = df_bollinger[df_bollinger['date'] > last_date_pd].reset_index(drop=True)

            else:
                # No previous calculations, load all data
                query = """
                        SELECT date, adj_close, adj_open
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ?
                        ORDER BY date \
                        """
                df = con.execute(query, [vendor_symbol_id]).df()

                # If somehow we got no rows, skip
                if len(df) == 0:
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])

                # Check if we have enough data for initial calculation
                if len(df) < period:  # Need at least period rows for initial calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # Calculate Bollinger Bands
                df_bollinger = compute_bollinger_incremental_with_open(df, period, stddev_mult)

            # If we have no rows to update (all filtered out), skip
            if df_bollinger.empty or df_bollinger.dropna(subset=['sma_20_close']).empty:
                con.execute("ROLLBACK")
                return 'skip'

            # Update database
            update_bollinger_batch(con, vendor_symbol_id, df_bollinger)

            # Commit transaction
            con.execute("COMMIT")
            return 'success'

    except Exception as e:
        # Rollback on error
        try:
            con.execute("ROLLBACK")
        except:
            pass

        logger.error(f"run_bollinger_pipeline failed for {vendor_symbol_id}: {e}")
        return 'fail'


def bollinger_sma(price_data: pd.DataFrame) -> pd.DataFrame:
    """Computes 20-period Bollinger Bands using SMA.
    price_data is a dataframe with a date column and an adj_close column."""
    # TAlib can't be used because adjusted close data with large variances cause a precision error.
    # If we don't use apply (which is 45x slower), Pandas will miscalculate for the same reason.
    sma_20 = price_data['adj_close'].rolling(window=20).mean()
    rolling_std = price_data['adj_close'].rolling(window=20).apply(lambda x: np.std(x, dtype='float64'), raw=True)
    bollinger_upper = sma_20 + (2 * rolling_std)
    bollinger_lower = sma_20 - (2 * rolling_std)

    # Prepare DataFrame to insert results
    results = pd.DataFrame({
        'date': price_data['date'],
        'bollinger_upper': bollinger_upper,
        'sma_20': sma_20,
        'bollinger_lower': bollinger_lower
    })
    # Clean data (remove NaNs due to SMA calculation starting period)
    results = results.dropna(subset=['bollinger_upper', 'sma_20', 'bollinger_lower'])
    return results


def bollinger_ema(price_data) -> pd.DataFrame:
    # Calculate Bollinger bands with EMA. This must be done with Pandas as TA lib can't do BB with EMA, but it's
    # also susceptible to the same precision error. The solution requires Weflord's algorithm for numeric stability.
    @njit
    def ewm_std_stable_numba(series, alpha):
        N = len(series)
        ewma_mean = np.zeros(N)
        ewma_var = np.zeros(N)
        ewma_mean[0] = series[0]
        for t in range(1, N):
            x = series[t]
            prev_mean = ewma_mean[t - 1]
            delta = x - prev_mean
            ewma_mean[t] = prev_mean + alpha * delta
            delta2 = x - ewma_mean[t]
            ewma_var[t] = (1 - alpha) * (ewma_var[t - 1] + alpha * delta * delta2)
        ewma_std = np.sqrt(ewma_var)
        return ewma_std

    # Calculate alpha
    period = 20
    alpha = 2 / (period + 1)

    # Ensure data is numpy array of float64
    adj_close_array = price_data['adj_close'].astype('float64').values

    # Calculate numerically stable EWMSD using Numba
    ewm_std = ewm_std_stable_numba(adj_close_array, alpha)

    # Calculate EMA
    ema_20 = price_data['adj_close'].ewm(span=period, adjust=False).mean()

    # Calculate upper and lower Bollinger Bands
    multiplier = 2.0
    bollinger_upper = ema_20 + (ewm_std * multiplier)
    bollinger_lower = ema_20 - (ewm_std * multiplier)

    # Prepare DataFrame to insert results
    results = pd.DataFrame({
        'date': price_data['date'],
        'bollinger_upper': bollinger_upper,
        'ema_20': ema_20,
        'bollinger_lower': bollinger_lower
    })

    # Clean data (remove NaNs due to EMA calculation starting period)
    results = results.dropna(subset=['bollinger_upper', 'ema_20', 'bollinger_lower'])
    return results


@DeprecationWarning
def update_bollinger(vendor_symbol_id: str, duckdb_con: duckdb.DuckDBPyConnection, start_date='1990-06-01',
                     end_date=None):
    with duckdb_con.cursor() as con:
        # Fetch the price data
        sql_query = f"""
        SELECT "date", adj_close
        FROM {eod_table}
        WHERE vendor_symbol_id = '{vendor_symbol_id}' 
            AND "date" >= '{start_date}' 
            {f"AND date <= '{end_date}'" if end_date else ""}
        ORDER BY "date" ASC;
        """
        price_data = con.execute(sql_query).df()
        # Check if data is sufficient
        if price_data.shape[0] == 0:
            logger.warn(
                f"update_bollinger: No price data for {vendor_symbol_id} from {start_date} to {end_date}")
            return 'skip'

        # Update the SMA Bollinger Bands
        sma_results = bollinger_sma(price_data)

        query = f"""
            UPDATE daily_metrics
            SET bollinger_upper = sma_results.bollinger_upper,
                sma_20 = sma_results.sma_20,
                bollinger_lower = sma_results.bollinger_lower
            FROM sma_results
            WHERE daily_metrics.vendor_symbol_id = '{vendor_symbol_id}'
                  AND daily_metrics.date = sma_results.date;"""
        con.execute(query)

        # Update the EMA Bollinger Bands
        ema_results = bollinger_ema(price_data)

        query = f"""
            UPDATE daily_metrics
            SET bollinger_upper_ema = ema_results.bollinger_upper,
                bollinger_lower_ema = ema_results.bollinger_lower,
                ema_20 = ema_results.ema_20            
            FROM ema_results
            WHERE daily_metrics.vendor_symbol_id = '{vendor_symbol_id}'
                  AND daily_metrics.date = ema_results.date;"""
        con.execute(query)

    return 'success'

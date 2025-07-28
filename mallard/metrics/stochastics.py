import duckdb
import numpy as np
import pandas as pd
import talib
from legitindicators import roofing_filter

from mallard.tiingo.tiingo_util import logger, config

db_file = config['DEFAULT']['db_file']
eod_table = config['tiingo']['eod_table']


def bulk_stochastics_calculation(duckdb_con, start_date, k_period=14, d_period=3):
    """
    Calculate Stochastic Oscillator metrics incrementally for stocks that are missing them.
    Uses only closing prices for stochastics calculation.
    """
    try:
        with duckdb_con.cursor() as con:
            min_history_needed = k_period + d_period  # 17 days

            # Focus on recent missing dates (last 60 days) to avoid historical gaps
            recent_cutoff_date = f"CURRENT_DATE - INTERVAL '60 days'"

            # Find stocks missing stochastics in recent dates
            missing_analysis_query = f"""
            WITH recent_missing_stochastics AS (
                SELECT 
                    dm.vendor_symbol_id,
                    MIN(dm.date) as first_recent_missing_date,
                    COUNT(*) as missing_count
                FROM daily_metrics dm
                JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
                WHERE dm.date >= {recent_cutoff_date}  -- Only look at recent dates
                  AND dm.slowk IS NULL
                  AND te.adj_high != te.adj_low  -- Should have stochastics (has price movement)
                GROUP BY dm.vendor_symbol_id
            ),
            stocks_with_sufficient_history AS (
                SELECT 
                    rms.vendor_symbol_id,
                    rms.first_recent_missing_date,
                    rms.missing_count,
                    -- Count history before the recent missing date (should be plenty)
                    (SELECT COUNT(*) 
                     FROM tiingo_eod te2 
                     WHERE te2.vendor_symbol_id = rms.vendor_symbol_id 
                       AND te2.date < rms.first_recent_missing_date) as prior_history_days
                FROM recent_missing_stochastics rms
                WHERE (SELECT COUNT(*) 
                       FROM tiingo_eod te2 
                       WHERE te2.vendor_symbol_id = rms.vendor_symbol_id 
                         AND te2.date < rms.first_recent_missing_date) >= {min_history_needed - 1}
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
                print("All stocks have up-to-date stochastics (recent incremental check)")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

            print(f"Found {stocks_needing_update:,} stocks with {total_missing:,} missing stochastic dates (recent)")
            print(f"Recent missing dates range: {earliest_date} to {latest_date}")

            # Load data for incremental calculation - from recent missing date back with lookback
            lookback_days = min_history_needed + 5  # Small buffer

            data_loading_query = f"""
            WITH recent_missing_stochastics AS (
                SELECT 
                    dm.vendor_symbol_id,
                    MIN(dm.date) as first_recent_missing_date
                FROM daily_metrics dm
                JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
                WHERE dm.date >= {recent_cutoff_date}
                  AND dm.slowk IS NULL
                  AND te.adj_high != te.adj_low
                GROUP BY dm.vendor_symbol_id
                HAVING (SELECT COUNT(*) 
                        FROM tiingo_eod te2 
                        WHERE te2.vendor_symbol_id = dm.vendor_symbol_id 
                          AND te2.date < MIN(dm.date)) >= {min_history_needed - 1}
            ),
            stock_date_ranges AS (
                SELECT 
                    rms.vendor_symbol_id,
                    rms.first_recent_missing_date,
                    -- Get the date of the last valid slowk before the missing range
                    (SELECT MAX(dm2.date) 
                     FROM daily_metrics dm2 
                     WHERE dm2.vendor_symbol_id = rms.vendor_symbol_id 
                       AND dm2.date < rms.first_recent_missing_date 
                       AND dm2.slowk IS NOT NULL) as last_valid_date,
                    (SELECT MAX(te.date) 
                     FROM tiingo_eod te 
                     WHERE te.vendor_symbol_id = rms.vendor_symbol_id) as load_end_date
                FROM recent_missing_stochastics rms
            )
            SELECT 
                dm.vendor_symbol_id AS vsid,
                dm.symbol,
                dm.date,
                te.adj_high,
                te.adj_low,
                te.adj_close,
                sdr.first_recent_missing_date
            FROM stock_date_ranges sdr
            JOIN daily_metrics dm ON sdr.vendor_symbol_id = dm.vendor_symbol_id
            JOIN tiingo_eod te ON dm.vendor_symbol_id = te.vendor_symbol_id AND dm.date = te.date
            -- Load from well before the last valid date to ensure sufficient history
            WHERE dm.date BETWEEN sdr.last_valid_date - INTERVAL '{k_period + d_period + 10} days' 
                              AND sdr.load_end_date
            ORDER BY dm.vendor_symbol_id, dm.date
            """

            df = con.execute(data_loading_query).df()

            if df.empty:
                print("No data loaded for recent incremental stochastics calculation")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

            unique_stocks = len(df['vsid'].unique())
            total_rows = len(df)
            print(f"Loading {total_rows:,} rows for {unique_stocks:,} stocks (recent incremental approach)")

            # Convert dates to datetime
            df['date'] = pd.to_datetime(df['date'])
            df['first_recent_missing_date'] = pd.to_datetime(df['first_recent_missing_date'])

            # Apply stochastics calculation to each symbol group
            print(f"Calculating Stochastics incrementally for {unique_stocks:,} symbols...")

            def calculate_stochastics_incremental_group(group_df):
                """Apply stochastics calculation and only keep rows from first_recent_missing_date forward"""
                try:
                    # Calculate stochastics for the entire loaded range
                    result_df = compute_stochastics_incremental(group_df, k_period, d_period)
                    
                    # Only return rows from the first recent missing date forward
                    first_missing = group_df['first_recent_missing_date'].iloc[0]
                    update_rows = result_df[result_df['date'] >= first_missing].copy()
                    
                    return update_rows
                except Exception as e:
                    print(f"Error calculating stochastics for symbol {group_df.iloc[0]['vsid']}: {e}")
                    return pd.DataFrame()

            # Process each stock and collect only the rows that need updating
            update_dfs = []
            for vsid, group in df.groupby('vsid'):
                result = calculate_stochastics_incremental_group(group)
                if not result.empty:
                    update_dfs.append(result)

            if not update_dfs:
                print("No recent stochastics calculated")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

            # Combine all update DataFrames
            df_to_update = pd.concat(update_dfs, ignore_index=True)

            # Update database
            columns_to_update = ['slowk', 'slowd']
            update_cols = ['vsid', 'date'] + columns_to_update

            temp_df = df_to_update[update_cols].dropna(subset=['slowk'], how='all')

            if not temp_df.empty:
                print(f"Updating {len(temp_df):,} Stochastic records for {len(temp_df['vsid'].unique()):,} symbols")

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
                print("No valid recent incremental stochastics calculated")
                return {'count_success': 0, 'count_skip': 1, 'count_fail': 0}

    except Exception as e:
        print(f"Error in recent incremental bulk Stochastic calculation: {e}")
        import traceback
        traceback.print_exc()
        return {'count_success': 0, 'count_skip': 0, 'count_fail': 1}


def compute_stochastics_incremental(df, k_period=14, d_period=3):
    """
    Compute Stochastic Oscillator (%K and %D) using closing prices only.

    Parameters:
        df (pd.DataFrame): Must contain 'date', 'adj_high', 'adj_low', 'adj_close'
        k_period (int): Lookback period for %K
        d_period (int): Smoothing period for %D

    Returns:
        pd.DataFrame: With added columns: slowk, slowd
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])  # ensure consistency

    highs = df['adj_high'].to_numpy()
    lows = df['adj_low'].to_numpy()
    closes = df['adj_close'].to_numpy()

    n = len(df)
    slowk = np.full(n, np.nan)

    # Use np.errstate to suppress specific warnings
    with np.errstate(invalid='ignore', divide='ignore'):
        for i in range(k_period - 1, n):
            # Get the lookback window
            window_highs = highs[i - k_period + 1: i + 1]
            window_lows = lows[i - k_period + 1: i + 1]

            # Check if window contains all NaN values
            if not np.isnan(window_highs).all() and not np.isnan(window_lows).all():
                hh = np.nanmax(window_highs)
                ll = np.nanmin(window_lows)

                # More robust check for valid calculation
                if (not np.isnan(closes[i]) and
                        not np.isnan(hh) and
                        not np.isnan(ll) and
                        hh != ll and
                        abs(hh - ll) > 1e-10):  # Avoid very small denominators

                    slowk[i] = 100 * (closes[i] - ll) / (hh - ll)
                    # Ensure value is in valid range
                    slowk[i] = np.clip(slowk[i], 0, 100)

    # Smooth with SMA for %D
    slowd = pd.Series(slowk).rolling(window=d_period, min_periods=d_period).mean().to_numpy()

    df['slowk'] = slowk
    df['slowd'] = slowd

    return df


def update_stochastics_batch(con, vendor_symbol_id, df):
    """
    Update Stochastic Oscillator values in daily_metrics table using DuckDB's direct DataFrame update.
    
    Args:
        con: Database connection
        vendor_symbol_id: Symbol identifier
        df: DataFrame with calculated Stochastic values
    """
    # Filter out rows with no Stochastic values (during warmup period)
    df_valid = df.dropna(subset=['slowk']).copy()

    if df_valid.empty:
        return

    # Add vendor_symbol_id column if not present
    if 'vendor_symbol_id' not in df_valid.columns:
        df_valid['vendor_symbol_id'] = vendor_symbol_id

    # Execute the update using DuckDB's direct DataFrame update
    con.execute("""
                UPDATE daily_metrics
                SET slowk = df_valid.slowk,
                    slowd = df_valid.slowd
                FROM df_valid
                WHERE daily_metrics.vendor_symbol_id = df_valid.vendor_symbol_id
                  AND daily_metrics.date = df_valid.date
                """)


def run_stochastics_pipeline(vendor_symbol_id, duckdb_con, k_period=14, d_period=3):
    """
    Run the complete Stochastic Oscillator calculation pipeline.
    
    Args:
        vendor_symbol_id: Symbol identifier
        duckdb_con: DuckDB connection
        k_period: Lookback period for %K (default 14)
        d_period: Smoothing period for %D (default 3)
        
    Returns:
        str: 'success', 'skip', or 'fail'
    """
    try:
        with duckdb_con.cursor() as con:
            # Begin transaction
            con.execute("BEGIN TRANSACTION")

            # Check for existing Stochastic data
            last_date_query = """
                              SELECT MAX(date)
                              FROM daily_metrics
                              WHERE vendor_symbol_id = ? AND slowk_close IS NOT NULL
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

                # For Stochastics, we need data for the lookback period plus the new dates
                # Get the start date for our query (k_period days before the first new date)
                start_date_query = """
                                   SELECT MIN(date)
                                   FROM tiingo_eod
                                   WHERE vendor_symbol_id = ? AND date > ? 
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
                    days=k_period * 2)  # Extra buffer for weekends/holidays

                # Query for price data including lookback period
                query = """
                        SELECT date, adj_high, adj_low, adj_close, adj_open
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ? AND date >= ?
                        ORDER BY date 
                        """
                df = con.execute(query, [vendor_symbol_id, lookback_start_date]).df()

                # If somehow we got no rows, skip
                if len(df) == 0:
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])

                # Calculate Stochastics
                df_stoch = compute_stochastics_incremental(df, k_period, d_period)

                # Filter to only include new dates (after last_date)
                df_stoch = df_stoch[df_stoch['date'] > last_date_pd].reset_index(drop=True)

            else:
                # No previous calculations, load all data
                query = """
                        SELECT date, adj_high, adj_low, adj_close, adj_open
                        FROM tiingo_eod
                        WHERE vendor_symbol_id = ?
                        ORDER BY date 
                        """
                df = con.execute(query, [vendor_symbol_id]).df()

                # If somehow we got no rows, skip
                if len(df) == 0:
                    con.execute("ROLLBACK")
                    return 'skip'

                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])

                # Check if we have enough data for initial calculation
                if len(df) < k_period + d_period:  # Need at least k_period+d_period rows for initial calculation
                    con.execute("ROLLBACK")
                    return 'skip'

                # Calculate Stochastics
                df_stoch = compute_stochastics_incremental(df, k_period, d_period)

            # If we have no rows to update (all filtered out), skip
            if df_stoch.empty or df_stoch.dropna(subset=['slowk_close']).empty:
                con.execute("ROLLBACK")
                return 'skip'

            # Update database
            update_stochastics_batch(con, vendor_symbol_id, df_stoch)

            # Commit transaction
            con.execute("COMMIT")
            return 'success'

    except Exception as e:
        # Rollback on error
        try:
            con.execute("ROLLBACK")
        except:
            pass

        logger.error(f"run_stochastics_pipeline failed for {vendor_symbol_id}: {e}")
        return 'fail'


@DeprecationWarning
def update_stochastics(vendor_symbol_id: str, duckdb_con: duckdb.DuckDBPyConnection, start_date='1990-06-01',
                       end_date=None):
    with duckdb_con.cursor() as con:
        # Fetch the price data
        sql_query = f"""        
        SELECT date, adj_high, adj_low, adj_close
        FROM {eod_table}
        WHERE vendor_symbol_id = '{vendor_symbol_id}' 
            AND date >= '{start_date}' 
            {f"AND date <= '{end_date}'" if end_date else ""}
        ORDER BY date ASC;
        """
        price_data = con.execute(sql_query).df()

        # Check if data is sufficient. Must have at least 14 rows for the Stochastics calculation.
        if price_data.shape[0] < 14:
            print(f"update_stochastics: No data available for params: {vendor_symbol_id}, {start_date}, {end_date}.")
            return 'skip'

        # Calculate stochastics using TA lib
        slowk, slowd = talib.STOCH(price_data['adj_high'].to_numpy(), price_data['adj_low'].to_numpy(),
                                   price_data['adj_close'].to_numpy(), fastk_period=14, slowk_period=3, slowk_matype=0,
                                   slowd_period=3, slowd_matype=0)

        # Apply the roofing filter to high, low, and close prices
        # Before applying roofing filter, clean the input data
        filtered_high = roofing_filter(
            price_data['adj_high'].replace([np.inf, -np.inf], np.nan).dropna().to_numpy(),
            hp_length=10,
            ss_length=30
        )
        filtered_low = roofing_filter(
            price_data['adj_low'].replace([np.inf, -np.inf], np.nan).dropna().to_numpy(),
            hp_length=10,
            ss_length=30
        )
        filtered_close = roofing_filter(
            price_data['adj_close'].replace([np.inf, -np.inf], np.nan).dropna().to_numpy(),
            hp_length=10,
            ss_length=30
        )

        price_data['filtered_high'] = filtered_high
        price_data['filtered_low'] = filtered_low
        price_data['filtered_close'] = filtered_close

        # Use filtered data to calculate the Stochastic Oscillator
        filtered_slowk, filtered_slowd = talib.STOCH(price_data['filtered_high'].to_numpy(),
                                                     price_data['filtered_low'].to_numpy(),
                                                     price_data['filtered_close'].to_numpy(), fastk_period=14,
                                                     slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)

        # Prepare Pandas DataFrame to insert results
        results = pd.DataFrame({
            'vendor_symbol_id': vendor_symbol_id,
            'date': price_data['date'],
            'slowk': slowk,
            'slowd': slowd,
            'filtered_slowk': filtered_slowk,
            'filtered_slowd': filtered_slowd
        })

        # Clean data (remove NaNs due to SMA calculation starting period)
        results = results.dropna(subset=['slowk', 'slowd'])
        results.reset_index(drop=True, inplace=True)

        # Insert/update the daily_metrics table via UPDATE statement
        query = """
                UPDATE daily_metrics
                SET slowk          = results.slowk,
                    slowd          = results.slowd,
                    filtered_slowk = results.filtered_slowk,
                    filtered_slowd = results.filtered_slowd FROM results
                WHERE daily_metrics.vendor_symbol_id = results.vendor_symbol_id
                  AND daily_metrics.date = results.date;"""
        con.execute(query)
    return 'success'

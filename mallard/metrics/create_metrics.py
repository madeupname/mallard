# Creates/updates daily metrics
import configparser
import os
import signal
import subprocess
import sys
from datetime import datetime

import duckdb

from mallard.metrics.bollinger import bulk_bollinger_ema_calculation
from mallard.metrics.daily_metrics import bulk_ema_calculation, bulk_past_returns_calculation_extended, bulk_regime_calculation, initialize_daily_metrics, \
    avg_daily_trading_value, rank, create_relevant_metrics_fm, create_relevant_metrics_fad, rank_metric, \
    create_consolidated_metrics, \
    create_max_returns, update_ema, bulk_streak_calculation, \
    bulk_volatility_calculation, bulk_trading_val_calculation, bulk_beta_calculation, bulk_daily_return_calculation, \
    bulk_vxx_calculation, bulk_atr_calculation, bulk_skew_calculation, bulk_distance_calculation

from mallard.metrics.fundamental_metrics import calculate_ttm, margins, initialize_fundamental_metrics
from mallard.metrics.macd import bulk_macd_calculation
from mallard.metrics.parallelize import parallelize
from mallard.metrics.rsi import bulk_rsi_calculation, run_rsi_pipeline
from mallard.metrics.stochastics import bulk_stochastics_calculation, run_stochastics_pipeline
from mallard.tiingo.tiingo_util import logger

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']
daily_metrics = set(config['DEFAULT']['metrics'].split(","))
eod_table = config['tiingo']['eod_table']
fundamentals_reported_table = config['tiingo']['fundamentals_reported_table']

# Run script db.py
# Determine the path to db.py
db_script_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'db.py')

# Run the db.py script
subprocess.check_call([sys.executable, db_script_path])

# Get argument --start
start = None
for i in range(len(sys.argv)):
    if sys.argv[i] == '--start':
        # Make sure there is a value after --start matching the format 'YYYY-MM-DD'
        if i + 1 >= len(sys.argv):
            raise Exception("Missing value after --start")
        # Check if the value after --start is a valid date
        try:
            datetime.strptime(sys.argv[i + 1], '%Y-%m-%d')
        except ValueError:
            raise Exception("Invalid date format after --start, use 'YYYY-MM-DD' format")
        start = sys.argv[i + 1]
        break

if start is None:
    start = config['DEFAULT']['metrics_start'] if config.has_option('DEFAULT', 'metrics_start') else '2000-01-01'

# Global flag to indicate shutdown
shutdown_flag = False


def signal_handler(signal, frame):
    global shutdown_flag
    print('Signal received, shutting down.')
    exit(0)


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
if hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, signal_handler)


def update_metrics():
    """Updates all metrics specified in the config file.
    Uses a ThreadPoolExecutor to run them in parallel if more than 1 worker specified."""
    workers = int(config['DEFAULT']['threads'])
    os.environ['NUMEXPR_MAX_THREADS'] = str(workers)
    msg = f"Updating metrics: {daily_metrics}"
    print(msg)
    logger.info(msg)
    # Get symbols to compile metrics on using EOD table.
    with duckdb.connect(db_file) as con:
        symbols_query = f"""
        SELECT DISTINCT vendor_symbol_id, symbol
        FROM tiingo_eod"""
        symbol_ids = con.sql(symbols_query)
        eod_symbols = symbol_ids.fetchall()

    vendor_symbol_ids = [row[0] for row in eod_symbols]
    initialize_fundamental_metrics()
    initialize_daily_metrics()
    if 'adtval' in daily_metrics:
        msg = "Calculating avg_daily_trading_value..."
        print(msg)
        logger.info(msg)
        avg_daily_trading_value()
        msg = "Done calculating avg_daily_trading_value"
        print(msg)
        logger.info(msg)

    if 'macd' in daily_metrics:
        print("Calculating MACD using bulk method...")
        msg = "Calculating MACD..."
        with duckdb.connect(db_file) as con:
            # Add columns if they don't exist
            sql_query = """
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ema_12 DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ema_26 DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_signal DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_hist DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ema_12_open DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ema_26_open DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_open DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_signal_open DOUBLE;
            ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_hist_open DOUBLE;
            """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            macd_results = bulk_macd_calculation(con, start)
        msg = f"MACD: Skipped {macd_results['count_skip']}  Updated {macd_results['count_success']}  Failed {macd_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'bollinger' in daily_metrics:
        msg = "Calculating Bollinger Bands with EMA..."
        with duckdb.connect(db_file) as con:
            # Fetch the price data
            sql_query = """
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS sma_20_close DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_upper_close DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_lower_close DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS sma_20_open DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_upper_open DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_lower_open DOUBLE;

                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS ema_20_close DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_upper_ema_close DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_lower_ema_close DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS ema_20_open DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_upper_ema_open DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS bollinger_lower_ema_open DOUBLE; \
                        """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            bollinger_results = bulk_bollinger_ema_calculation(con, start)
        msg = f"Bollinger Bands: Skipped {bollinger_results['count_skip']}  Updated {bollinger_results['count_success']}  Failed {bollinger_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'rsi' in daily_metrics:
        msg = "Calculating RSI..."
        with duckdb.connect(db_file) as con:
            # Fetch the price data
            sql_query = """
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS avg_gain DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS avg_loss DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS rsi DOUBLE;
                        """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            rsi_results = bulk_rsi_calculation(con, start)
        msg = f"RSI: Skipped {rsi_results['count_skip']}  Updated {rsi_results['count_success']}  Failed {rsi_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'daily_return' in daily_metrics:  
        print("Calculating daily returns using bulk method...")
        msg = "Calculating daily returns..."
        with duckdb.connect(db_file) as con:
            # Add columns if they don't exist
            sql_query = """
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS return_0 DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS return_0_zscore_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS return_close_close DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spy_return_0 DOUBLE;                            
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spy_return_0_pctile_1y DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS log_return_tlt_hyg DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS tlt_hyg_ratio DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS tlt_hyg_ratio_zscore_3m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS vol_tlt_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS vol_hyg_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS zscore_return_tlt_3m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS zscore_return_hyg_3m DOUBLE;
            """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            
            # Run individual stock daily returns (simplified version)
            daily_return_results = bulk_daily_return_calculation(con, start)
            
            # Run regime calculations separately
            regime_results = bulk_regime_calculation(con, start)
            
        # Combine results for reporting
        total_success = daily_return_results['count_success'] + regime_results['count_success']
        total_skip = daily_return_results['count_skip'] + regime_results['count_skip'] 
        total_fail = daily_return_results['count_fail'] + regime_results['count_fail']
        
        msg = f"Daily Returns: Skipped {total_skip}  Updated {total_success}  Failed {total_fail}"
        print(msg)
        logger.info(msg)

    if 'past_returns' in daily_metrics:  
        with duckdb.connect(db_file) as con:
            con.execute("""
                ALTER TABLE daily_metrics
                    ADD COLUMN IF NOT EXISTS return_1w DOUBLE;
                ALTER TABLE daily_metrics
                    ADD COLUMN IF NOT EXISTS return_1m DOUBLE;
                ALTER TABLE daily_metrics
                    ADD COLUMN IF NOT EXISTS return_6m DOUBLE;
                ALTER TABLE daily_metrics
                    ADD COLUMN IF NOT EXISTS return_1y DOUBLE;
            """)
        
        msg = "Calculating past returns including return_1w, return_1m, return_6m, and return_1y..."
        print(msg)
        logger.info(msg)
        with duckdb.connect(db_file) as con:
            past_return_results = bulk_past_returns_calculation_extended(con, start)
        msg = f"Past returns: Skipped {past_return_results['count_skip']}  Updated {past_return_results['count_success']}  Failed {past_return_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'streak' in daily_metrics:  
        with duckdb.connect(db_file) as con:
            con.execute("""
                ALTER TABLE daily_metrics
                    ADD COLUMN IF NOT EXISTS streak_0 INTEGER;
                ALTER TABLE daily_metrics
                    ADD COLUMN IF NOT EXISTS streak_close_close INTEGER;
            """)
        
        msg = "Calculating winning/losing streaks..."
        print(msg)
        logger.info(msg)
        with (duckdb.connect(db_file) as con):
            streak_results = bulk_streak_calculation(con, start)
        msg = f"Streaks: Skipped {streak_results['count_skip']}  Updated {streak_results['count_success']}  Failed {streak_results['count_fail']}"
        print(msg)
        logger.info(msg)
    
    if 'volatility' in daily_metrics:  
        with duckdb.connect(db_file) as con:
            con.execute("""
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS log_return DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS volatility_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS volatility_1m_zscore_3m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS volatility_3m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spy_log_return DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spy_volatility_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spy_volatility_3m DOUBLE;
            """)
            volatility_results = bulk_volatility_calculation(con, start)
            msg = f"Volatility: Skipped {volatility_results['count_skip']}  Updated {volatility_results['count_success']}  Failed {volatility_results['count_fail']}"
            print(msg)
            logger.info(msg)

    if 'trading_val' in daily_metrics:  
        with duckdb.connect(db_file) as con:
            con.execute("""
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS trading_val DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS trading_val_adtval DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS trading_val_zscore_3m DOUBLE;
            """)
            trading_val_results = bulk_trading_val_calculation(con, start)
            msg = f"Trading value: Skipped {trading_val_results['count_skip']}  Updated {trading_val_results['count_success']}  Failed {trading_val_results['count_fail']}"
            print(msg)
            logger.info(msg)

    if 'beta' in daily_metrics:  
        with duckdb.connect(db_file) as con:
            con.execute("""
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS beta_3m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS beta_3m_zscore_3m DOUBLE;
                        """)
            beta_results = bulk_beta_calculation(con, start)
            msg = f"Trading value: Skipped {beta_results['count_skip']}  Updated {beta_results['count_success']}  Failed {beta_results['count_fail']}"
            print(msg)
            logger.info(msg)


    if 'vxx' in daily_metrics:  
        print("Calculating VXX volatility metrics...")
        msg = "Calculating VXX metrics..."
        with duckdb.connect(db_file) as con:
            # Add columns if they don't exist
            sql_query = """
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS vxx_close_zscore_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS vxx_close_pctile_1y DOUBLE;
            """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            vxx_results = bulk_vxx_calculation(con, start)
        msg = f"VXX Metrics: Skipped {vxx_results['count_skip']}  Updated {vxx_results['count_success']}  Failed {vxx_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'atr' in daily_metrics:  
        msg = "Calculating ATR metrics..."
        with duckdb.connect(db_file) as con:
            # Add columns if they don't exist
            sql_query = """
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS atr_14 DOUBLE;
            """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            atr_results = bulk_atr_calculation(con, start)
        msg = f"ATR Metrics: Skipped {atr_results['count_skip']}  Updated {atr_results['count_success']}  Failed {atr_results['count_fail']}"
        print(msg)
        logger.info(msg)
        
    if 'skew' in daily_metrics:  
        msg = "Calculating skewness and kurtosis metrics..."
        with duckdb.connect(db_file) as con:
            # Add columns if they don't exist
            sql_query = """
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS skew_1m DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS kurt_1m DOUBLE;
            """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            skew_results = bulk_skew_calculation(con, start)
        msg = f"Skew/Kurtosis Metrics: Skipped {skew_results['count_skip']}  Updated {skew_results['count_success']}  Failed {skew_results['count_fail']}"
        print(msg)
        logger.info(msg)
    
    if 'distance' in daily_metrics:  
        print("Calculating distance from 52-week high/low metrics...")
        msg = "Calculating distance from 52-week high/low metrics..."
        with duckdb.connect(db_file) as con:
            # Add columns if they don't exist
            sql_query = """
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS dist_from_high_1y DOUBLE;
                        ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS dist_from_low_1y DOUBLE;
            """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            distance_results = bulk_distance_calculation(con, start)
        msg = f"Distance Metrics: Skipped {distance_results['count_skip']}  Updated {distance_results['count_success']}  Failed {distance_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'ema' in daily_metrics:
        msg = "Calculating 50-period EMA..."
        with duckdb.connect(db_file) as con:
            # Fetch the price data
            sql_query = """
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS ema_9 DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS ema_50 DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS ema_100 DOUBLE;"""
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            ema_results = bulk_ema_calculation(con, start_date=start)
        msg = f"EMA: Skipped {ema_results['count_skip']}  Updated {ema_results['count_success']}  Failed {ema_results['count_fail']}"
        print(msg)
        logger.info(msg)

    if 'stochastics' in daily_metrics:
        msg = "Calculating Stochastics..."
        with duckdb.connect(db_file) as con:
            # Fetch the price data
            sql_query = """
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS slowk DOUBLE;
                        ALTER TABLE daily_metrics
                            ADD COLUMN IF NOT EXISTS slowd DOUBLE;
                        """
            con.execute(sql_query)
            print(msg)
            logger.info(msg)
            stochastics_results = bulk_stochastics_calculation(con, start_date=start)
        msg = f"Stochastics: Skipped {stochastics_results['count_skip']}  Updated {stochastics_results['count_success']}  Failed {stochastics_results['count_fail']}"
        print(msg)
        logger.info(msg)
    # TODO: refactor to just NOPAT
    # if 'roic' in daily_metrics:
    #     msg = "Calculating ROIC (roic) with dependencies EBIT TTM, tax expense TTM, and NOPAT."
    #     print(msg)
    #     logger.info(msg)
    #     msg = "Calculating EBIT TTM."
    #     print(msg)
    #     logger.info(msg)
    #     calculate_ttm("ebit")
    #     msg = "Calculating tax expense TTM."
    #     print(msg)
    #     logger.info(msg)
    #     calculate_ttm("taxExp")
    #     msg = "Calculating NOPAT."
    #     print(msg)
    #     logger.info(msg)
    #     nopat(duckdb_con)
    #     msg = "Calculating ROIC."
    #     print(msg)
    #     logger.info(msg)
    #     roic(duckdb_con)
    #     msg = f"ROIC backtesting metric complete"
    #     print(msg)
    #     logger.info(msg)
    if {'margins', 'rank'}.intersection(daily_metrics):
        msg = "Calculating TTM for grossProfit, netinc, opinc, revenue, and freeCashFlow."
        print(msg)
        logger.info(msg)
        calculate_ttm('grossProfit')
        calculate_ttm('netinc')
        calculate_ttm('opinc')
        calculate_ttm('revenue')
        calculate_ttm('freeCashFlow')
        msg = "Calculating gross margins, operating margins, and profit margins (net margin) TTM."
        print(msg)
        logger.info(msg)
        margins()
        msg = "Margins calculated in fundamental metrics table."
        print(msg)
        logger.info(msg)
    if 'rank' in daily_metrics:
        msg = "Calculating rank percentiles for metrics."
        print(msg)
        logger.info(msg)
        # For enterprise multiple
        calculate_ttm('ebitda')
        slope_script_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'slope.py')
        # Calculate slopes before ranking
        subprocess.check_call([sys.executable, slope_script_path])
        # Calculate percentiles for each fundamental metric
        with duckdb.connect(db_file) as duckdb_con:
            create_relevant_metrics_fm(duckdb_con)
            rank_metric(duckdb_con, 'grossProfit_ttm')
            rank_metric(duckdb_con, 'opinc_ttm')
            rank_metric(duckdb_con, 'opinc_trend_slope')
            rank_metric(duckdb_con, 'netinc_ttm')
            rank_metric(duckdb_con, 'revenue_ttm')
            rank_metric(duckdb_con, 'gross_margin_ttm', by_industry=True)
            rank_metric(duckdb_con, 'operating_margin_ttm', by_industry=True)
            rank_metric(duckdb_con, 'net_margin_ttm')
            rank_metric(duckdb_con, 'freeCashFlow_ttm')
            rank_metric(duckdb_con, 'grossProfit_trend_slope')
            rank_metric(duckdb_con, 'opinc_trend_slope')
            rank_metric(duckdb_con, 'netinc_trend_slope')
            rank_metric(duckdb_con, 'revenue_trend_slope')
            rank_metric(duckdb_con, 'gross_margin_trend_slope', by_industry=True)
            rank_metric(duckdb_con, 'operating_margin_trend_slope', by_industry=True)
            rank_metric(duckdb_con, 'net_margin_trend_slope')
            rank_metric(duckdb_con, 'freeCashFlow_trend_slope')
            rank_metric(duckdb_con, 'currentRatio_trend_slope')
            rank_metric(duckdb_con, 'debt_trend_slope', order='DESC')
            rank_metric(duckdb_con, 'debtEquity_trend_slope', order='DESC')
            rank_metric(duckdb_con, 'roe_trend_slope')
        with duckdb.connect(db_file) as duckdb_con:
            create_relevant_metrics_fad(duckdb_con)
            rank_metric(duckdb_con, 'currentRatio', by_industry=True)
            rank_metric(duckdb_con, 'debt', by_industry=True, order='DESC')
            rank_metric(duckdb_con, 'debtEquity', by_industry=True, order='DESC')
            rank_metric(duckdb_con, 'piotroskiFScore')
            rank_metric(duckdb_con, 'roe')
        rank('2005-01-01')
        msg = "Rank percentiles calculated in fundamental metrics table."
        print(msg)
        logger.info(msg)
    create_max_returns()
    if 'consolidated' in daily_metrics:
        create_consolidated_metrics()
    msg = "Metrics update complete."
    print(msg)
    logger.info(msg)


update_metrics()

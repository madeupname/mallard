import configparser
import os

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

fundamental_metrics_table = config['tiingo']['fundamental_metrics_table']

def net_income_ttm(duckdb_con):
    """Calculate net income (earnings) over the trailing 12 months.
    Will be NULL if missing quarters to avoid misinterpretation.
    To maintain some semblance of accuracy for backtesting, this starts with data from the reported fundamentals table
    for the first quarter, but gets the previous 3 quarters from the amended fundamentals. The exception is it uses
    the TTM from all reported annual reports since that is guaranteed to be accurate for that date."""
    with duckdb_con.cursor() as con:
        # Insert or update avg_daily_trading_value in daily_metrics
        con.execute(f"""
        -- Step 1: Filter relevant data for reported and amended datasets
        WITH Reported AS (
            SELECT
                vendor_symbol_id, symbol, date, year, quarter, value
            FROM tiingo_fundamentals_reported
            WHERE dataCode = 'netIncComStock' AND quarter > 0 AND quarter < 4
        ),
        Amended AS (
            SELECT
                vendor_symbol_id, year, quarter, value
            FROM tiingo_fundamentals_amended_distinct
            WHERE dataCode = 'netIncComStock'
        ),
        -- Step 2: Compute TTM by combining reported quarter with previous three quarters from amended
            TTM AS (
                SELECT
                    r.vendor_symbol_id,
                    r.symbol,
                    r.date,
                    r.year,
                    r.quarter,
                    'netIncComStockTTM' AS metric,
                    r.value + a1.value + a2.value + a3.value AS value
                FROM Reported r
                LEFT JOIN Amended a1 ON r.vendor_symbol_id = a1.vendor_symbol_id
                                      AND a1.year = CASE WHEN r.quarter = 1 THEN r.year - 1 ELSE r.year END
                                      AND a1.quarter = CASE WHEN r.quarter = 1 THEN 4 ELSE r.quarter - 1 END
                LEFT JOIN Amended a2 ON r.vendor_symbol_id = a2.vendor_symbol_id
                                      AND a2.year = CASE WHEN r.quarter <= 2 THEN r.year - 1 ELSE r.year END
                                      AND a2.quarter = CASE WHEN r.quarter = 1 THEN 3 WHEN r.quarter = 2 THEN 4 ELSE r.quarter - 2 END
                LEFT JOIN Amended a3 ON r.vendor_symbol_id = a3.vendor_symbol_id
                                      AND a3.year = CASE WHEN r.quarter <= 3 THEN r.year - 1 ELSE r.year END
                                      AND a3.quarter = CASE WHEN r.quarter = 1 THEN 2 WHEN r.quarter = 2 THEN 3 WHEN r.quarter = 3 THEN 4 ELSE r.quarter - 3 END
            )
        -- Step 3: Output or store the results
        INSERT OR REPLACE INTO tiingo_fundamental_metrics_backtest BY NAME FROM TTM;
        
        -- For quarter 4, use quarter 0 from reported since that includes the year TTM and it is guaranteed accurate for that 
        -- point in time.
        INSERT OR REPLACE INTO tiingo_fundamental_metrics BY NAME
        FROM (SELECT vendor_symbol_id, symbol, date, year, 4 AS 'quarter', 'netIncomeStockTTM' AS metric, value
            FROM tiingo_fundamentals_reported
            WHERE dataCode = 'netIncComStock' AND quarter = 0
            ORDER BY symbol, year, quarter);        
        """)

def eps_ttm(duckdb_con):
    """Calculate earnings per share (EPS) over the trailing 12 months.
    Will be NULL if missing quarters to avoid misinterpretation.
    Uses netIncComStockTTM from fundamental_metrics as earnings and sharesBasic dataCode from
    tiingo_fundamentals_reported as number of shares."""
    with duckdb_con.cursor() as con:
        con.execute(f"""
        -- Calculate EPS based on net income TTM and basic shares outstanding
        SELECT
            fm.vendor_symbol_id,
            fm.symbol,
            fm.date,
            fm.year,
            fm.quarter,
            fm.metric AS earnings_metric,
            fm.value AS netIncComStockTTM,
            fr.value AS sharesBasic,
            CASE 
                WHEN fr.value > 0 THEN fm.value / fr.value 
                ELSE NULL 
            END AS EPS
        FROM
            fundamental_metrics fm
        JOIN
            (SELECT vendor_symbol_id, date, dataCode, value
             FROM tiingo_fundamentals_reported
             WHERE dataCode = 'sharesBasic') fr
        ON
            fm.vendor_symbol_id = fr.vendor_symbol_id AND
            fm.year = fr.year AND fm.quarter = fr.quarter 
        WHERE
            fm.metric = 'netIncComStockTTM';
        
        """)

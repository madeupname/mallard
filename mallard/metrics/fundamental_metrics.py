import configparser
import os

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

fundamental_metrics_table = config['tiingo']['fundamental_metrics_table']
fundamentals_reported_table = config['tiingo']['fundamentals_reported_table']
fundamentals_amended_distinct_table = config['tiingo']['fundamentals_amended_distinct_table']

def calculate_ttm(dataCode, duckdb_con):
    """Calculate the given metric (dataCode) over the trailing 12 months for backtesting.
        Will be NULL if missing quarters to avoid misinterpretation.
        To maintain some semblance of accuracy for backtesting, this starts with data from the reported fundamentals table
        for the first quarter, but gets the previous 3 quarters from the amended fundamentals. The exception is it uses
        the TTM from all reported annual reports since that is guaranteed to be accurate for that date."""
    metric = f"{dataCode}_ttm" # indicate trailing 12 months and backtesting

    with duckdb_con.cursor() as con:
            # Insert or update avg_daily_trading_value in daily_metrics
            con.execute(f"""
            -- Step 1: Filter relevant data for reported and amended datasets
            WITH Reported AS (
                SELECT
                    vendor_symbol_id, symbol, date, year, quarter, value
                FROM {fundamentals_reported_table}
                WHERE dataCode = '{dataCode}' AND quarter > 0 AND quarter < 4
            ),
            Amended AS (
                SELECT
                    vendor_symbol_id, year, quarter, value
                FROM {fundamentals_amended_distinct_table}
                WHERE dataCode = '{dataCode}'
            ),
            -- Step 2: Compute TTM by combining reported quarter with previous three quarters from amended
                TTM AS (
                    SELECT
                        r.vendor_symbol_id,
                        r.symbol,
                        r.date,
                        r.year,
                        r.quarter,
                        '{metric}' AS metric,
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
            INSERT OR REPLACE INTO {fundamental_metrics_table} BY NAME FROM TTM;
            
            -- For quarter 4, use quarter 0 from reported since that includes the year TTM and it is guaranteed accurate for that 
            -- point in time.
            INSERT OR REPLACE INTO {fundamental_metrics_table} BY NAME
            FROM (SELECT vendor_symbol_id, symbol, date, year, 4 AS 'quarter', '{metric}' AS metric, value
                FROM tiingo_fundamentals_reported
                WHERE dataCode = '{dataCode}' AND quarter = 0
                ORDER BY symbol, year, quarter);        
            """)

def nopat(duckdb_con):
    """Calculate NOPAT based on ebit_ttm and taxExp_ttm metrics."""
    with duckdb_con.cursor() as con:
        con.execute(f"""
        -- Calculate NOPAT and insert into fundamental_metrics
        INSERT OR REPLACE INTO {fundamental_metrics_table} (vendor_symbol_id, symbol, date, year, quarter, metric, value)
        SELECT
            e.vendor_symbol_id,
            e.symbol,
            e.date,  -- Assumes e.date is the same date when ebit_ttm was last updated
            e.year,
            e.quarter,
            'nopat' AS metric,
            e.value * (1 - (t.value / e.value)) AS value  -- NOPAT calculation
        FROM
            {fundamental_metrics_table} e
        JOIN
            {fundamental_metrics_table} t ON e.vendor_symbol_id = t.vendor_symbol_id AND e.date = t.date
        WHERE
            e.metric = 'ebit_ttm' AND
            t.metric = 'taxExp_ttm' AND
            e.value IS NOT NULL AND
            t.value IS NOT NULL AND
            e.value != 0;  -- Ensures EBIT is not zero to avoid division by zero
        """)

def roic(duckdb_con):
    """Calculate ROIC for backtesting."""
    with duckdb_con.cursor() as con:
        con.execute(f"""
        -- Calculate Invested Capital and ROIC for each date using the new NOPAT and debt metrics
        WITH BalanceData AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                dataCode,
                LAST_VALUE(value IGNORE NULLS) OVER (PARTITION BY vendor_symbol_id, dataCode ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_value
            FROM {fundamentals_reported_table}
        ),
        AggregatedData AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                MAX(CASE WHEN dataCode = 'equity' THEN last_value END) AS equity,
                MAX(CASE WHEN dataCode = 'debt' THEN last_value END) AS totalDebt,
                MAX(CASE WHEN dataCode = 'cashAndEq' THEN last_value END) AS cashAndEq,
                MAX(CASE WHEN dataCode = 'investmentsCurrent' THEN last_value END) AS investmentsCurrent
            FROM BalanceData
            GROUP BY vendor_symbol_id, symbol, date, year, quarter
        ),
        NOPATData AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                value AS nopat
            FROM {fundamental_metrics_table}
            WHERE metric = 'nopat'
        ),
        InvestedCapital AS (
            SELECT
                ad.vendor_symbol_id,
                ad.symbol,
                ad.date,
                ad.year,
                ad.quarter,
                ad.equity,
                ad.totalDebt,
                ad.cashAndEq,
                ad.investmentsCurrent,
                ad.equity + ad.totalDebt - ad.cashAndEq - ad.investmentsCurrent AS investedCapital
            FROM AggregatedData ad
        )
        INSERT OR REPLACE INTO {fundamental_metrics_table} (vendor_symbol_id, symbol, date, year, quarter, metric, value)
        SELECT
            ic.vendor_symbol_id,
            ic.symbol,
            ic.date,
            ic.year,
            ic.quarter,
            'roic' AS metric,
            (CASE WHEN nd.nopat > 0 AND ic.investedCapital > 0 THEN nd.nopat / ic.investedCapital ELSE NULL END) AS value
        FROM InvestedCapital ic
        JOIN NOPATData nd ON nd.vendor_symbol_id = ic.vendor_symbol_id AND nd.date = ic.date AND nd.year = ic.year AND nd.quarter = ic.quarter;
        """)

def eps_ttm(duckdb_con):
    """Calculate earnings per share (EPS) over the trailing 12 months.
    Will be NULL if missing quarters to avoid misinterpretation.
    Uses netIncComStock_TTM_BT from fundamental_metrics as earnings and sharesBasic dataCode from
    tiingo_fundamentals_reported as number of shares."""
    # TODO this is unfinished
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
            fm.metric = 'netIncComStock_ttm_bt';
        
        """)

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


def calculate_ttm(metric, duckdb_con):
    """Calculate the given metric (dataCode) over the trailing 12 months for backtesting.
        Will be NULL if missing quarters to avoid misinterpretation.
        To maintain some semblance of accuracy for backtesting, this starts with data from the reported fundamentals table
        for the first quarter, but gets the previous 3 quarters from the amended fundamentals. The exception is it uses
        the TTM from all reported annual reports since that is guaranteed to be accurate for that date."""
    metric_ttm = f"{metric}_ttm"  # indicate trailing 12 months and backtesting

    with duckdb_con.cursor() as con:
        # Insert or update avg_daily_trading_value in daily_metrics
        query = f"""
        WITH Latest_Metrics AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                LAST_VALUE({metric} IGNORE NULLS) OVER (
                    PARTITION BY vendor_symbol_id, year, quarter
                    ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS latest_metric
            FROM {fundamentals_reported_table}
            WHERE quarter != 0  -- Exclude quarter 0 from general data processing
        ),
        
        Aggregated_Reported AS (
            SELECT
                vendor_symbol_id,
                ANY_VALUE(symbol) AS symbol,
                MAX(date) AS max_date,  -- The latest date for each quarter
                year,
                quarter,
                ANY_VALUE(latest_metric) AS latest_metric
            FROM Latest_Metrics
            GROUP BY vendor_symbol_id, year, quarter
        ),
        
        Combined_TTM AS (
            SELECT
                r.vendor_symbol_id,
                r.symbol,
                r.max_date AS date,
                r.year,
                r.quarter,
                CASE
                    WHEN r.quarter = 4 THEN a0.{metric} -- Use quarter 0 data for Q4
                    ELSE r.latest_metric
                END AS current_quarter_metric,
                a1.{metric} AS prev_quarter_1,
                a2.{metric} AS prev_quarter_2,
                a3.{metric} AS prev_quarter_3
            FROM Aggregated_Reported r
            LEFT JOIN {fundamentals_amended_distinct_table} a0 ON r.vendor_symbol_id = a0.vendor_symbol_id
                AND a0.year = r.year
                AND a0.quarter = 0  -- Specifically for Q4 calculations
            LEFT JOIN {fundamentals_amended_distinct_table} a1 ON r.vendor_symbol_id = a1.vendor_symbol_id
                AND a1.year = CASE WHEN r.quarter = 1 THEN r.year - 1 ELSE r.year END
                AND a1.quarter = CASE WHEN r.quarter = 1 THEN 4 ELSE r.quarter - 1 END
            LEFT JOIN {fundamentals_amended_distinct_table} a2 ON r.vendor_symbol_id = a2.vendor_symbol_id
                AND a2.year = CASE WHEN r.quarter <= 2 THEN r.year - 1 ELSE r.year END
                AND a2.quarter = CASE WHEN r.quarter = 1 THEN 3 WHEN r.quarter = 2 THEN 4 ELSE r.quarter - 2 END
            LEFT JOIN {fundamentals_amended_distinct_table} a3 ON r.vendor_symbol_id = a3.vendor_symbol_id
                AND a3.year = CASE WHEN r.quarter <= 3 THEN r.year - 1 ELSE r.year END
                AND a3.quarter = CASE WHEN r.quarter = 1 THEN 2 WHEN r.quarter = 2 THEN 3 WHEN r.quarter = 3 THEN 4 ELSE r.quarter - 3 END
        ),
        
        Final_TTM AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                CASE
                    WHEN current_quarter_metric IS NULL OR prev_quarter_1 IS NULL OR prev_quarter_2 IS NULL OR prev_quarter_3 IS NULL
                    THEN NULL
                    ELSE current_quarter_metric + prev_quarter_1 + prev_quarter_2 + prev_quarter_3
                END AS {metric_ttm}
            FROM Combined_TTM
        )
        
        INSERT OR REPLACE INTO {fundamental_metrics_table} (vendor_symbol_id, symbol, date, year, quarter, {metric_ttm})
        SELECT
            vendor_symbol_id,
            symbol,
            date,
            year,
            quarter,
            {metric_ttm}
        FROM
            Final_TTM;        
        """
        # print(query)
        try:
            con.execute(query)
        except Exception as e:
            print(f"Error calculating {metric_ttm}.\n{e}")
            # print stack trace to console
            import traceback
            traceback.print_exc()


def nopat(duckdb_con):
    """Calculate NOPAT based on ebit_ttm and taxExp_ttm metrics."""
    with duckdb_con.cursor() as con:
        con.execute(f"""
        -- Calculate NOPAT and insert into fundamental_metrics
        INSERT OR REPLACE INTO {fundamental_metrics_table} (vendor_symbol_id, symbol, date, year, quarter, nopat)
        SELECT
            vendor_symbol_id,
            symbol,
            date,  -- Assumes e.date is the same date when ebit_ttm was last updated
            year,
            quarter,
            ebit_ttm * (1 - (taxExp_ttm / ebit_ttm)) AS nopat  -- NOPAT calculation
        FROM
            {fundamental_metrics_table}
        WHERE
            ebit_ttm != 0;  -- Ensures EBIT is not zero to avoid division by zero
        """)


def roic(duckdb_con):
    """Calculate ROIC for backtesting."""
    with duckdb_con.cursor() as con:
        query = f"""
        -- Calculate ROIC for backtesting
        WITH InvestedCapital AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                LAST_VALUE(equity IGNORE NULLS) OVER (
                            PARTITION BY vendor_symbol_id, year, quarter
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS last_equity,
                LAST_VALUE(debt IGNORE NULLS) OVER (
                            PARTITION BY vendor_symbol_id, year, quarter
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS last_debt,
                LAST_VALUE(cashAndEq IGNORE NULLS) OVER (
                            PARTITION BY vendor_symbol_id, year, quarter
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS last_cashAndEq,
                LAST_VALUE(investmentsCurrent IGNORE NULLS) OVER (
                            PARTITION BY vendor_symbol_id, year, quarter
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS last_investmentsCurrent
            FROM {fundamentals_reported_table}
        ),
        InvestedCapitalFinal AS (
            SELECT
                vendor_symbol_id,
                symbol,
                date,
                year,
                quarter,
                last_equity + last_debt - last_cashAndEq - last_investmentsCurrent AS investedCapital
            FROM InvestedCapital
        )
        INSERT OR REPLACE INTO {fundamental_metrics_table} (vendor_symbol_id, symbol, date, year, quarter, roic)
        SELECT
            ic.vendor_symbol_id,
            ic.symbol,
            ic.date,
            ic.year,
            ic.quarter,
            CASE WHEN fm.nopat > 0 AND ic.investedCapital > 0 THEN fm.nopat / ic.investedCapital ELSE NULL END AS value
        FROM InvestedCapitalFinal ic
        JOIN {fundamental_metrics_table} fm ON ic.vendor_symbol_id = fm.vendor_symbol_id AND ic.date = fm.date AND ic.quarter = fm.quarter
        """
        con.execute(query)


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

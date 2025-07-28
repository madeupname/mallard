import configparser
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

import duckdb
import numpy as np
import pandas as pd

from mallard.metrics.slope_util import get_raw_income
from mallard.tiingo.tiingo_util import logger

# Get config file
config_file = "/data/mallard/config.ini"
# Set MALLARD_CONFIG to the config file
os.environ['MALLARD_CONFIG'] = config_file
config = configparser.ConfigParser()
config.read(config_file)

db_file = config['DEFAULT']['db_file']
fundamental_metrics_table = config['tiingo']['fundamental_metrics_table']
threads = int(config['DEFAULT']['threads'])
duckdb_con = duckdb.connect(db_file)

rows_processed = 0
balance_sheet_metrics = ['currentRatio', 'debt', 'debtEquity', 'piotroskiFScore', 'roe']
column_names = duckdb_con.sql(f"SELECT * FROM {fundamental_metrics_table} LIMIT 1").columns

income_statement_metrics = [col for col in column_names if col.endswith("_ttm")]
combined_metrics = balance_sheet_metrics + income_statement_metrics
msg = f"Calculating trend slopes for metrics: {combined_metrics}"
print(msg)
logger.info(msg)
with duckdb_con.cursor() as con:
    for col in combined_metrics:
        con.execute(
            f"ALTER TABLE {fundamental_metrics_table} ADD COLUMN IF NOT EXISTS {col[:-4] if col.endswith('_ttm') else col}_trend_slope DOUBLE")


def calculate_slope(batch: List[Dict[str, Any]], metrics: List[str]) -> List[Dict[str, Any]]:
    global rows_processed
    results = []
    for row in batch:
        result = row.copy()
        for metric in metrics:
            trimmed_metric = metric[:-4] if metric.endswith("_ttm") else metric
            y = np.array([row[f'{trimmed_metric}_y3'], row[f'{trimmed_metric}_y2'], row[f'{trimmed_metric}_y1'],
                          row[f'{metric}']])
            # Verify that there are no NaN values in the array and all values are numeric
            if not all(isinstance(val, (int, float)) and not np.isnan(val) for val in y):
                result[f'{trimmed_metric}_trend_slope'] = None
                continue
            x = np.array([1, 2, 3, 4])  # Time points
            first_non_zero = next((val for val in y if val != 0 and not np.isnan(val)), None)
            if first_non_zero is None:  # All values are zero or NaN
                result[f'{trimmed_metric}_trend_slope'] = None
            else:
                # Handle cases where first_non_zero is negative, which will reverse the direction of the slope
                y_normalized = y / abs(first_non_zero)
                slope, _ = np.polyfit(x, y_normalized, 1)
                result[f'{trimmed_metric}_trend_slope'] = slope
        results.append(result)
        rows_processed += 1
    return results


def batch_processor(df: pd.DataFrame, func: callable, metrics: List[str], batch_size: int = 1000) -> pd.DataFrame:
    msg = f"Calculating slope for {len(df)} rows in batches of {batch_size}"
    print(msg)
    logger.info(msg)
    # Create batches of rows as dictionaries
    batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
    results = []
    if threads == 1:
        for batch in batches:
            batch_results = func(batch.to_dict('records'), metrics)
            results.extend(batch_results)
    else:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(func, batch.to_dict('records'), metrics): batch for batch in batches}
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    results.extend(batch_results)  # Collect all results
                except Exception as exc:
                    logger.error(f"Exception occurred: {exc}")

    # Create a DataFrame from the results
    msg = f"Processed {rows_processed} rows\nCreating DataFrame from {len(results)} results"
    print(msg)
    logger.info(msg)
    update_df = pd.DataFrame(results)
    return update_df


def update_database(final_df: pd.DataFrame, metrics: List[str]):
    """Update the database with the calculated slope values. All metrics should be without the _ttm suffix."""
    # Remove _ttm suffix from metrics
    metrics = [metric[:-4] if metric.endswith("_ttm") else metric for metric in metrics]
    # Execute the bulk update using a SQL command
    msg = f"Updating database with {len(final_df)} rows"
    print(msg)
    logger.info(msg)
    set_query = ', '.join([f"{metric}_trend_slope = final_df.{metric}_trend_slope" for metric in metrics])
    query = f"""
        UPDATE tiingo_fundamental_metrics
        SET
            {set_query}
        FROM final_df
        WHERE
            tiingo_fundamental_metrics.vendor_symbol_id = final_df.vendor_symbol_id AND
            tiingo_fundamental_metrics.date = final_df.date AND
            tiingo_fundamental_metrics.year = final_df.year AND
            tiingo_fundamental_metrics.quarter = final_df.quarter
    """
    duckdb_con.execute(query)
    msg = "Database updated"
    print(msg)
    logger.info(msg)


# Balance sheet and overview metrics
select_columns = [f"fr.{col}, a1.{col} AS {col}_y1, a2.{col} AS {col}_y2, a3.{col} AS {col}_y3" for col in
                  balance_sheet_metrics]
where_columns = [
    f"(fr.{col} IS NOT NULL AND a1.{col} IS NOT NULL AND a2.{col} IS NOT NULL AND a3.{col} IS NOT NULL)" for col in
    balance_sheet_metrics]
# Remember: Tiingo does not give you overview metrics for annual reports, so use quarter 4, which will be the same.
query = f"""
SELECT
    fr.vendor_symbol_id,
    fr.year,
    fr.quarter,
    fr.date,
    {', '.join(select_columns)}
FROM
    tiingo_fundamentals_reported fr
JOIN
    tiingo_fundamentals_amended_distinct a1 ON fr.vendor_symbol_id = a1.vendor_symbol_id AND a1.year = fr.year - 1 AND a1.quarter = 4
JOIN
    tiingo_fundamentals_amended_distinct a2 ON fr.vendor_symbol_id = a2.vendor_symbol_id AND a2.year = fr.year - 2 AND a2.quarter = 4
JOIN
    tiingo_fundamentals_amended_distinct a3 ON fr.vendor_symbol_id = a3.vendor_symbol_id AND a3.year = fr.year - 3 AND a3.quarter = 4
WHERE
    fr.quarter != 0 AND ({' OR '.join(where_columns)})
ORDER BY
    fr.vendor_symbol_id, fr.year, fr.quarter;
"""
unprocessed_balance_df = duckdb_con.execute(query).fetchdf()

unprocessed_income_df = get_raw_income(duckdb_con, income_statement_metrics)
unprocessed_combined_df = pd.merge(unprocessed_income_df, unprocessed_balance_df,
                                   on=['vendor_symbol_id', 'year', 'quarter', 'date'], how='outer')

# Set batch size to the number of rows in the DataFrame divided by the number of threads
batch_size = len(unprocessed_combined_df) // threads
combined_df = batch_processor(unprocessed_combined_df, calculate_slope, combined_metrics, batch_size)

# Drop unnecessary columns, keep only the vendor_symbol_id, date, year, quarter, and those ending with _trend_slope
combined_df = combined_df[['vendor_symbol_id', 'date', 'year', 'quarter'] + [col for col in combined_df.columns if
                                                                             col.endswith("_trend_slope")]]

update_database(combined_df, combined_metrics)
msg = f"Calculated slopes, {rows_processed} rows processed"
print(msg)
logger.info(msg)

# Calculate the mean of the slopes

duckdb_con.close()

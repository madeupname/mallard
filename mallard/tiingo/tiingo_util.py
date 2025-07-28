# Get config file
import configparser
import logging
import os

import duckdb
import pandas
import pandas as pd
from duckdb import DuckDBPyConnection

config_file = os.getenv("MALLARD_CONFIG")
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

logger = logging.getLogger(__name__)
# Format with created, level, filename, message
logging.basicConfig(filename=config['DEFAULT']['log_file'], level=config['DEFAULT']['log_level'],
                    format=config['DEFAULT']['log_format'])

exclude_dir = config['tiingo']['dir'] + '/exclude'
os.makedirs(exclude_dir, exist_ok=True)
quarantine_dir = config['tiingo']['dir'] + '/quarantine'
os.makedirs(quarantine_dir, exist_ok=True)


def quarantine_file(symbol, file_name, is_active=False):
    """Move the file to the quarantine or exclude directory if config allows. Log error. """
    error = f"{symbol}: No data returned or incorrect format. "
    target_dir = quarantine_dir if is_active else exclude_dir
    if config.getboolean('DEFAULT', 'quarantine_bad_data'):
        # Check if the file exists
        if os.path.exists(file_name):
            os.rename(file_name, os.path.join(target_dir, os.path.basename(file_name)))
            error += "Moving to quarantine. "
    logger.error(error)


def quarantine_data(symbol, vendor_symbol_id, is_active, data, file_type):
    """Save the data as a file in the appropriate directory per is_active."""
    target_dir = quarantine_dir if is_active else exclude_dir
    with open(os.path.join(target_dir, f"{symbol}_{vendor_symbol_id}_{file_type}.csv"), 'wb') as f:
        f.write(data)
    logger.error(f"{symbol}: No data returned or incorrect format. Moving to {target_dir}.")


# dependencies link metrics to their dependent metrics. Not all metrics have dependencies
dependencies = {'currentRatio': ['assetsCurrent'],
                'assetsCurrent': ['acctRec', 'cashAndEq', 'inventory', 'investmentsCurrent']}


def collect_dependencies(metric, collected=None):
    if collected is None:
        collected = set()

    # Add the metric itself
    collected.add(metric)

    # Recursively add dependencies
    for dep in dependencies.get(metric, []):
        if dep not in collected:  # Avoid re-processing and potential cycles
            collect_dependencies(dep, collected)

    return collected


def get_fundamentals(vendor_symbol_id: str, date: str, metrics: list[str], reported: bool = True,
                     duckdb_con: DuckDBPyConnection = None) -> pd.DataFrame:
    """Gets a dataframe containing the requested metrics (fundamental data) for a given vendor_symbol_id and date.
    The data is retrieved from the fundamentals_reported_table or fundamentals_amended_table based on the reported flag.
    """
    if duckdb_con:
        con = duckdb_con.cursor()
    else:
        con = duckdb.connect(config['DEFAULT']['db_file'])
    table = config['tiingo']['fundamentals_reported_table'] if reported else config['tiingo'][
        'fundamentals_amended_table']
    # Determine the most recent quarter and year from the target_date for the given vendor_symbol_id
    quarter_year_query = f"""
    SELECT year, quarter
    FROM {table}
    WHERE vendor_symbol_id = ?
      AND date <= ?
    ORDER BY year DESC, quarter DESC
    LIMIT 1;
    """
    year, quarter = con.execute(quarter_year_query, (vendor_symbol_id, date)).fetchall()[0]

    # If metrics are in dependencies, add the dependent metrics to the list. Store in a set to avoid duplicates.
    all_metrics = set()
    for metric in metrics:
        needed = collect_dependencies(metric)
        all_metrics.update(needed)

    # Prepare SQL query to select the most recent non-null entries within the identified quarter and year
    metrics_query_part = ', '.join(
        [f"LAST_VALUE({metric} IGNORE NULLS) OVER (PARTITION BY vendor_symbol_id ORDER BY date) AS {metric}" for metric
         in all_metrics])
    query = f"""
    SELECT vendor_symbol_id, symbol, date, {metrics_query_part}
    FROM {table}
    WHERE vendor_symbol_id = ? AND year = ? AND quarter = ?
    AND date <= ?
    ORDER BY date DESC
    LIMIT 1;
    """

    # Execute the query for metrics
    df = con.execute(query, (vendor_symbol_id, year, quarter, date)).df()

    # Close the database connection
    con.close()

    # Handle special cases.

    return df


def get_assetsCurrent(df: pandas.DataFrame):
    """Calculate current assets from the dataframe. We first check if assetsCurrent has a numeric value in the that
    column. If not, we proceed to calculate it from component columns:
     'acctRec', 'cashAndEq', 'inventory', 'investmentsCurrent'."""
    if 'assetsCurrent' in df.columns:
        return df['assetsCurrent'].iloc[0]
    else:
        # Calculate assetsCurrent from components, but any can be null or NaN so we use fillna(0) to avoid NaN.
        # We also use sum() to sum the values of the components.
        return df[['acctRec', 'cashAndEq', 'inventory', 'investmentsCurrent']].fillna(0).sum()


# Add this function to the existing tiingo_util.py file

import datetime
from datetime import timedelta
import pytz
import exchange_calendars as xcals

# Eastern timezone for cutoff time
ET = pytz.timezone('US/Eastern')


def get_next_trading_date_to_download(max_date_in_db):
    """
    Get the next trading date that we should download data for.
    Uses NYSE calendar since data is finalized by 5PM ET for all exchanges.
    
    Args:
        max_date_in_db: Latest date we have in the database (can be None)
    
    Returns:
        datetime.date or None: Next trading date to download, or None if no data should be downloaded yet
    """
    cal = xcals.get_calendar('XNYS')  # NYSE calendar for all exchanges

    # Get current time in ET
    now_et = datetime.datetime.now(ET)
    today = now_et.date()

    # Get the calendar's valid date range
    cal_start = cal.first_session.date()
    cal_end = cal.last_session.date()

    # Determine the next date we should try to download
    if max_date_in_db is None:
        # No data in DB, start from a reasonable date (e.g., 30 days ago)
        candidate_date = today - timedelta(days=30)
    else:
        # Start from the day after our last data
        candidate_date = max_date_in_db + timedelta(days=1)

    # If candidate_date is before the calendar's start date, we can't use the calendar
    # In this case, just return the candidate_date if it's a reasonable trading day
    if candidate_date < cal_start:
        # For dates before the calendar, we'll assume Mon-Fri are trading days
        # and skip obvious holidays, but this is a fallback for old data
        if candidate_date > today:
            return None

        # Simple weekday check for pre-calendar dates
        while candidate_date.weekday() >= 5:  # Skip weekends
            candidate_date += timedelta(days=1)
            if candidate_date >= cal_start:
                break

        if candidate_date >= cal_start:
            # We've reached the calendar range, use the calendar logic below
            pass
        else:
            # Still before calendar range, do time check and return
            if candidate_date == today:
                cutoff_time = ET.localize(datetime.datetime.combine(candidate_date, datetime.time(17, 0)))
                if now_et < cutoff_time:
                    return None
            elif candidate_date > today:
                return None
            return candidate_date

    # Use calendar for dates within its range
    end_date = min(today + timedelta(days=5), cal_end)

    try:
        trading_days = cal.sessions_in_range(max(candidate_date, cal_start), end_date)
    except Exception as e:
        logger.warning(f"Error getting trading days from calendar: {e}")
        # Fallback to simple weekday logic
        if candidate_date > today:
            return None
        while candidate_date.weekday() >= 5:  # Skip weekends
            candidate_date += timedelta(days=1)
        if candidate_date == today:
            cutoff_time = ET.localize(datetime.datetime.combine(candidate_date, datetime.time(17, 0)))
            if now_et < cutoff_time:
                return None
        return candidate_date if candidate_date <= today else None

    if len(trading_days) == 0:
        return None

    # Find the first trading day we don't have data for
    next_trading_date = None
    for session in trading_days:
        session_date = session.date()

        # If this date is after our max date in DB (or we have no data), this is our candidate
        if max_date_in_db is None or session_date > max_date_in_db:
            next_trading_date = session_date
            break

    if next_trading_date is None:
        return None

    # Check if we should download data for this date
    # Data is available after 5 PM ET on the trading day
    cutoff_time = ET.localize(datetime.datetime.combine(next_trading_date, datetime.time(17, 0)))

    # If the next trading date is today, check if it's after 5 PM ET
    if next_trading_date == today:
        if now_et < cutoff_time:
            return None

    # If the next trading date is in the future, skip it
    if next_trading_date > today:
        return None

    return next_trading_date

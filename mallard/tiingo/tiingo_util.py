# Get config file
import configparser
import logging
import os

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

"""add minutes table

NOTE!!! This is for Alpaca minute data, NOT Tiingo or IBKR!

Revision ID: 917dc1a10016
Revises: 40c1fa981e95
Create Date: 2025-01-23 01:08:41.825480

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
# noinspection PyUnresolvedReferences
from AlembicDuckDBImpl import AlembicDuckDBImpl
import os
import configparser

# revision identifiers, used by Alembic.
revision: str = '917dc1a10016'
down_revision: Union[str, None] = '40c1fa981e95'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Get MALLARD_CONFIG, read config file
config_file = os.getenv("MALLARD_CONFIG")
if not config_file:
    raise ValueError("MALLARD_CONFIG environment variable not set.")
mallard_config = configparser.ConfigParser()
mallard_config.read(config_file)
# Get location of DuckDB database file
db_file = mallard_config["DEFAULT"]["db_file"]
minutes_table = mallard_config['alpaca']['minutes_table']


def upgrade() -> None:
    conn = op.get_bind()
    # Create intraday price history table for minute bars retrieved from Alpaca
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {minutes_table} (
        vendor_symbol_id VARCHAR, 
        symbol VARCHAR, 
        "date" TIMESTAMPTZ, 
        "open" DOUBLE, 
        high DOUBLE, 
        low DOUBLE, 
        "close" DOUBLE, 
        volume BIGINT, 
        vwap DOUBLE,
        trade_count INT, 
        PRIMARY KEY (vendor_symbol_id, "date"))
    """)
    conn.execute(query)


def downgrade() -> None:
    conn = op.get_bind()
    query = sa.text(f"""
    DROP TABLE IF EXISTS {minutes_table}
    """)
    conn.execute(query)

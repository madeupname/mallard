"""add intraday table

NOTE!!! This is for IBKR minute data, NOT Tiingo!

Revision ID: f37d66b0181c
Revises: 188a19e5f040
Create Date: 2024-10-16 18:10:23.563132

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
# noinspection PyUnresolvedReferences
from AlembicDuckDBImpl import AlembicDuckDBImpl
import os
import configparser

# revision identifiers, used by Alembic.
revision: str = 'f37d66b0181c'
down_revision: Union[str, None] = '188a19e5f040'
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
intraday_table = mallard_config['ibkr']['intraday_table']


def upgrade() -> None:
    conn = op.get_bind()
    # Create intraday price history table for minute bars retrieved from IBKR
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {intraday_table} (
        vendor_symbol_id VARCHAR, 
        symbol VARCHAR, 
        "date" TIMESTAMPTZ, 
        bar_size VARCHAR,
        "open" DOUBLE, 
        high DOUBLE, 
        low DOUBLE, 
        "close" DOUBLE, 
        volume BIGINT, 
        PRIMARY KEY (vendor_symbol_id, "date", bar_size))
    """)
    conn.execute(query)


def downgrade() -> None:
    conn = op.get_bind()
    query = sa.text(f"""
    DROP TABLE IF EXISTS {intraday_table}
    """)
    conn.execute(query)

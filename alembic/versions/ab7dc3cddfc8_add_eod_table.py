"""Add EOD table

Revision ID: ab7dc3cddfc8
Revises: 8821f7a5f5cb
Create Date: 2024-04-23 00:40:34.416055

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from AlembicDuckDBImpl import AlembicDuckDBImpl

# revision identifiers, used by Alembic.
revision: str = 'ab7dc3cddfc8'
down_revision: Union[str, None] = '8821f7a5f5cb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Get MALLARD_CONFIG, read config file
import os
import configparser

config_file = os.getenv("MALLARD_CONFIG")
if not config_file:
    raise ValueError("MALLARD_CONFIG environment variable not set.")
mallard_config = configparser.ConfigParser()
mallard_config.read(config_file)
# Get location of DuckDB database file
db_file = mallard_config["DEFAULT"]["db_file"]


def upgrade() -> None:
    eod_table = mallard_config['tiingo']['eod_table']
    # Get connection from Alembic
    conn = op.get_bind()
    # Create End of Day (EOD) price history table
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {eod_table}
    (vendor_symbol_id VARCHAR, symbol VARCHAR, date DATE, close DOUBLE, high DOUBLE, low DOUBLE, open DOUBLE, volume BIGINT, adj_close DOUBLE,
    adj_high DOUBLE, adj_low DOUBLE, adj_open DOUBLE, adj_volume BIGINT, div_cash DOUBLE, split_factor DOUBLE,
    PRIMARY KEY (vendor_symbol_id, date))
    """)
    conn.execute(query)


def downgrade() -> None:
    eod_table = mallard_config['tiingo']['eod_table']
    # Get connection from Alembic
    conn = op.get_bind()
    # Drop End of Day (EOD) price history table
    query = sa.text(f"""
    DROP TABLE IF EXISTS {eod_table}
    """)
    conn.execute(query)

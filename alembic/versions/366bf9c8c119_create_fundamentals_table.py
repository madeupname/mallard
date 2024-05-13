"""Create fundamentals table

Revision ID: 366bf9c8c119
Revises: ab7dc3cddfc8
Create Date: 2024-04-27 22:47:11.725889

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl

# revision identifiers, used by Alembic.
revision: str = '366bf9c8c119'
down_revision: Union[str, None] = 'ab7dc3cddfc8'
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
fundamentals_amended_table = mallard_config['tiingo']['fundamentals_amended_table']


def upgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Create fundamentals table
    # vendor_symbol_id,symbol,date,year,quarter,statementType,dataCode,value
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {fundamentals_amended_table}
    (vendor_symbol_id VARCHAR, symbol VARCHAR, date DATE, year INT, quarter INT, statementType VARCHAR, dataCode VARCHAR, 
    value DOUBLE)
    """)
    conn.execute(query)


def downgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Drop fundamentals table
    query = sa.text(f"""
    DROP TABLE IF EXISTS {fundamentals_amended_table}
    """)
    conn.execute(query)

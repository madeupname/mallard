"""Create reported fundamentals table

Revision ID: b9313f131122
Revises: 366bf9c8c119
Create Date: 2024-05-03 15:37:27.254138

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl

# revision identifiers, used by Alembic.
revision: str = 'b9313f131122'
down_revision: Union[str, None] = '366bf9c8c119'
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
    fundamentals_reported_table = mallard_config['tiingo']['fundamentals_reported_table']
    # Get connection from Alembic
    conn = op.get_bind()
    # Create fundamentals table
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {fundamentals_reported_table}
    (vendor_symbol_id VARCHAR, symbol VARCHAR, date DATE, year INT, quarter INT, statementType VARCHAR, dataCode VARCHAR, 
    value DOUBLE)
    """)
    conn.execute(query)
    fundamentals_amended_table = mallard_config['tiingo']['fundamentals_amended_table']
    # Change the name of tiingo_fundamentals table to amended_fundamentals_table
    query = sa.text(f"""
    ALTER TABLE tiingo RENAME TO {fundamentals_amended_table}
    """)


def downgrade() -> None:
    fundamentals_reported_table = mallard_config['tiingo']['fundamentals_reported_table']
    # Get connection from Alembic
    conn = op.get_bind()
    # Drop fundamentals table
    query = sa.text(f"""
    DROP TABLE IF EXISTS {fundamentals_reported_table}
    """)
    conn.execute(query)

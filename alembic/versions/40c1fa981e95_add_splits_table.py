"""add splits table

Revision ID: 40c1fa981e95
Revises: f37d66b0181c
Create Date: 2024-11-25 18:32:13.644462

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl

# revision identifiers, used by Alembic.
revision: str = '40c1fa981e95'
down_revision: Union[str, None] = 'f37d66b0181c'
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
splits_table = mallard_config['tiingo']['splits_table']


def upgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Create splits table
    # vendor_symbol_id,symbol,date,split_from,split_to,split_factor,split_status
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {splits_table} (
        vendor_symbol_id VARCHAR,
        symbol VARCHAR,
        date DATE,
        split_from DOUBLE,
        split_to DOUBLE,
        split_factor DOUBLE,
        PRIMARY KEY (vendor_symbol_id, date)
    );
    """)
    conn.execute(query)


def downgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Drop splits table
    query = sa.text(f"DROP TABLE IF EXISTS {splits_table};")
    conn.execute(query)
    

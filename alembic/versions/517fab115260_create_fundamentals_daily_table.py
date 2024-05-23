"""Create fundamentals daily table

Revision ID: 517fab115260
Revises: b9313f131122
Create Date: 2024-05-10 14:53:14.939478

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '517fab115260'
down_revision: Union[str, None] = 'b9313f131122'
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
    fundamentals_daily_table = mallard_config['tiingo']['fundamentals_daily_table']
    # Get connection from Alembic
    conn = op.get_bind()
    # Create fundamentals daily table
    query = sa.text(f"""
        CREATE TABLE IF NOT EXISTS {fundamentals_daily_table}
        (vendor_symbol_id VARCHAR PRIMARY KEY, symbol VARCHAR, date DATE, market_cap DOUBLE, enterprise_val DOUBLE, pe_ratio DOUBLE,
        pb_ratio DOUBLE, trailing_peg_1y DOUBLE)
        """)
    conn.execute(query)


def downgrade() -> None:
    pass

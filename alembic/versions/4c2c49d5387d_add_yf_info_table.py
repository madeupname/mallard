"""add yf_info table

Revision ID: 4c2c49d5387d
Revises: 917dc1a10016
Create Date: 2025-05-23 16:07:32.722110

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl


# revision identifiers, used by Alembic.
revision: str = '4c2c49d5387d'
down_revision: Union[str, None] = '917dc1a10016'
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
yf_info_table = mallard_config['tiingo']['yf_info_table']

    
def upgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Create yf_info table
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS yf_info (
        vendor_symbol_id VARCHAR,
        symbol VARCHAR,
        long_business_summary TEXT,
        currency VARCHAR(10),
        category VARCHAR(100),
        fund_family VARCHAR(100),
        fund_inception_date BIGINT,
        legal_type VARCHAR(100),
        quote_type VARCHAR(50),
        region VARCHAR(50),
        type_disp VARCHAR(50),
        long_name VARCHAR(255),
        exchange VARCHAR(50),
        exchange_timezone_name VARCHAR(100),
        exchange_timezone_short_name VARCHAR(10),
        market VARCHAR(50),
        short_name VARCHAR(255),
        full_exchange_name VARCHAR(100),
        net_expense_ratio DOUBLE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (vendor_symbol_id)
    );
    """)
    conn.execute(query)


def downgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Drop splits table
    query = sa.text(f"DROP TABLE IF EXISTS {yf_info_table};")
    conn.execute(query)

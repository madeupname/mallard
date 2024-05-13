"""create tiingo_ticker table

Revision ID: 8821f7a5f5cb
Revises: 
Create Date: 2024-04-16 15:08:32.958334

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl

# revision identifiers, used by Alembic.
revision: str = '8821f7a5f5cb'
down_revision: Union[str, None] = None
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
    symbols_table = mallard_config['tiingo']['symbols_table']
    # Get connection from Alembic
    conn = op.get_bind()
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {symbols_table}
    (symbol VARCHAR PRIMARY KEY, exchange VARCHAR, asset_type VARCHAR, price_currency VARCHAR, start_date DATE, end_date DATE)""")
    conn.execute(query)
    fundamentals_meta_table = mallard_config['tiingo']['fundamentals_meta_table']
    query = sa.text(f"""
    CREATE TABLE IF NOT EXISTS {fundamentals_meta_table}(
            vendor_symbol_id VARCHAR PRIMARY KEY,
            symbol VARCHAR,
            "name" VARCHAR,
            is_active BOOLEAN,
            is_adr BOOLEAN,
            sector VARCHAR,
            industry VARCHAR,
            sic_code VARCHAR,
            sic_sector VARCHAR,
            sic_industry VARCHAR,
            reporting_currency VARCHAR,
            location VARCHAR,
            company_website VARCHAR,
            sec_filing_website VARCHAR,
            statement_last_updated TIMESTAMPTZ,
            daily_last_updated TIMESTAMPTZ,
            vendor_entity_id VARCHAR)
    """)
    conn.execute(query)


def downgrade() -> None:
    table_name = mallard_config['tiingo']['symbols_table']
    # Get connection from Alembic
    conn = op.get_bind()
    drop_table = sa.text(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(drop_table)
    fundamentals_meta_table = mallard_config['tiingo']['fundamentals_meta_table']
    drop_table = sa.text(f"DROP TABLE IF EXISTS {fundamentals_meta_table}")
    conn.execute(drop_table)

"""add fundamentals last updated table

Revision ID: 188a19e5f040
Revises: a219dc95c8bb
Create Date: 2024-08-03 11:33:31.479969

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl


# revision identifiers, used by Alembic.
revision: str = '188a19e5f040'
down_revision: Union[str, None] = 'a219dc95c8bb'
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
fundamentals_last_updated_table = mallard_config['tiingo']['fundamentals_last_updated_table']
fundamentals_meta_table = mallard_config['tiingo']['fundamentals_meta_table']


def upgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Create fundamentals_last_updated table with vendor_symbol_id and statement_last_updated columns
    print(f"Adding table {fundamentals_last_updated_table}")
    query = sa.text(f"""
    CREATE TABLE {fundamentals_last_updated_table} (
        vendor_symbol_id VARCHAR PRIMARY KEY,
        amended_last_updated TIMESTAMP,
        reported_last_updated TIMESTAMP
    )
    """)
    conn.execute(query)

def downgrade() -> None:
    # Get connection from Alembic
    conn = op.get_bind()
    # Drop last_updated column from fundamentals reported table
    query = sa.text(f"""
    DROP TABLE IF EXISTS {fundamentals_last_updated_table}
    """)
    conn.execute(query)

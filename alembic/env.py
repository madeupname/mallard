from logging.config import fileConfig
from AlembicDuckDBImpl import AlembicDuckDBImpl

from sqlalchemy import create_engine

from alembic import context


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

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
if not db_file:
    raise ValueError("db_file not set in config file.")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = f"duckdb://{db_file}"
    print(f"INFO: DuckDB URL: {url}")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    print("RUNNING ONLINE MIGRATIONS")
    connectable = create_engine(f"duckdb:///{db_file}")

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

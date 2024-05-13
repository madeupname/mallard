from alembic.ddl.impl import DefaultImpl

class AlembicDuckDBImpl(DefaultImpl):
    """Alembic implementation for DuckDB."""

    __dialect__ = "duckdb"
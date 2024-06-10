"""Migrate fundamentals tables from long to wide

Revision ID: a219dc95c8bb
Revises: 517fab115260
Create Date: 2024-05-30 18:16:07.371927

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from AlembicDuckDBImpl import AlembicDuckDBImpl

# revision identifiers, used by Alembic.
revision: str = 'a219dc95c8bb'
down_revision: Union[str, None] = '517fab115260'
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


def wide_from_long_query(table, metric_column):
    """Creates a query to convert a long fundamentals table into a wide one. I have intentionally commented out fields
    that are missing from the data, but listed in the definitions endpoint as being present."""
    return sa.text(f"""
    CREATE TABLE {table + '_wide'} AS
    SELECT vendor_symbol_id, symbol, date, year, quarter,
        MAX(CASE WHEN {metric_column} = 'accoci' THEN value ELSE NULL END) AS accoci,
        MAX(CASE WHEN {metric_column} = 'acctPay' THEN value ELSE NULL END) AS acctPay,
        MAX(CASE WHEN {metric_column} = 'acctRec' THEN value ELSE NULL END) AS acctRec,
    --     MAX(CASE WHEN {metric_column} = 'assetTurnover' THEN value ELSE NULL END) AS assetTurnover,
        MAX(CASE WHEN {metric_column} = 'assetsCurrent' THEN value ELSE NULL END) AS assetsCurrent,
        MAX(CASE WHEN {metric_column} = 'assetsNonCurrent' THEN value ELSE NULL END) AS assetsNonCurrent,
        MAX(CASE WHEN {metric_column} = 'bookVal' THEN value ELSE NULL END) AS bookVal,
        MAX(CASE WHEN {metric_column} = 'businessAcqDisposals' THEN value ELSE NULL END) AS businessAcqDisposals,
        MAX(CASE WHEN {metric_column} = 'bvps' THEN value ELSE NULL END) AS bvps,
        MAX(CASE WHEN {metric_column} = 'capex' THEN value ELSE NULL END) AS capex,
        MAX(CASE WHEN {metric_column} = 'cashAndEq' THEN value ELSE NULL END) AS cashAndEq,
        MAX(CASE WHEN {metric_column} = 'consolidatedIncome' THEN value ELSE NULL END) AS consolidatedIncome,
        MAX(CASE WHEN {metric_column} = 'costRev' THEN value ELSE NULL END) AS costRev,
        MAX(CASE WHEN {metric_column} = 'currentRatio' THEN value ELSE NULL END) AS currentRatio,
        MAX(CASE WHEN {metric_column} = 'debt' THEN value ELSE NULL END) AS debt,
        MAX(CASE WHEN {metric_column} = 'debtCurrent' THEN value ELSE NULL END) AS debtCurrent,
        MAX(CASE WHEN {metric_column} = 'debtEquity' THEN value ELSE NULL END) AS debtEquity,
        MAX(CASE WHEN {metric_column} = 'debtNonCurrent' THEN value ELSE NULL END) AS debtNonCurrent,
        MAX(CASE WHEN {metric_column} = 'deferredRev' THEN value ELSE NULL END) AS deferredRev,
        MAX(CASE WHEN {metric_column} = 'depamor' THEN value ELSE NULL END) AS depamor,
        MAX(CASE WHEN {metric_column} = 'deposits' THEN value ELSE NULL END) AS deposits,
        MAX(CASE WHEN {metric_column} = 'ebit' THEN value ELSE NULL END) AS ebit,
        MAX(CASE WHEN {metric_column} = 'ebitda' THEN value ELSE NULL END) AS ebitda,
        MAX(CASE WHEN {metric_column} = 'ebt' THEN value ELSE NULL END) AS ebt,
    --     MAX(CASE WHEN {metric_column} = 'enterpriseVal' THEN value ELSE NULL END) AS enterpriseVal,
        MAX(CASE WHEN {metric_column} = 'eps' THEN value ELSE NULL END) AS eps,
        MAX(CASE WHEN {metric_column} = 'epsDil' THEN value ELSE NULL END) AS epsDil,
        MAX(CASE WHEN {metric_column} = 'epsQoQ' THEN value ELSE NULL END) AS epsQoQ,
        MAX(CASE WHEN {metric_column} = 'equity' THEN value ELSE NULL END) AS equity,
        MAX(CASE WHEN {metric_column} = 'freeCashFlow' THEN value ELSE NULL END) AS freeCashFlow,
        MAX(CASE WHEN {metric_column} = 'fxRate' THEN value ELSE NULL END) AS fxRate,
        MAX(CASE WHEN {metric_column} = 'grossMargin' THEN value ELSE NULL END) AS grossMargin,
        MAX(CASE WHEN {metric_column} = 'grossProfit' THEN value ELSE NULL END) AS grossProfit,
        MAX(CASE WHEN {metric_column} = 'intangibles' THEN value ELSE NULL END) AS intangibles,
        MAX(CASE WHEN {metric_column} = 'intexp' THEN value ELSE NULL END) AS intexp,
        MAX(CASE WHEN {metric_column} = 'inventory' THEN value ELSE NULL END) AS inventory,
        MAX(CASE WHEN {metric_column} = 'investments' THEN value ELSE NULL END) AS investments,
        MAX(CASE WHEN {metric_column} = 'investmentsAcqDisposals' THEN value ELSE NULL END) AS investmentsAcqDisposals,
        MAX(CASE WHEN {metric_column} = 'investmentsCurrent' THEN value ELSE NULL END) AS investmentsCurrent,
        MAX(CASE WHEN {metric_column} = 'investmentsNonCurrent' THEN value ELSE NULL END) AS investmentsNonCurrent,
        MAX(CASE WHEN {metric_column} = 'issrepayDebt' THEN value ELSE NULL END) AS issrepayDebt,
        MAX(CASE WHEN {metric_column} = 'issrepayEquity' THEN value ELSE NULL END) AS issrepayEquity,
        MAX(CASE WHEN {metric_column} = 'liabilitiesCurrent' THEN value ELSE NULL END) AS liabilitiesCurrent,
        MAX(CASE WHEN {metric_column} = 'liabilitiesNonCurrent' THEN value ELSE NULL END) AS liabilitiesNonCurrent,
        MAX(CASE WHEN {metric_column} = 'longTermDebtEquity' THEN value ELSE NULL END) AS longTermDebtEquity,
    --     MAX(CASE WHEN {metric_column} = 'marketCap' THEN value ELSE NULL END) AS marketCap,
        MAX(CASE WHEN {metric_column} = 'ncf' THEN value ELSE NULL END) AS ncf,
        MAX(CASE WHEN {metric_column} = 'ncff' THEN value ELSE NULL END) AS ncff,
        MAX(CASE WHEN {metric_column} = 'ncfi' THEN value ELSE NULL END) AS ncfi,
        MAX(CASE WHEN {metric_column} = 'ncfo' THEN value ELSE NULL END) AS ncfo,
        MAX(CASE WHEN {metric_column} = 'ncfx' THEN value ELSE NULL END) AS ncfx,
        MAX(CASE WHEN {metric_column} = 'netIncComStock' THEN value ELSE NULL END) AS netIncComStock,
        MAX(CASE WHEN {metric_column} = 'netIncDiscOps' THEN value ELSE NULL END) AS netIncDiscOps,
    --     MAX(CASE WHEN {metric_column} = 'netMargin' THEN value ELSE NULL END) AS netMargin,
        MAX(CASE WHEN {metric_column} = 'netinc' THEN value ELSE NULL END) AS netinc,
        MAX(CASE WHEN {metric_column} = 'nonControllingInterests' THEN value ELSE NULL END) AS nonControllingInterests,
    --     MAX(CASE WHEN {metric_column} = 'opMargin' THEN value ELSE NULL END) AS opMargin,
        MAX(CASE WHEN {metric_column} = 'opex' THEN value ELSE NULL END) AS opex,
        MAX(CASE WHEN {metric_column} = 'opinc' THEN value ELSE NULL END) AS opinc,
        MAX(CASE WHEN {metric_column} = 'payDiv' THEN value ELSE NULL END) AS payDiv,
    --     MAX(CASE WHEN {metric_column} = 'pbRatio' THEN value ELSE NULL END) AS pbRatio,
    --     MAX(CASE WHEN {metric_column} = 'peRatio' THEN value ELSE NULL END) AS peRatio,
        MAX(CASE WHEN {metric_column} = 'piotroskiFScore' THEN value ELSE NULL END) AS piotroskiFScore,
        MAX(CASE WHEN {metric_column} = 'ppeq' THEN value ELSE NULL END) AS ppeq,
        MAX(CASE WHEN {metric_column} = 'prefDVDs' THEN value ELSE NULL END) AS prefDVDs,
        MAX(CASE WHEN {metric_column} = 'profitMargin' THEN value ELSE NULL END) AS profitMargin,
        MAX(CASE WHEN {metric_column} = 'retainedEarnings' THEN value ELSE NULL END) AS retainedEarnings,
        MAX(CASE WHEN {metric_column} = 'revenue' THEN value ELSE NULL END) AS revenue,
        MAX(CASE WHEN {metric_column} = 'revenueQoQ' THEN value ELSE NULL END) AS revenueQoQ,
        MAX(CASE WHEN {metric_column} = 'rnd' THEN value ELSE NULL END) AS rnd,
        MAX(CASE WHEN {metric_column} = 'roa' THEN value ELSE NULL END) AS roa,
        MAX(CASE WHEN {metric_column} = 'roe' THEN value ELSE NULL END) AS roe,
        MAX(CASE WHEN {metric_column} = 'rps' THEN value ELSE NULL END) AS rps,
        MAX(CASE WHEN {metric_column} = 'sbcomp' THEN value ELSE NULL END) AS sbcomp,
        MAX(CASE WHEN {metric_column} = 'sga' THEN value ELSE NULL END) AS sga,
        MAX(CASE WHEN {metric_column} = 'shareFactor' THEN value ELSE NULL END) AS shareFactor,
        MAX(CASE WHEN {metric_column} = 'sharesBasic' THEN value ELSE NULL END) AS sharesBasic,
        MAX(CASE WHEN {metric_column} = 'shareswa' THEN value ELSE NULL END) AS shareswa,
        MAX(CASE WHEN {metric_column} = 'shareswaDil' THEN value ELSE NULL END) AS shareswaDil,
        MAX(CASE WHEN {metric_column} = 'taxAssets' THEN value ELSE NULL END) AS taxAssets,
        MAX(CASE WHEN {metric_column} = 'taxExp' THEN value ELSE NULL END) AS taxExp,
        MAX(CASE WHEN {metric_column} = 'taxLiabilities' THEN value ELSE NULL END) AS taxLiabilities,
        MAX(CASE WHEN {metric_column} = 'totalAssets' THEN value ELSE NULL END) AS totalAssets,
        MAX(CASE WHEN {metric_column} = 'totalLiabilities' THEN value ELSE NULL END) AS totalLiabilities,
    --     MAX(CASE WHEN {metric_column} = 'trailingPEG1Y' THEN value ELSE NULL END) AS trailingPEG1Y
    FROM {table}
    GROUP BY vendor_symbol_id, symbol, date, year, quarter;""")


def upgrade() -> None:
    fundamentals_amended_table = mallard_config['tiingo']['fundamentals_amended_table']
    fundamentals_reported_table = mallard_config['tiingo']['fundamentals_reported_table']
    fundamental_metrics_table = mallard_config['tiingo']['fundamental_metrics_table']
    # Get connection from Alembic
    conn = op.get_bind()
    # for table in [fundamentals_amended_table, fundamentals_reported_table, fundamental_metrics_table]:
    msg = f"Converting {fundamentals_amended_table} from long to wide schema."
    print(msg)
    query = wide_from_long_query(fundamentals_amended_table, 'dataCode')
    print(query)
    conn.execute(query)
    query = sa.text(f"DROP TABLE {fundamentals_amended_table}")
    conn.execute(query)
    query = sa.text(f"ALTER TABLE {fundamentals_amended_table + '_wide'} RENAME TO {fundamentals_amended_table}")
    conn.execute(query)
    msg = f"Conversion complete for {fundamentals_amended_table}."
    print(msg)

    msg = f"Converting {fundamentals_reported_table} from long to wide schema."
    print(msg)
    query = wide_from_long_query(fundamentals_reported_table, 'dataCode')
    print(query)
    conn.execute(query)
    query = sa.text(f"DROP TABLE {fundamentals_reported_table}")
    conn.execute(query)
    query = sa.text(f"ALTER TABLE {fundamentals_reported_table + '_wide'} RENAME TO {fundamentals_reported_table}")
    conn.execute(query)
    msg = f"Conversion complete for {fundamentals_reported_table}."
    print(msg)

    msg = f"Converting {fundamental_metrics_table} from long to wide schema."
    print(msg)
    query = wide_from_long_query(fundamental_metrics_table, 'metric')
    conn.execute(query)
    query = sa.text(f"DROP TABLE {fundamental_metrics_table}")
    conn.execute(query)
    query = sa.text(f"ALTER TABLE {fundamental_metrics_table + '_wide'} RENAME TO {fundamental_metrics_table}")
    conn.execute(query)
    msg = f"Conversion complete for {fundamental_metrics_table}."
    print(msg)


def downgrade() -> None:
    pass

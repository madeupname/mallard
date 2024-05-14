# mallard
Mallard is a personal project to use [DuckDB](https://duckdb.org) as an OLAP database/warehouse for securities data,
such as historical prices and fundamentals. By default, it creates a database where:
* all symbols have fundamental data directly tied to price data
* you get both reported and amended financials, so you can use this both for backtesting and stock screening/modeling
* includes vendor-provided metrics

At this point, it's just a collection of scripts to download data to the DB and generate metrics.

The first data provider is Tiingo because they are the most respected affordable data vendor.

Long term goals:
* Generate a large set of metrics/indicators. 
* Turn that into the world's greatest stock screener for anyone who knows SQL. 
* Easily generate datasets to use in backtesting (e.g., [Lumibot](https://github.com/Lumiwealth/lumibot/)) and ML.

Disclaimer: I am not associated with any vendor or project used by Mallard.

## Installation and Configuration
Decide where your data directory will be. It will hold all files from data providers and the database, so expect it to
be ~6GB assuming you're getting the full dataset of price and fundamentals. That's with the quality filters enabled.
Mine with various metrics generated is currently 10GB.

Copy `example_config.ini` to your data directory and edit it as needed. Must add your Tiingo API token.

Set the `MALLARD_CONFIG` environment variable to the full path of your `config.ini` file location.

**Windows users:** remember that Windows is incapable of having two processes open the same file so this will fail if
you have any data files open in Excel, your IDE, etc. This includes a console connection to the database file itself.

As the scripts are in the library, you currently have to run this using these commands **in this order**:

```bash
# create DB tables
alembic upgrade head
# load supported tickers
python -m mallard.tiingo.supported_tickers
# load fundamental data (requires addon)
python -m mallard.tiingo.fundamentals
# load Tiingo's daily metrics
python -m mallard.tiingo.fundamentals_daily
# load end of day data
python -m mallard.tiingo.eod
# build metrics 
python -m mallard.metrics.create_metrics
```
This gets all data you're eligible for. Since it runs multithreaded, it will hit the request per second cap from
Tiingo unless you're on a very slow connection. It uses a rate limiter to prevent rejections, but overall it will take
4-5 hours to get all data assuming you have the largest fundamentals addon.
## Metrics
Tiingo provides [a few metrics](https://www.tiingo.com/documentation/fundamentals), which are stored in the 
tiingo_fundamentals_daily table. 

Here are the metrics Mallard provides, all of which are configurable. This will continually be added to and PRs are 
welcome:

* Average Daily Trading Value (adtval)
  * This is the average volume * price over the last 5 days.
  * While average daily trading **volume** is the more common metric, traders typically require a minimum **value** before investing as a proxy for liquidity, assuming you don't have historical bid/ask. 
  * The current recommendation is $20M per day, so we also create an inflation rate table to adjust this for previous years. The rates are from the US government (FRED). 
* MACD
  * Standard 12, 26, 9 period.

## Data Management
If you keep all the default filters for symbols, you still get a large amount of data:

* 32M rows end of day price data, growing at over 6,500 rows per day
* 58M rows of fundamentals data, growing at 520K rows per quarter
* Double the fundamental data because you get reported and amended.

Because the value of many fields like adjusted prices can be very high or very low, we store them as double, which is
half the decimal equivalent but still 8 bytes per field. So if you want to calculate metrics over the entire price
history, you're adding 256MB per column. Considering the number of even basic daily metrics like P/E ratio, that adds up
fast.

The good news is that, as an OLAP database, this is precisely what DuckDB is for and you have the option of not storing
more metrics and instead calculating them on the fly via queries. Copilot, GPT 4 and Phind are capable of 
generating complex queries for you.

## Design Decisions and Principles
Whenever possible, use DuckDB. It's the fastest open source tool for analytics. It has columnar storage, works in memory
but can process larger than memory data sets. [Polars](https://docs.pola.rs/) is used if a dataframe is required, like
parsing CSV from a string. (Currently, DuckDB can only read CSV from files.) We incorporate TA lib, but most financial 
calculations are trivial to do in SQL and will be faster.

Use SQLAlchemy Alembic for data migrations. No other use of SQLAlchemy is planned at this time because the vast 
majority of finance data is tabular and DuckDB should do all the heavy lifting.

To maintain data provenance, tables with data from a specific provider will be prefixed by the provider's name, hence we
have a `tiingo_symbols` table and not a `symbols` table.

However, we follow OpenBB and Finance Toolkit's lead of minimally normalizing things like column names. So even though
it's a Tiingo table, we call it symbol and not ticker. We also use snake case instead of camel case. `normalization.py`
holds a simple translation dictionary. This follows the principle of Ubiquitous Language.

## Tiingo
Note: this code assumes you have a paid subscription to Tiingo as that is required for the whole-market dataset that
Mallard aims to create. It's very affordable. 

This project works under the assumption that fundamental data is used in yours strategy. It will work without the Tiingo
fundamental data addon subscription, but it starts with the fundamentals meta data as that ensures your universe has
stocks that are reporting their financials to the SEC and hence are tradeable on American exchanges. For Tiingo, the
benefit is this endpoint gives their "permaTicker" (which Mallard calls vendor_symbol_id), an internal vendor ID for the
equity. That is also used to get prices so there is a clear connection between fundamental and price data.

Tiingo has a supported tickers file, but many tickers in that file have no data in the historical price endpoint and
fewer still in the fundamentals endpoint. It also has no primary key. Therefore, I've created a ticker_requirements
config variable to filter out rows without the following to avoid pointless API calls to the search endpoint. These are
all optional. Keep in mind if you use supported tickers as your source of truth, you'll have duplicate tickers and 
you'll have to determine active symbols by date. Not recommended.

* exchange
  * I found no instance of data from stocks with empty exchange values. 
  * Docs state that if a ticker is reused, there be no historical data for the delisted stock(s). I've observed that those recycled tickers have empty exchange values. 
  * There are many non-recycled tickers with no exchange listed, but spot checks haven't found data. Please report any exceptions.
* stock
  * assetType == 'Stock'
  * excludes ETF, Mutual Fund types, leaving only symbols that could match fundamental data
* usd
  * priceCurrency == 'USD'
  * excludes Chinese, Hong Kong, and Australian currencies
* dates
  * excludes rows where start or end date is NULL (although I found no instance where only one is non-null)
* 6days
  * requires the symbol to have lived for at least 6 days
  * as I filter based on average daily trading value, which requires at least 5 days of price history (like ADTV), this does not introduce lookahead bias the way a longer existence requirement might
* nocolon
  * Tiingo symbols with a colon refer to foreign exchanges, like :MU is Munich. So this is filtering out those exchanges.
* fundamentals
  * downloads the fundamentals meta file and removes all symbols that are not there
  * this ensures all symbols have some fundamental data, which should eliminate all stocks not registered with the SEC
  * that should eliminate all stocks not traded on US exchanges and those failing to report financials

After that, I order by end_date and select distinct symbols, so I'm left with symbol as a pseudo primary key. 

Another issue is that Tiingo returns no data or incorrectly formatted data for some symbols during the supported date
range. In some cases, Tiingo appears to mark some symbols as active when they are not (spot checks have found
articles announcing their delisting from their exchange). In others, they may be active on an exchange with minimal
listing requirements and genuinely have no trades. I'm focused on American symbols with SEC records, but if you're
including Chinese stocks you'll find their circuit breaker (halting trading) has a hair trigger compared to American
exchanges.

## Lessons Learned
### DuckDB
**DuckDB requires flawless connection management!** It allows either a single process to access a file with read/write
permissions, OR multiple processes have read-only permissions. Not both. So if one process is writing to a file, no
other process can read from it. However, you can have a **multithreaded** process where all the threads read and write.
This is what Mallard does when updating your data.

To aid in this, consider `with` blocks.

DuckDB:
```
with duckdb.connect(db_file) as connection:
  connection.execute(SELECT COUNT(*) FROM tiingo_symbols).fetchall()
```
SQLAlchemy:
```
with engine.connect() as connection:
  connection.execute(SELECT COUNT(*) FROM tiingo_symbols).fetchall()
```
These will automatically close the connection for you, although there is an issue with closing the connection if you use 
fetchone() and there are remaining results. Without with blocks or explicit closes, you're at the mercy of Python 
garbage collection to close connections. This is especially frustrating when using notebooks.

When creating a datasource in your IDE, consider these settings.
* read-only
* single-session
* disconnect after 5 seconds
* In Jupyter, consider:
  * wrapping all blocks in a new connection that is closed
  * restarting the kernel if closing the connection isn't releasing the file lock. It's possible rerunning a block has created a new connection without closing the previous one.

One of the really cool features of an in-process DB is that you can use it with the greatest of ease. Just type
something like `duckdb.sql("SELECT * FROM sec_symbols")` and it's off to the races! In milliseconds, it will inform you
that there is no sec_symbols table, despite you having just created it, and _yes_, in the right database file. But it's
right and you're wrong because you used `duckdb.sql`, not `con.sql` (where con is the connection you created in the with
block), so it defaulted to the in-memory database where you indeed have created no tables. The best remedy to this
problem is a good night's sleep.

DuckDB can be used with JetBrains Ultimate products like IntelliJ and Pycharm. You have to create a DuckDB datasource.

https://youtrack.jetbrains.com/issue/DBE-19335/Cant-attach-Duckdb-session-to-created-sql-file

You can use it with a .sql file by choosing "Generic SQL" as the dialect. 

https://www.jetbrains.com/help/idea/2023.3/other-databases.html

### Excel
We create files like NVDA_daily.csv, even though it's in a daily directory, in part because Excel can't open two files 
with the same name. (More importantly, it allows differentiation in the quarantine directory.)

Excel will reformat dates from programming-safe formats like YYYY-MM-DD to American M/D/YYYY, which is fine for viewing,
but if you save the file, it will change the format. Then on EOD file update, Mallard will append a different date
format and break the file. Excel will show it in one format and you will lose hours trying to figure out what went
wrong. The moral of the story is to copy files to another directory before opening with Excel. Treat the Mallard-created
files as system files you don't edit.


### SEC
I briefly looked into downloading their company symbol and exchanges files but they state:
* they update this "periodically" but don't say when
* don't define the scope
* make no guarantees as to accuracy

I sincerely appreciate this honesty, but it makes those files worthless. It makes more sense to use Tiingo's
fundamentals metadata instead, since that's what you're downloading anyway.

I plan to get a subscription to sec-api.io, but for shares outstanding, insider trades, etc.  

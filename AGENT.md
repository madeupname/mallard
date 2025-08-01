# Project: Mallard
If you haven't already, read @../AGENT.md for instructions that apply to this project. 

This is code for a data warehouse for financial data, currently for stocks/ETFs. You must read @README.md for more critical information. 

@update_mallard_1.bat is the starting point as it executes all scripts to update the DB with the latest financial and EOD data.

@/data/mallard/config.ini has current production config.

Tiingo is our data provider for financial data (via Sharadar) and EOD data.

We get minute bars via Alpaca. We currently have the Algo Trader Plus subscription.

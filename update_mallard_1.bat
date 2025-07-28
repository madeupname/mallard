@echo off
cd /d C:\Java\Source\Idea\mallard
call C:\Java\Source\Idea\Jupyter\venv\Scripts\activate.bat
python -m mallard.tiingo.supported_tickers
python -m mallard.tiingo.fundamentals
python -m mallard.tiingo.fundamentals_daily

set /a mins=%time:~3,2%
set /a wait_mins=60-mins
set /a wait_secs=wait_mins*60
timeout /t %wait_secs% /nobreak

python -m mallard.tiingo.eod
python -m mallard.metrics.create_metrics
python -m mallard.tiingo.splits

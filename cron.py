"""Cron driver for the Railway worker process.

Schedule:
  :01 hourly   weather_api_export.py   — fetch all source forecasts + obs
  :05 hourly   predict_ensemble.py     — write predictions to ensemble_forecast
  03:01 daily  train_ensemble.py       — refit quantile models, write models.pkl

Boot also triggers a one-off retrain if models.pkl is missing or older than 26h
(so a mid-day Railway restart doesn't skip the day's training).
"""

import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import schedule

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

MODELS_PATH = Path('models.pkl')
STALE_AFTER = timedelta(hours=26)


def run(script, timeout):
    logging.info(f'--- Starting {script} ---')
    try:
        result = subprocess.run(
            ['python', script],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.stdout:
            logging.info(f'{script} stdout: {result.stdout.strip()}')
        if result.returncode == 0:
            logging.info(f'Success: {script} finished.')
        else:
            logging.error(f'{script} exited {result.returncode}: {result.stderr}')
    except subprocess.TimeoutExpired:
        logging.error(f'Failure: {script} exceeded timeout ({timeout}s) and was killed.')
    except Exception as e:
        logging.error(f'Unexpected error running {script}: {e}')
    logging.info(f'--- Finished {script} ---')


def hourly_export():
    run('weather_api_export.py', timeout=300)


def hourly_predict():
    if not MODELS_PATH.exists():
        logging.warning('predict_ensemble skipped: models.pkl not present yet')
        return
    run('predict_ensemble.py', timeout=180)


def daily_train():
    run('train_ensemble.py', timeout=1800)


def models_are_stale():
    if not MODELS_PATH.exists():
        return True
    mtime = datetime.fromtimestamp(MODELS_PATH.stat().st_mtime)
    return datetime.now() - mtime > STALE_AFTER


# Boot-time catch-up: if we have no models or stale models, train before the
# first hourly cycle so predictions start as soon as the next :05 fires.
if models_are_stale():
    logging.info('models.pkl missing or stale on boot — running train_ensemble.py')
    daily_train()


schedule.every().hour.at(':01').do(hourly_export)
schedule.every().hour.at(':05').do(hourly_predict)
schedule.every().day.at('03:01').do(daily_train)

logging.info('Cron scheduler started. Hourly :01 export, :05 predict, 03:01 daily train.')

while True:
    schedule.run_pending()
    time.sleep(1)

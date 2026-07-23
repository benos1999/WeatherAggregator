"""Materialise the model-quality metrics into Postgres so Power BI (which reads
Postgres directly) can consume them alongside the four raw tables.

Replaces the Google Sheets hop: the same builders in tableau_export.py that used
to feed the Tableau Sheet tabs are written here as regular tables.

Tables written (each fully replaced per run):
    efficacy          — one row per past forecast joined to obs, signed `error`
    metrics_daily     — per (date, tier, measure, source): n, mae, bias
    reliability       — RainProbability calibration bins
    horizon_accuracy  — per (tier, city, source, measure, lead_bucket): n, mae, bias

An empty build is skipped (the existing table is left intact) so a broken
upstream — predict didn't run, a query returned nothing — can't wipe last-good
metrics out from under Power BI.

Usage:
    python metrics_to_postgres.py            # build + write
    python metrics_to_postgres.py --dry-run  # build only, print row counts
"""

from __future__ import annotations

import argparse
import logging as log
import os
import sys
import time

from dotenv import load_dotenv
from sqlalchemy import create_engine

import tableau_export


def _write(engine, df, table):
    """Replace `table` with `df`. Skip (keep last-good) when the build is empty."""
    if df is None or df.empty:
        log.warning(f'skipping "{table}": new data is empty — keeping existing table')
        return 0
    df.to_sql(table, engine, if_exists='replace', index=False)
    log.info(f'wrote {len(df):,} rows to "{table}"')
    return len(df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='build but do not write')
    args = parser.parse_args()

    load_dotenv()
    log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    engine = create_engine(os.environ['DATABASE_URL'])
    t0 = time.time()

    log.info('Building efficacy + metrics...')
    parts = tableau_export.build_parts(engine)
    efficacy_df = parts['efficacy']
    metrics_df = tableau_export.build_metrics_daily(efficacy_df)
    reliability_df = tableau_export.build_reliability(efficacy_df)
    horizon_df = tableau_export.build_horizon_accuracy(engine)
    log.info(f'  efficacy: {len(efficacy_df):,}  metrics_daily: {len(metrics_df):,}  '
             f'reliability: {len(reliability_df):,}  horizon_accuracy: {len(horizon_df):,}  '
             f'({time.time()-t0:.1f}s)')

    if args.dry_run:
        log.info('dry-run: not writing to Postgres')
        return

    _write(engine, efficacy_df,    'efficacy')
    _write(engine, metrics_df,     'metrics_daily')
    _write(engine, reliability_df, 'reliability')
    _write(engine, horizon_df,     'horizon_accuracy')
    log.info(f'metrics materialised in {time.time()-t0:.1f}s')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.error(f'metrics_to_postgres failed: {e}')
        sys.exit(1)

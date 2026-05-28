"""Push the denormalised long-format DataFrame to a Google Sheet, where
Tableau Public's daily auto-refresh will pick it up.

Auth uses OAuth2 *user credentials* (not a service account — Google org
policies often disable service-account key creation). The one-time browser
flow is in oauth_setup.py; it mints a refresh token that this script swaps
for short-lived access tokens at run time.

Required env vars:
    GOOGLE_OAUTH_CLIENT_ID       — from the Desktop OAuth client in Google Cloud
    GOOGLE_OAUTH_CLIENT_SECRET   — same
    GOOGLE_OAUTH_REFRESH_TOKEN   — produced by oauth_setup.py
    TABLEAU_SHEET_ID             — the long key in the sheet URL
    TABLEAU_SHEET_TAB            — worksheet/tab name (default 'ensemble')

Usage:
    python sheets_export.py          # build and push using current DB state
    python sheets_export.py --dry-run # build only, print row count, no push
"""

from __future__ import annotations

import argparse
import logging as log
import os
import time

import gspread
import pandas as pd
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from sqlalchemy import create_engine

import tableau_export

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
TOKEN_URI = 'https://oauth2.googleapis.com/token'
DEFAULT_TAB = 'ensemble'


def _authorize() -> gspread.Client:
    """Build a gspread client from the OAuth user-credentials env vars."""
    try:
        client_id = os.environ['GOOGLE_OAUTH_CLIENT_ID']
        client_secret = os.environ['GOOGLE_OAUTH_CLIENT_SECRET']
        refresh_token = os.environ['GOOGLE_OAUTH_REFRESH_TOKEN']
    except KeyError as e:
        raise RuntimeError(f'OAuth env var {e.args[0]} not set — run oauth_setup.py first') from None
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri=TOKEN_URI,
        scopes=SCOPES,
    )
    creds.refresh(Request())  # mint an access token now, fail fast if creds are bad
    return gspread.authorize(creds)


def _df_to_values(df: pd.DataFrame) -> list[list]:
    """Convert DataFrame to a 2D list suitable for gspread, stringifying timestamps
    so the Sheet doesn't apply locale-dependent auto-parsing."""
    out = df.copy()
    for col in ('target_datetime', 'forecast_taken'):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col]).dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    if 'date' in out.columns:
        out['date'] = pd.to_datetime(out['date']).dt.strftime('%Y-%m-%d').fillna('')
    if 'time' in out.columns:
        out['time'] = out['time'].astype('object').where(out['time'].notna(), '')
        out['time'] = out['time'].astype(str).str.replace('NaT', '', regex=False)
    out = out.fillna('')
    return [out.columns.tolist()] + out.values.tolist()


def push_dataframe(df: pd.DataFrame, sheet_id: str, tab: str = DEFAULT_TAB) -> None:
    """Clear the target tab and write the DataFrame to it. One API write call."""
    gc = _authorize()
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(tab, rows=max(len(df) + 10, 100), cols=len(df.columns) + 1)
        log.info(f'created worksheet "{tab}"')
    values = _df_to_values(df)
    # Grow the sheet if needed so the values fit
    needed_rows = len(values)
    needed_cols = len(values[0]) if values else 1
    if ws.row_count < needed_rows:
        ws.add_rows(needed_rows - ws.row_count)
    if ws.col_count < needed_cols:
        ws.add_cols(needed_cols - ws.col_count)
    ws.clear()
    ws.update(values=values, range_name='A1', value_input_option='RAW')
    log.info(f'wrote {len(df):,} rows to sheet {sheet_id} / tab "{tab}"')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='build but do not push')
    args = parser.parse_args()

    load_dotenv()
    log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    engine = create_engine(os.environ['DATABASE_URL'])
    t0 = time.time()
    df = tableau_export.build_long_dataframe(engine)
    log.info(f'built long DataFrame: {len(df):,} rows in {time.time()-t0:.1f}s')

    if args.dry_run:
        log.info('dry-run: skipping sheet push')
        return

    sheet_id = os.environ.get('TABLEAU_SHEET_ID')
    if not sheet_id:
        raise RuntimeError('TABLEAU_SHEET_ID env var not set')
    tab = os.environ.get('TABLEAU_SHEET_TAB', DEFAULT_TAB)
    push_dataframe(df, sheet_id, tab)


if __name__ == '__main__':
    main()

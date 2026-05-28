"""One-time browser OAuth2 flow that mints a refresh token for the Google
Sheets push. Run this once locally; copy the printed refresh token into your
Railway env vars (and your local .env) as GOOGLE_OAUTH_REFRESH_TOKEN.

Prerequisite: a Desktop OAuth 2.0 client created in Google Cloud Console
under "APIs & Services > Credentials". Download its JSON ("OAuth client") to
this directory as oauth_client.json — that file is gitignored.

Usage:
    python oauth_setup.py
        opens a browser to grant Sheets access to your own Google account,
        then prints CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN to stdout.

The refresh token is long-lived: Google rotates it only if you revoke access
or it goes unused for >6 months. Treat it like a password.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
DEFAULT_CLIENT_FILE = Path('oauth_client.json')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-file', type=Path, default=DEFAULT_CLIENT_FILE,
                        help='Path to the downloaded OAuth Desktop client JSON')
    args = parser.parse_args()

    if not args.client_file.exists():
        sys.exit(
            f'{args.client_file} not found.\n'
            'Download the Desktop OAuth client JSON from Google Cloud Console '
            '("APIs & Services > Credentials") and save it as oauth_client.json.'
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(args.client_file), SCOPES)
    creds = flow.run_local_server(
        port=0,
        access_type='offline',
        prompt='consent',
    )

    if not creds.refresh_token:
        sys.exit(
            'Google did not return a refresh token. This usually means the '
            'consent screen was already approved. Revoke access at '
            'https://myaccount.google.com/permissions and retry, or pass '
            'prompt=consent (already set here).'
        )

    print()
    print('-' * 60)
    print('OAuth setup complete. Copy the three values below into your .env')
    print('and into Railway\'s environment variables:')
    print('-' * 60)
    print(f'GOOGLE_OAUTH_CLIENT_ID={creds.client_id}')
    print(f'GOOGLE_OAUTH_CLIENT_SECRET={creds.client_secret}')
    print(f'GOOGLE_OAUTH_REFRESH_TOKEN={creds.refresh_token}')
    print('-' * 60)


if __name__ == '__main__':
    main()

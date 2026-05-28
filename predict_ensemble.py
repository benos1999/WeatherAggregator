"""Hourly ensemble prediction.

Pulls the latest forecasts per (city, Model), builds the same feature matrices
as training, runs LightGBM quantile p10/p50/p90, computes per-source SHAP
contributions, and writes one row per (city, target_datetime, ForecastTaken,
tier, target) into the ensemble_forecast table.

Usage:  python predict_ensemble.py
"""

from __future__ import annotations

import logging as log
import os
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd
import shap
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import ensemble_lib as L

load_dotenv()
log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MODELS_PATH = Path('models.pkl')

SHAP_COLS = {
    'MetOffice':         'shap_MetOffice',
    'OpenMeteo-ECMWF':   'shap_OpenMeteo_ECMWF',
    'OpenMeteo-GFSHRRR': 'shap_OpenMeteo_GFSHRRR',
    'AccuWeather':       'shap_AccuWeather',
    'other':             'shap_other',
}


def build_prediction_matrices(engine):
    """Same structure as train_ensemble.build_training_matrices but using the
    forecast-only LIVE_* queries (no observation join). The lag and evolution
    features are added from full-history obs + evolution tables."""
    log.info('Pulling latest hourly forecasts...')
    raw_h = pd.read_sql(L.LIVE_HOURLY_SQL, engine, params={'mph_to_kmh': L.MPH_TO_KMH})
    log.info(f'  live hourly rows: {len(raw_h):,}')

    log.info('Pulling latest daily forecasts...')
    raw_d = pd.read_sql(L.LIVE_DAILY_SQL, engine, params={'mph_to_kmh': L.MPH_TO_KMH})
    log.info(f'  live daily rows: {len(raw_d):,}')

    obs_all = L.load_observations(engine)
    evo_h = L.fetch_evolution_hourly(engine)
    evo_d = L.fetch_evolution_daily(engine)

    # Live data has no observation join — pivot on forecast-only index cols.
    # (Using INDEX_COLS would drop every row because pivot_table drops NaN index keys.)

    # Hourly-Short (1-12h): require all 4 models present
    short = L.build_pivot(raw_h, models_to_pivot=L.MODELS,
                          require_models=L.MODELS, hours_filter=(1, 12),
                          index_cols=L.LIVE_INDEX_COLS_HOURLY,
                          value_cols=L.VALUE_COLS)
    short = L.add_lag_features(short, obs_all, hours_list=(1, 3, 6))
    short = L.add_time_features(short)
    short = L.add_winddir_features(short, L.MODELS)
    short = L.add_city_features(short)
    short = L.add_presence_indicators(short, L.MODELS)
    short = L.merge_evolution(short, evo_h)

    # Hourly-Long (13-48h): require MetOffice
    long_ = L.build_pivot(raw_h, models_to_pivot=L.MODELS_LONG,
                          require_models=['MetOffice'], hours_filter=(13, 48),
                          index_cols=L.LIVE_INDEX_COLS_HOURLY,
                          value_cols=L.VALUE_COLS)
    long_ = L.add_lag_features(long_, obs_all, hours_list=(1, 3, 6))
    long_ = L.add_time_features(long_)
    long_ = L.add_winddir_features(long_, L.MODELS_LONG)
    long_ = L.add_city_features(long_)
    long_ = L.add_presence_indicators(long_, L.MODELS_LONG)
    long_ = L.merge_evolution(long_, evo_h)

    # Daily
    daily = L.build_pivot(raw_d, models_to_pivot=L.MODELS,
                          require_models=['MetOffice'], hours_filter=None,
                          index_cols=L.LIVE_INDEX_COLS_DAILY,
                          value_cols=L.DAILY_VALUE_COLS)
    daily = L.add_time_features(daily, date_col='Date', time_col='__none__')
    daily = L.add_winddir_features(daily, L.MODELS)
    daily = L.add_city_features(daily)
    daily = L.add_presence_indicators(daily, L.MODELS, ref_col='fc_mintemp')
    daily = L.add_lag_features(daily, obs_all, hours_list=(24, 48, 72))
    daily = L.merge_evolution(daily, evo_d)

    log.info(f'pred matrices — short:{len(short)} long:{len(long_)} daily:{len(daily)}')
    return {'Hourly-Short': short, 'Hourly-Long': long_, 'Daily': daily}


def _align_features(df, feature_cols):
    """Return df subset to feature_cols, adding any missing columns as 0/NaN."""
    for c in feature_cols:
        if c not in df.columns:
            df[c] = np.nan
    return df[feature_cols]


def _aggregate_shap_row(shap_vals, feature_names, source_feature_map):
    """Return a dict {SHAP_COLS[source]: signed sum} per row. shap_vals shape (n,nf)."""
    rolled = L.aggregate_shap_by_source(shap_vals, feature_names, source_feature_map)
    return {SHAP_COLS[src]: rolled[src] for src in SHAP_COLS}


def predict_tier(tier, df, models_dict):
    """Run all (tier, target) heads for this tier, return one tall DataFrame of rows
    ready for INSERT into ensemble_forecast."""
    rows = []
    if len(df) == 0:
        return pd.DataFrame()

    if tier == 'Daily':
        id_cols = ['city', 'Date', 'ForecastTaken']
        df = df.copy()
        df['Time'] = None
    else:
        id_cols = ['city', 'Date', 'Time', 'ForecastTaken']

    for (t, target), payload in models_dict.items():
        if t != tier:
            continue
        feature_cols = payload['feature_cols']
        X = _align_features(df.copy(), feature_cols)
        transform = payload.get('transform')

        if transform == 'winddir':
            sin_p50 = payload['sin']['p50'].predict(X)
            cos_p50 = payload['cos']['p50'].predict(X)
            angle = np.rad2deg(np.arctan2(sin_p50, cos_p50)) % 360
            # No natural quantile band for circular; report p50 only
            p10 = angle
            p50 = angle
            p90 = angle
            # SHAP: explain the sin head as a representative direction signal
            explainer = shap.TreeExplainer(payload['sin']['p50'])
            shap_vals = explainer.shap_values(X)
        else:
            p10_raw = payload['p10'].predict(X)
            p50_raw = payload['p50'].predict(X)
            p90_raw = payload['p90'].predict(X)
            if transform == 'log1p':
                p10 = np.clip(np.expm1(p10_raw), 0, None)
                p50 = np.clip(np.expm1(p50_raw), 0, None)
                p90 = np.clip(np.expm1(p90_raw), 0, None)
            else:
                p10, p50, p90 = p10_raw, p50_raw, p90_raw
            if target == 'RainProbability':
                p10 = np.clip(p10, 0, 100)
                p50 = np.clip(p50, 0, 100)
                p90 = np.clip(p90, 0, 100)
            # Enforce monotonicity row-wise — independent quantile models can cross.
            stacked = np.sort(np.column_stack([p10, p50, p90]), axis=1)
            p10, p50, p90 = stacked[:, 0], stacked[:, 1], stacked[:, 2]
            explainer = shap.TreeExplainer(payload['p50'])
            shap_vals = explainer.shap_values(X)

        shap_per_source = L.aggregate_shap_by_source(
            shap_vals, feature_cols, payload['source_feature_map'])

        out = df[id_cols].copy()
        out['tier'] = tier
        out['target'] = target
        out['p10'] = p10
        out['p50'] = p50
        out['p90'] = p90
        for src, col in SHAP_COLS.items():
            out[col] = shap_per_source.get(src, np.zeros(len(out)))
        rows.append(out)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def write_predictions(engine, df):
    if df.empty:
        log.warning('No prediction rows to write.')
        return
    # Make sure 'Time' column exists (Daily has NaT)
    if 'Time' not in df.columns:
        df['Time'] = None
    cols_order = ['Date', 'Time', 'city', 'ForecastTaken', 'tier', 'target',
                  'p10', 'p50', 'p90',
                  'shap_MetOffice', 'shap_OpenMeteo_ECMWF',
                  'shap_OpenMeteo_GFSHRRR', 'shap_AccuWeather', 'shap_other']
    df = df[cols_order]
    df.to_sql('ensemble_forecast', engine, if_exists='append', index=False)
    log.info(f'Wrote {len(df):,} rows to ensemble_forecast')


def main():
    t0 = time.time()
    if not MODELS_PATH.exists():
        raise SystemExit(f'{MODELS_PATH} missing — run train_ensemble.py first.')
    with open(MODELS_PATH, 'rb') as f:
        models_dict = pickle.load(f)
    log.info(f'Loaded {len(models_dict)} (tier,target) heads from {MODELS_PATH}')

    engine = create_engine(os.environ['DATABASE_URL'])
    matrices = build_prediction_matrices(engine)

    all_rows = []
    for tier, df in matrices.items():
        log.info(f'Predicting {tier} ({len(df)} input rows)...')
        all_rows.append(predict_tier(tier, df, models_dict))

    combined = pd.concat([d for d in all_rows if not d.empty], ignore_index=True)
    log.info(f'Total prediction rows: {len(combined):,}')

    write_predictions(engine, combined)
    log.info(f'Done in {time.time() - t0:.1f}s')


if __name__ == '__main__':
    main()

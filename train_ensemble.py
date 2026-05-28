"""Daily ensemble training.

Builds the three training matrices (Hourly-Short, Hourly-Long, Daily) using
helpers from ensemble_lib, then trains three LightGBM quantile models
(alpha = 0.1, 0.5, 0.9) per (tier, target). WindDirection is trained as
separate sin/cos heads; RainVolume is trained on log1p(y).

Outputs:
  models.pkl      — pickled dict keyed by (tier, target)
  metrics.json    — per-(tier, target) MAE/coverage on the time-split holdout

Usage:  python train_ensemble.py
"""

from __future__ import annotations

import json
import logging as log
import os
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error
from sqlalchemy import create_engine

import ensemble_lib as L

load_dotenv()
log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MODELS_PATH = Path('models.pkl')
METRICS_PATH = Path('metrics.json')

LGBM_BASE_PARAMS = dict(
    n_estimators=500,
    learning_rate=0.05,
    num_leaves=63,
    n_jobs=-1,
    random_state=0,
    verbose=-1,
)


# ---------------------------------------------------------------------------
# Training-matrix construction
# ---------------------------------------------------------------------------

def build_training_matrices(engine):
    """Return {tier_name: training_df} for the three tiers."""
    log.info('Pulling raw hourly join...')
    raw_h = pd.read_sql(L.HOURLY_JOIN_SQL, engine, params={'mph_to_kmh': L.MPH_TO_KMH})
    log.info(f'  raw hourly rows: {len(raw_h):,}')

    log.info('Pulling raw daily join...')
    raw_d = pd.read_sql(L.DAILY_JOIN_SQL, engine, params={'mph_to_kmh': L.MPH_TO_KMH})
    log.info(f'  raw daily rows: {len(raw_d):,}')

    log.info('Loading observations for lag features...')
    obs_all = L.load_observations(engine)

    log.info('Fetching hourly forecast-evolution features...')
    evo_h = L.fetch_evolution_hourly(engine)
    log.info(f'  evolution-hourly rows: {len(evo_h):,}')

    log.info('Fetching daily forecast-evolution features...')
    evo_d = L.fetch_evolution_daily(engine)
    log.info(f'  evolution-daily rows: {len(evo_d):,}')

    # ---- Hourly-Short (all 4 models, 1-12h) ----
    short = L.build_pivot(raw_h, models_to_pivot=L.MODELS,
                          require_models=L.MODELS, hours_filter=(1, 12))
    short = L.add_lag_features(short, obs_all, hours_list=(1, 3, 6))
    short = L.add_time_features(short)
    short = L.add_winddir_features(short, L.MODELS)
    short = L.add_city_features(short)
    short = L.add_presence_indicators(short, L.MODELS)
    short = L.add_rain_targets(short)
    short = L.merge_evolution(short, evo_h)
    log.info(f'Hourly-Short matrix: {len(short):,} rows, {len(short.columns)} cols')

    # ---- Hourly-Long (3 models, 13-48h) ----
    long_ = L.build_pivot(raw_h, models_to_pivot=L.MODELS_LONG,
                          require_models=['MetOffice'], hours_filter=(13, 48))
    long_ = L.add_lag_features(long_, obs_all, hours_list=(1, 3, 6))
    long_ = L.add_time_features(long_)
    long_ = L.add_winddir_features(long_, L.MODELS_LONG)
    long_ = L.add_city_features(long_)
    long_ = L.add_presence_indicators(long_, L.MODELS_LONG)
    long_ = L.add_rain_targets(long_)
    long_ = L.merge_evolution(long_, evo_h)
    log.info(f'Hourly-Long matrix:  {len(long_):,} rows, {len(long_.columns)} cols')

    # ---- Daily (all 4 models, OpenMeteo-ECMWF required) ----
    # OpenMeteo-ECMWF always reaches 14 days; MetOffice caps at ~7d and
    # AccuWeather at 5d. Requiring MetOffice would cap training/prediction
    # at 7 days; requiring OM-ECMWF lets the model learn days 8-14 too,
    # with the MetOffice/AccuWeather columns handled as NaN by LightGBM.
    daily = L.build_pivot(raw_d, models_to_pivot=L.MODELS,
                          require_models=['OpenMeteo-ECMWF'], hours_filter=None,
                          index_cols=L.DAILY_INDEX_COLS, value_cols=L.DAILY_VALUE_COLS)
    daily = L.add_rain_targets(daily)
    daily = L.add_time_features(daily, date_col='Date', time_col='__none__')
    daily = L.add_winddir_features(daily, L.MODELS)
    daily = L.add_city_features(daily)
    # Presence indicators tell the model which sources are usable at each row
    # (e.g. MetOffice=0 implies a long-range prediction where the model must
    # lean on OpenMeteo only). Critical now that we span 1-14 day horizons.
    daily = L.add_presence_indicators(daily, L.MODELS, ref_col='fc_mintemp')
    daily = L.add_lag_features(daily, obs_all, hours_list=(24, 48, 72))
    daily = L.merge_evolution(daily, evo_d)
    log.info(f'Daily matrix:        {len(daily):,} rows, {len(daily.columns)} cols')

    return {'Hourly-Short': short, 'Hourly-Long': long_, 'Daily': daily}


# ---------------------------------------------------------------------------
# Per-(tier, target) training
# ---------------------------------------------------------------------------

# Per-tier target specification.
#   target_col: observation column
#   transform:  None | 'log1p' | 'winddir' (sin/cos pair)
#   source_fc_prefix: prefix used to build per-source baseline source feature names
TIER_TARGETS = {
    'Hourly-Short': [
        ('Temperature',     'obs_temperature',     None,      'fc_temperature',   L.MODELS),
        ('WindSpeed',       'obs_windspeed_kmh',   None,      'fc_windspeed_kmh', L.MODELS),
        ('WindDirection',   'obs_winddir',         'winddir', 'fc_winddir',       L.MODELS),
        ('RainProbability', 'obs_rainprob',        None,      'fc_rainprob',      L.MODELS),
        ('RainVolume',      'obs_rainvol',         'log1p',   'fc_rainvol',       L.MODELS),
    ],
    'Hourly-Long': [
        ('Temperature',     'obs_temperature',     None,      'fc_temperature',   L.MODELS_LONG),
        ('WindSpeed',       'obs_windspeed_kmh',   None,      'fc_windspeed_kmh', L.MODELS_LONG),
        ('WindDirection',   'obs_winddir',         'winddir', 'fc_winddir',       L.MODELS_LONG),
        ('RainProbability', 'obs_rainprob',        None,      'fc_rainprob',      L.MODELS_LONG),
        ('RainVolume',      'obs_rainvol',         'log1p',   'fc_rainvol',       L.MODELS_LONG),
    ],
    'Daily': [
        ('MinTemperature',  'obs_mintemp',         None,      'fc_mintemp',       L.MODELS),
        ('MaxTemperature',  'obs_maxtemp',         None,      'fc_maxtemp',       L.MODELS),
        ('WindSpeed',       'obs_windspeed_avg',   None,      'fc_windspeed_kmh', L.MODELS),
        ('WindDirection',   'obs_winddir',         'winddir', 'fc_winddir',       L.MODELS),
        ('RainProbability', 'obs_rainprob',        None,      'fc_rainprob',      L.MODELS),
        ('RainVolume',      'obs_rainvol',         'log1p',   'fc_rainvol',       L.MODELS),
    ],
}


def _fit_quantile(X, y, alpha):
    return LGBMRegressor(objective='quantile', alpha=alpha, **LGBM_BASE_PARAMS).fit(X, y)


def _train_quantile_trio(X_tr, y_tr):
    return {
        'p10': _fit_quantile(X_tr, y_tr, 0.1),
        'p50': _fit_quantile(X_tr, y_tr, 0.5),
        'p90': _fit_quantile(X_tr, y_tr, 0.9),
    }


def train_one(tier, target_label, target_col, transform, source_fc_prefix,
              tier_models, train_df, test_df, feature_cols):
    """Train a single (tier, target). Returns a dict to store in models.pkl plus metrics."""
    source_feature_map = L.build_source_feature_map(feature_cols, models=tier_models)
    payload = {
        'feature_cols': feature_cols,
        'source_feature_map': source_feature_map,
        'transform': transform,
        'tier_models': tier_models,
        'source_fc_cols': [f'{m}_{source_fc_prefix}' for m in tier_models],
    }
    metrics = {'target': target_label, 'tier': tier}

    if transform == 'winddir':
        rad_tr = np.deg2rad(train_df[target_col])
        rad_te = np.deg2rad(test_df[target_col])
        sin_tr, cos_tr = np.sin(rad_tr), np.cos(rad_tr)

        mask_tr = train_df[target_col].notna()
        mask_te = test_df[target_col].notna()
        Xtr = train_df.loc[mask_tr, feature_cols]
        Xte = test_df.loc[mask_te, feature_cols]

        payload['sin'] = _train_quantile_trio(Xtr, sin_tr[mask_tr])
        payload['cos'] = _train_quantile_trio(Xtr, cos_tr[mask_tr])

        sin_p50 = payload['sin']['p50'].predict(Xte)
        cos_p50 = payload['cos']['p50'].predict(Xte)
        angle = np.rad2deg(np.arctan2(sin_p50, cos_p50)) % 360
        metrics['mae'] = L.circular_mae(test_df.loc[mask_te, target_col], angle)
        metrics['n_test'] = int(mask_te.sum())
        metrics['coverage_80'] = None  # not meaningful for circular
    else:
        y_tr_raw = train_df[target_col]
        y_te_raw = test_df[target_col]
        mask_tr = y_tr_raw.notna()
        mask_te = y_te_raw.notna()
        Xtr = train_df.loc[mask_tr, feature_cols]
        Xte = test_df.loc[mask_te, feature_cols]
        y_tr = y_tr_raw[mask_tr]
        y_te = y_te_raw[mask_te]

        if transform == 'log1p':
            y_tr_use = np.log1p(np.clip(y_tr, 0, None))
        else:
            y_tr_use = y_tr

        payload.update(_train_quantile_trio(Xtr, y_tr_use))

        def _inv(arr):
            return np.clip(np.expm1(arr), 0, None) if transform == 'log1p' else arr

        p10 = _inv(payload['p10'].predict(Xte))
        p50 = _inv(payload['p50'].predict(Xte))
        p90 = _inv(payload['p90'].predict(Xte))

        metrics['mae'] = float(mean_absolute_error(y_te, p50))
        metrics['coverage_80'] = float(((y_te.values >= p10) & (y_te.values <= p90)).mean())
        metrics['n_test'] = int(mask_te.sum())

        # Best source MAE on the same rows, for context
        best_src_mae = None
        for fc in payload['source_fc_cols']:
            if fc not in test_df.columns:
                continue
            m = mask_te & test_df[fc].notna()
            if m.sum() == 0:
                continue
            src_mae = float(mean_absolute_error(y_te_raw[m], test_df.loc[m, fc]))
            if best_src_mae is None or src_mae < best_src_mae:
                best_src_mae = src_mae
        metrics['best_source_mae'] = best_src_mae

    return payload, metrics


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()
    engine = create_engine(os.environ['DATABASE_URL'])
    matrices = build_training_matrices(engine)

    models_out = {}
    all_metrics = []

    for tier, df in matrices.items():
        if len(df) == 0:
            log.warning(f'{tier}: empty matrix, skipping')
            continue
        train_df, test_df, _ = L.time_split(df, test_frac=0.2)
        tier_models_list = TIER_TARGETS[tier][0][4]  # Each tier shares one model list
        feature_cols = L.feature_columns(df, tier_models_list)

        for target_label, target_col, transform, source_fc_prefix, tier_models in TIER_TARGETS[tier]:
            if target_col not in df.columns:
                log.warning(f'{tier} {target_label}: target_col {target_col} missing, skipping')
                continue
            log.info(f'Training {tier} / {target_label} ({transform or "raw"})...')
            payload, metrics = train_one(
                tier, target_label, target_col, transform, source_fc_prefix,
                tier_models, train_df, test_df, feature_cols,
            )
            models_out[(tier, target_label)] = payload
            all_metrics.append(metrics)
            mae_str = f"{metrics.get('mae', float('nan')):.3f}"
            cov_str = ('-' if metrics.get('coverage_80') is None
                       else f"{metrics['coverage_80']*100:.0f}%")
            src_str = ('-' if metrics.get('best_source_mae') is None
                       else f"{metrics['best_source_mae']:.3f}")
            log.info(f'  -> MAE {mae_str} | coverage80 {cov_str} | best_src {src_str} | n_test {metrics["n_test"]}')

    log.info(f'Pickling {len(models_out)} (tier,target) entries to {MODELS_PATH}')
    with open(MODELS_PATH, 'wb') as f:
        pickle.dump(models_out, f, protocol=pickle.HIGHEST_PROTOCOL)

    with open(METRICS_PATH, 'w') as f:
        json.dump({
            'trained_at': pd.Timestamp.utcnow().isoformat(),
            'duration_s': round(time.time() - t0, 1),
            'metrics': all_metrics,
        }, f, indent=2)

    log.info(f'Done in {time.time() - t0:.1f}s.')


if __name__ == '__main__':
    main()

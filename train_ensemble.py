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
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.metrics import brier_score_loss, mean_absolute_error
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

# Tighter regularisation for Daily heads — with only a few weeks of training
# data, the full-capacity LGBM memorises (city × doy) climatology and ignores
# source forecasts. Smaller trees + larger leaves force broader patterns.
DAILY_LGBM_PARAMS = dict(
    n_estimators=150,
    learning_rate=0.05,
    num_leaves=15,
    min_data_in_leaf=100,
    n_jobs=-1,
    random_state=0,
    verbose=-1,
)


def _lgbm_params(tier: str) -> dict:
    return DAILY_LGBM_PARAMS if tier == 'Daily' else LGBM_BASE_PARAMS


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

    # ---- Daily (all 4 models as features; rows must have OpenMeteo-ECMWF) ----
    # models_to_pivot lists the sources that BECOME FEATURES — all 4 here, so
    # every training row has columns for MetOffice / OpenMeteo-ECMWF /
    # OpenMeteo-GFSHRRR / AccuWeather (NaN where that source had no forecast).
    # require_models is a ROW FILTER, not a feature filter: it drops rows where
    # OpenMeteo-ECMWF has no forecast for the target date. OM-ECMWF is the only
    # source that reliably reaches 14 days (MetOffice caps at ~7d, AccuWeather
    # at 5d), so requiring it keeps every horizon-day represented in training.
    daily = L.build_pivot(raw_d, models_to_pivot=L.MODELS,
                          require_models=['OpenMeteo-ECMWF'], hours_filter=None,
                          index_cols=L.DAILY_INDEX_COLS, value_cols=L.DAILY_VALUE_COLS)
    daily = L.add_rain_targets(daily)
    # NOTE: deliberately no add_time_features here. With < ~6 months of data,
    # doy_sin/cos doesn't encode seasonality — it just identifies the narrow
    # training window, and the model uses (city × doy) as a climatology lookup
    # that crowds out the source forecasts. Re-enable once 180+ days of data
    # have accumulated.
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
        # Binary classifier rather than quantile regression: obs_rainprob is
        # {0, 100} so quantile loss gives the trees no usable gradient (the
        # pre-fix model literally had total_gain == 0).
        ('RainProbability', 'obs_rainprob',        'binary',  'fc_rainprob',      L.MODELS),
        ('RainVolume',      'obs_rainvol',         'log1p',   'fc_rainvol',       L.MODELS),
    ],
}

# Initial fixed band for the binary RainProbability head (centered on the
# predicted probability × 100). Conformal calibration tightens or widens this
# downstream via the per-head 'conformal_k' stored alongside the model.
BINARY_BAND_HALF_WIDTH = 25.0


def _fit_quantile(X, y, alpha, params):
    return LGBMRegressor(objective='quantile', alpha=alpha, **params).fit(X, y)


def _train_quantile_trio(X_tr, y_tr, params):
    return {
        'p10': _fit_quantile(X_tr, y_tr, 0.1, params),
        'p50': _fit_quantile(X_tr, y_tr, 0.5, params),
        'p90': _fit_quantile(X_tr, y_tr, 0.9, params),
    }


def _conformal_k(y_te, p10, p50, p90, target_coverage=0.8) -> float:
    """Return scaling factor k such that p50 ± k·half_band covers
    ~target_coverage of the holdout. Higher k = wider band."""
    y_te = np.asarray(y_te, dtype=float)
    band_low = np.maximum(p50 - p10, 1e-9)
    band_high = np.maximum(p90 - p50, 1e-9)
    score = np.maximum((p50 - y_te) / band_low, (y_te - p50) / band_high)
    score = np.clip(score, 0, None)
    if len(score) == 0:
        return 1.0
    return float(np.quantile(score, target_coverage))


def train_one(tier, target_label, target_col, transform, source_fc_prefix,
              tier_models, train_df, test_df, feature_cols):
    """Train a single (tier, target). Returns a dict to store in models.pkl plus metrics."""
    source_feature_map = L.build_source_feature_map(feature_cols, models=tier_models)
    params = _lgbm_params(tier)
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

        payload['sin'] = _train_quantile_trio(Xtr, sin_tr[mask_tr], params)
        payload['cos'] = _train_quantile_trio(Xtr, cos_tr[mask_tr], params)

        sin_p50 = payload['sin']['p50'].predict(Xte)
        cos_p50 = payload['cos']['p50'].predict(Xte)
        angle = np.rad2deg(np.arctan2(sin_p50, cos_p50)) % 360
        metrics['mae'] = L.circular_mae(test_df.loc[mask_te, target_col], angle)
        metrics['n_test'] = int(mask_te.sum())
        metrics['coverage_80'] = None  # not meaningful for circular

    elif transform == 'binary':
        # obs_rainprob is {0, 100}. Train a classifier on the {0,1} mapping;
        # at predict, p50 = predict_proba × 100 with a fixed ±25 band that
        # conformal calibration will tune.
        y_tr_raw = train_df[target_col]
        y_te_raw = test_df[target_col]
        mask_tr = y_tr_raw.notna()
        mask_te = y_te_raw.notna()
        Xtr = train_df.loc[mask_tr, feature_cols]
        Xte = test_df.loc[mask_te, feature_cols]
        y_tr_bin = (y_tr_raw[mask_tr] > 0).astype(int).values
        y_te_bin = (y_te_raw[mask_te] > 0).astype(int).values

        clf = LGBMClassifier(objective='binary', **params).fit(Xtr, y_tr_bin)
        payload['classifier'] = clf
        payload['p_band'] = BINARY_BAND_HALF_WIDTH

        proba = clf.predict_proba(Xte)[:, 1]
        p50 = proba * 100.0
        p10 = np.clip(p50 - BINARY_BAND_HALF_WIDTH, 0, 100)
        p90 = np.clip(p50 + BINARY_BAND_HALF_WIDTH, 0, 100)

        metrics['mae'] = float(mean_absolute_error(y_te_raw[mask_te], p50))
        metrics['brier'] = float(brier_score_loss(y_te_bin, proba))
        metrics['coverage_80'] = float(
            ((y_te_raw[mask_te].values >= p10) & (y_te_raw[mask_te].values <= p90)).mean())
        metrics['n_test'] = int(mask_te.sum())

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

        payload['conformal_k'] = _conformal_k(
            y_te_raw[mask_te].values, p10, p50, p90)
        metrics['conformal_k'] = payload['conformal_k']

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

        payload.update(_train_quantile_trio(Xtr, y_tr_use, params))

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

        payload['conformal_k'] = _conformal_k(y_te.values, p10, p50, p90)
        metrics['conformal_k'] = payload['conformal_k']

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
            extra = ''
            if 'brier' in metrics:
                extra += f' | brier {metrics["brier"]:.3f}'
            if 'conformal_k' in metrics:
                extra += f' | k {metrics["conformal_k"]:.2f}'
            log.info(f'  -> MAE {mae_str} | coverage80 {cov_str} | best_src {src_str} | n_test {metrics["n_test"]}{extra}')

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

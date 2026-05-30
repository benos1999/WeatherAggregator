"""Model quality audit — read-only diagnostic for the Daily ensemble.

Prints a markdown report to stdout. Does not write to the DB and does not
mutate models.pkl. Run as:

    python model_quality_audit.py            # full audit
    python model_quality_audit.py --quick    # skip A5 (re-training, slow)

Context: the Daily ensemble's p50 predictions sit flat across the 14-day
horizon while source forecasts vary. This script confirms the suspected
causes (lag-feature dominance, narrow training window, degenerate
RainProbability head) and rules out silent bugs (column misalignment at
predict time). See plans/we-need-to-talk-rippling-unicorn.md.
"""

from __future__ import annotations

import argparse
import os
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error
from sqlalchemy import create_engine

import ensemble_lib as L
import predict_ensemble as P
import train_ensemble as T

load_dotenv()
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 160)

MODELS_PATH = Path('models.pkl')
FOCAL_CITY = 'London'
DAILY_MEASURES = ['MinTemperature', 'MaxTemperature', 'WindSpeed',
                  'RainProbability', 'RainVolume']


# ---------------------------------------------------------------------------
# Feature bucketing
# ---------------------------------------------------------------------------

def bucket_feature(name: str, models=L.MODELS) -> str:
    for m in models:
        if name.startswith(f'{m}_'):
            return m
    if name.startswith('lag'):
        return 'lag obs'
    if name.startswith('doy_') or name.startswith('hour_'):
        return 'time'
    if name.startswith('city_'):
        return 'city'
    if name.endswith('_present'):
        return 'presence'
    if any(s in name for s in ('_stddev_24', '_trend_12', '_range_24')):
        return 'evolution'
    if name in ('days_ahead', 'hours_ahead'):
        return 'horizon'
    return 'other'


# ---------------------------------------------------------------------------
# A1. Data span + distribution shift
# ---------------------------------------------------------------------------

def section_a1(daily_train: pd.DataFrame, engine) -> dict:
    print('## A1. Training-data span and distribution shift\n')

    ft = pd.to_datetime(daily_train['ForecastTaken'])
    ft_min, ft_max = ft.min(), ft.max()
    span_days = (ft_max - ft_min).days + 1
    print(f'- ForecastTaken span: **{ft_min} → {ft_max}** ({span_days} days)')
    print(f'- Total Daily training rows: **{len(daily_train):,}**')
    print(f'- Distinct cities: {daily_train["city"].nunique()}')
    print(f'- Distinct target Dates: {daily_train["Date"].nunique()}')
    print(f'- Rows per city:')
    rpc = daily_train.groupby('city').size()
    for c, n in rpc.items():
        print(f'    {c:<14} {n:>6,}')
    print()

    obs_cols = ['obs_mintemp', 'obs_maxtemp', 'obs_windspeed_avg',
                'obs_rain_total', 'obs_rainprob']
    train_dist = daily_train[obs_cols].describe().T[['mean', 'std', 'min', 'max']]
    print('### Training obs distribution (all cities):')
    print(train_dist.round(2).to_string())
    print()

    live = pd.read_sql(L.LIVE_DAILY_SQL, engine, params={'mph_to_kmh': L.MPH_TO_KMH})
    fc_cols = ['fc_mintemp', 'fc_maxtemp', 'fc_windspeed_kmh',
               'fc_rainvol', 'fc_rainprob']
    live_dist = live[fc_cols].describe().T[['mean', 'std', 'min', 'max']]
    print('### Live source forecast distribution (next 14 days, all cities):')
    print(live_dist.round(2).to_string())
    print()

    shift_warning = span_days < 30
    if shift_warning:
        print(f'> ⚠️  Training window is **{span_days} days** (< 30). Holdout MAEs '
              'reported in metrics.json are graded on a similar narrow window '
              'and over-estimate real-world skill.\n')

    return {
        'span_days': span_days,
        'narrow_window': shift_warning,
    }


# ---------------------------------------------------------------------------
# A2. Feature importance per head + bucket aggregation
# ---------------------------------------------------------------------------

def section_a2(models_dict: dict) -> dict:
    print('## A2. Feature importance per Daily head\n')

    lag_dominance_count = 0
    bucket_table = []

    for measure in DAILY_MEASURES:
        key = ('Daily', measure)
        if key not in models_dict:
            print(f'### Daily / {measure}: model missing in models.pkl\n')
            continue
        payload = models_dict[key]
        transform = payload.get('transform')
        if transform == 'winddir':
            continue

        feature_cols = payload['feature_cols']
        if transform == 'binary':
            booster = payload['classifier'].booster_
        else:
            booster = payload['p50'].booster_
        gain = booster.feature_importance(importance_type='gain')
        total = float(gain.sum())
        if total == 0:
            print(f'### Daily / {measure}: total gain is 0 (model is constant!)\n')
            continue

        imp = pd.DataFrame({'feature': feature_cols, 'gain': gain})
        imp['pct'] = imp['gain'] / total * 100
        imp['bucket'] = imp['feature'].apply(bucket_feature)
        imp = imp.sort_values('gain', ascending=False).reset_index(drop=True)

        print(f'### Daily / {measure}')
        print('Top 20 features by gain:')
        print(imp.head(20)[['feature', 'pct', 'bucket']]
                  .to_string(index=False, formatters={'pct': '{:.1f}%'.format}))

        bucket_pct = imp.groupby('bucket')['pct'].sum().sort_values(ascending=False)
        print('\nBy bucket:')
        for b, p in bucket_pct.items():
            marker = '   <-- LAG DOMINANCE' if (b == 'lag obs' and p > 30) else ''
            print(f'    {b:<22} {p:>5.1f}%{marker}')
        print()

        if bucket_pct.get('lag obs', 0) > 30:
            lag_dominance_count += 1
        bucket_table.append({'measure': measure, **bucket_pct.to_dict()})

    print(f'> Lag dominance (>30% importance) confirmed in '
          f'**{lag_dominance_count} of {len(DAILY_MEASURES)}** Daily heads.\n')

    return {
        'lag_dominant_heads': lag_dominance_count,
        'lag_dominance_confirmed': lag_dominance_count >= 3,
    }


# ---------------------------------------------------------------------------
# A3. Live prediction breakdown for focal city
# ---------------------------------------------------------------------------

def section_a3(engine, models_dict: dict) -> dict:
    print(f'## A3. Live prediction breakdown — {FOCAL_CITY}\n')

    matrices = P.build_prediction_matrices(engine)
    daily = matrices['Daily']
    daily_city = daily[daily['city'] == FOCAL_CITY].copy()

    if daily_city.empty:
        print(f'> No live Daily rows for {FOCAL_CITY} — predict matrix is empty.\n')
        return {'source_cols_vary': False}

    daily_city = daily_city.sort_values('days_ahead').reset_index(drop=True)

    # Verify source columns vary across days_ahead — if they don't, that's a
    # smoking gun for a silent column-misalignment bug.
    source_fc_cols = [c for c in daily_city.columns
                      if any(c.startswith(f'{m}_fc_') for m in L.MODELS)
                      and 'winddir' not in c]
    varying_cols = []
    constant_cols = []
    for c in source_fc_cols:
        vals = daily_city[c].dropna()
        if len(vals) >= 2 and vals.nunique() > 1:
            varying_cols.append(c)
        else:
            constant_cols.append(c)
    print(f'- Source forecast columns that vary across days_ahead: '
          f'{len(varying_cols)} / {len(source_fc_cols)}')
    if constant_cols:
        print(f'- Constant/all-NaN source columns ({len(constant_cols)}):')
        for c in constant_cols:
            print(f'    {c}')
    print()

    preds = P.predict_tier('Daily', daily_city, models_dict)

    # Merge predictions back onto the daily_city rows by Date.
    for measure in DAILY_MEASURES:
        sub = preds[preds['measure'] == measure][['Date', 'p10', 'p50', 'p90']]
        if sub.empty:
            continue
        merged = daily_city.merge(sub, on='Date', how='left').sort_values('days_ahead')

        # Pick the source forecast columns relevant to this measure
        measure_to_fc = {
            'MinTemperature': 'fc_mintemp',
            'MaxTemperature': 'fc_maxtemp',
            'WindSpeed': 'fc_windspeed_kmh',
            'RainProbability': 'fc_rainprob',
            'RainVolume': 'fc_rainvol',
        }
        fc_suffix = measure_to_fc[measure]
        src_cols = [f'{m}_{fc_suffix}' for m in L.MODELS if f'{m}_{fc_suffix}' in merged.columns]

        lag_col = 'lag24h_temp' if 'Temp' in measure else (
            'lag24h_wind' if measure == 'WindSpeed' else 'lag24h_rain')
        lag_cols = [lag_col] if lag_col in merged.columns else []

        cols = ['days_ahead', 'Date'] + src_cols + lag_cols + ['p10', 'p50', 'p90']
        print(f'### {measure}')
        print(merged[cols].round(2).to_string(index=False))
        print()

    return {
        'source_cols_vary': len(varying_cols) > 0 and len(constant_cols) == 0,
        'constant_source_cols': constant_cols,
    }


# ---------------------------------------------------------------------------
# A4. RainProbability target class balance
# ---------------------------------------------------------------------------

def section_a4(daily_train: pd.DataFrame) -> dict:
    print('## A4. Daily RainProbability target distribution\n')

    rp = daily_train['obs_rainprob'].dropna()
    n = len(rp)
    n0 = int((rp == 0).sum())
    n100 = int((rp == 100).sum())
    pct0 = 100 * n0 / n
    pct100 = 100 * n100 / n
    print(f'- All cities: {n0:,} dry ({pct0:.1f}%), {n100:,} rainy ({pct100:.1f}%) of {n:,}')

    print('- Per-city:')
    by_city = daily_train.groupby('city')['obs_rainprob'].agg(
        lambda s: (s == 100).mean() * 100)
    for c, p in by_city.items():
        print(f'    {c:<14} {p:>5.1f}% rainy days')
    print()

    skewed = max(pct0, pct100) > 60
    if skewed:
        majority = 'rainy (100)' if pct100 > pct0 else 'dry (0)'
        print(f'> ⚠️  Binary target is **skewed**: {majority} is the majority. '
              'Quantile p50 will collapse to the majority class — this is the '
              'root cause of the RainProbability orange line being pinned.\n')

    return {'rainprob_skewed': skewed, 'pct_rainy': pct100}


# ---------------------------------------------------------------------------
# A5. Holdout integrity check (reversed split + 3-fold CV on MinTemp)
# ---------------------------------------------------------------------------

def section_a5(daily_train: pd.DataFrame, quick: bool) -> dict:
    if quick:
        print('## A5. Holdout integrity check — SKIPPED (--quick)\n')
        return {'reversed_mae_ratio': None}

    print('## A5. Holdout integrity check (Daily / MinTemperature)\n')

    # Small/fast LGBM for the audit
    from lightgbm import LGBMRegressor
    fast = dict(objective='quantile', alpha=0.5, n_estimators=200,
                learning_rate=0.05, num_leaves=63, n_jobs=-1,
                random_state=0, verbose=-1)

    feature_cols = L.feature_columns(daily_train, L.MODELS)
    target_col = 'obs_mintemp'
    df = daily_train.dropna(subset=[target_col]).copy()
    df = df.sort_values('ForecastTaken').reset_index(drop=True)

    def _mae(train_df, test_df):
        m = LGBMRegressor(**fast).fit(train_df[feature_cols], train_df[target_col])
        return float(mean_absolute_error(test_df[target_col],
                                         m.predict(test_df[feature_cols])))

    # Production split: oldest 80% train, newest 20% test
    n = len(df)
    prod_cutoff = int(n * 0.8)
    prod_train = df.iloc[:prod_cutoff]
    prod_test = df.iloc[prod_cutoff:]
    prod_mae = _mae(prod_train, prod_test)

    # Reversed split: newest 80% train, oldest 20% test
    rev_test = df.iloc[:int(n * 0.2)]
    rev_train = df.iloc[int(n * 0.2):]
    rev_mae = _mae(rev_train, rev_test)

    print(f'- Production-direction MAE (train oldest 80%, test newest 20%): **{prod_mae:.3f}**')
    print(f'- Reversed-direction MAE (train newest 80%, test oldest 20%):  **{rev_mae:.3f}**')
    ratio = rev_mae / prod_mae if prod_mae > 0 else float('inf')
    print(f'- Ratio (reversed / production): {ratio:.2f}x')
    print()

    # Blocked 3-fold (chronological thirds)
    print('- Blocked 3-fold (hold out one chronological third at a time):')
    fold_size = n // 3
    fold_maes = []
    for i in range(3):
        lo = i * fold_size
        hi = (i + 1) * fold_size if i < 2 else n
        test = df.iloc[lo:hi]
        train = pd.concat([df.iloc[:lo], df.iloc[hi:]])
        mae = _mae(train, test)
        fold_maes.append(mae)
        print(f'    fold {i+1}: MAE {mae:.3f}  (n_test={len(test):,})')
    print(f'    mean={np.mean(fold_maes):.3f}, std={np.std(fold_maes):.3f}')
    if np.std(fold_maes) > 0.5:
        print('    > High fold-to-fold variance — model performance is unstable across time windows.')
    print()

    return {
        'reversed_mae_ratio': ratio,
        'reversed_split_bad': ratio > 2.0,
        'fold_mae_std': float(np.std(fold_maes)),
    }


# ---------------------------------------------------------------------------
# A6. Findings footer
# ---------------------------------------------------------------------------

def section_a6(findings: dict) -> None:
    print('## A6. Findings\n')
    bullets = [
        ('Lag obs > 30% importance in ≥3 Daily heads',
         findings.get('lag_dominance_confirmed')),
        ('Source forecast columns vary across days_ahead at predict time',
         findings.get('source_cols_vary')),
        ('Reversed-split MAE > 2× production-split MAE',
         findings.get('reversed_split_bad')),
        ('Daily training matrix spans < 30 days',
         findings.get('narrow_window')),
        ('Daily obs_rainprob class balance > 60% one way',
         findings.get('rainprob_skewed')),
    ]
    for label, val in bullets:
        if val is None:
            mark = '  ?'
        elif val:
            mark = '[X]'
        else:
            mark = '[ ]'
        print(f'- {mark} {label}')
    print()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--quick', action='store_true', help='Skip A5 (slow retraining).')
    args = ap.parse_args()

    if not MODELS_PATH.exists():
        raise SystemExit(f'{MODELS_PATH} missing — run train_ensemble.py first.')
    with open(MODELS_PATH, 'rb') as f:
        models_dict = pickle.load(f)

    engine = create_engine(os.environ['DATABASE_URL'])
    print('# Model quality audit\n')
    print(f'Loaded {len(models_dict)} (tier, target) heads from {MODELS_PATH}\n')

    print('_Building Daily training matrix..._\n')
    matrices = T.build_training_matrices(engine)
    daily_train = matrices['Daily']
    print()

    findings = {}
    findings.update(section_a1(daily_train, engine))
    findings.update(section_a2(models_dict))
    findings.update(section_a3(engine, models_dict))
    findings.update(section_a4(daily_train))
    findings.update(section_a5(daily_train, quick=args.quick))
    section_a6(findings)


if __name__ == '__main__':
    main()

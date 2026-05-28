"""Roll the enriched efficacy slice into per-day model-quality tables
and assemble the metadata footer for the Tableau dashboard.

Three top-level builders:
  build_metrics_daily(efficacy_df) -> DataFrame
    per (date, tier, target, source): n, mae, bias, coverage_80,
    skill_vs_best_source, brier (rain prob), cond_mae_rainy (rain volume)
  build_reliability(efficacy_df) -> DataFrame
    Per (tier, target=RainProbability, decile_lo..decile_hi):
    n_predictions, mean_predicted, observed_rate
  build_ops_metadata(engine, metrics_json_path) -> DataFrame
    key/value pairs: model freshness, source freshness, obs freshness

All inputs are pure DataFrames produced by tableau_export (so the
expensive SQL only runs once per export cycle).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

RAIN_PROB_TARGET = 'RainProbability'
RAIN_VOL_TARGET = 'RainVolume'

METRICS_DAILY_COLUMNS = [
    'date', 'tier', 'target', 'source',
    'n', 'mae', 'bias', 'coverage_80',
    'skill_vs_best_source', 'brier', 'cond_mae_rainy',
]
RELIABILITY_COLUMNS = [
    'tier', 'target', 'decile_lo', 'decile_hi',
    'n_predictions', 'mean_predicted', 'observed_rate',
]
OPS_COLUMNS = ['metric', 'value', 'unit']


# ---------------------------------------------------------------------------
# metrics_daily
# ---------------------------------------------------------------------------

def _classify_tier(efficacy_df: pd.DataFrame) -> pd.Series:
    """All efficacy rows currently carry tier='Efficacy'; derive a finer
    'Hourly' / 'Daily' tier from the presence of a time component so the
    metrics table can split them. Returns a Series aligned to efficacy_df."""
    is_hourly = efficacy_df['time'].notna() & (efficacy_df['time'].astype(str) != 'None')
    return np.where(is_hourly, 'Hourly', 'Daily')


def build_metrics_daily(efficacy_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate efficacy rows into per-day quality scores."""
    if efficacy_df.empty:
        return pd.DataFrame(columns=METRICS_DAILY_COLUMNS)

    df = efficacy_df.copy()
    df = df[df['error'].notna()]
    if df.empty:
        return pd.DataFrame(columns=METRICS_DAILY_COLUMNS)

    df['tier'] = _classify_tier(df)
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Recover the observation from value + error so we don't need to re-query.
    df['obs_value'] = df['value'] - df['error']

    grouped = df.groupby(['date', 'tier', 'target', 'source'], dropna=False)

    metrics = grouped.agg(
        n=('error', 'size'),
        mae=('error', lambda s: float(np.mean(np.abs(s)))),
        bias=('error', lambda s: float(np.mean(s))),
    ).reset_index()

    # 80% coverage — only meaningful for Ensemble rows (have p10/p90)
    def _coverage(g: pd.DataFrame) -> float:
        m = g['p10'].notna() & g['p90'].notna()
        if m.sum() == 0:
            return float('nan')
        return float(((g.loc[m, 'obs_value'] >= g.loc[m, 'p10'])
                      & (g.loc[m, 'obs_value'] <= g.loc[m, 'p90'])).mean())
    cov = (df.groupby(['date', 'tier', 'target', 'source'], dropna=False)
             .apply(_coverage, include_groups=False)
             .reset_index(name='coverage_80'))
    metrics = metrics.merge(cov, on=['date', 'tier', 'target', 'source'], how='left')
    # Coverage only meaningful for Ensemble; NULL elsewhere
    metrics.loc[metrics['source'] != 'Ensemble', 'coverage_80'] = np.nan

    # skill_vs_best_source: per (date, tier, target), compare Ensemble MAE
    # to the best source MAE. Attached only to the Ensemble row.
    src_mae = metrics[metrics['source'] != 'Ensemble'].copy()
    if not src_mae.empty:
        best = (src_mae.groupby(['date', 'tier', 'target'])['mae']
                       .min().reset_index(name='best_source_mae'))
        metrics = metrics.merge(best, on=['date', 'tier', 'target'], how='left')
        is_ens = metrics['source'] == 'Ensemble'
        metrics['skill_vs_best_source'] = np.where(
            is_ens & metrics['best_source_mae'].notna() & (metrics['best_source_mae'] > 0),
            (metrics['best_source_mae'] - metrics['mae']) / metrics['best_source_mae'],
            np.nan,
        )
        metrics = metrics.drop(columns=['best_source_mae'])
    else:
        metrics['skill_vs_best_source'] = np.nan

    # Brier on RainProbability — mean((p/100 − indicator)^2)
    def _brier(g: pd.DataFrame) -> float:
        # value is the predicted probability (0-100); obs is 0 or 100 (already in [0,100]).
        return float(np.mean(((g['value'] - g['obs_value']) / 100.0) ** 2))
    brier_df = (df[df['target'] == RAIN_PROB_TARGET]
                .groupby(['date', 'tier', 'target', 'source'], dropna=False)
                .apply(_brier, include_groups=False)
                .reset_index(name='brier'))
    metrics = metrics.merge(brier_df, on=['date', 'tier', 'target', 'source'], how='left')

    # Conditional MAE on rainy rows for RainVolume — only rows where obs > 0
    rainvol = df[(df['target'] == RAIN_VOL_TARGET) & (df['obs_value'] > 0)]
    if not rainvol.empty:
        cond = (rainvol.groupby(['date', 'tier', 'target', 'source'], dropna=False)
                       .apply(lambda g: float(np.mean(np.abs(g['error']))),
                              include_groups=False)
                       .reset_index(name='cond_mae_rainy'))
        metrics = metrics.merge(cond, on=['date', 'tier', 'target', 'source'], how='left')
    else:
        metrics['cond_mae_rainy'] = np.nan

    metrics['date'] = pd.to_datetime(metrics['date']).dt.strftime('%Y-%m-%d')
    return metrics[METRICS_DAILY_COLUMNS]


# ---------------------------------------------------------------------------
# reliability (rain probability calibration)
# ---------------------------------------------------------------------------

def build_reliability(efficacy_df: pd.DataFrame, n_bins: int = 10) -> pd.DataFrame:
    """Probability-calibration table for RainProbability over the last 7d."""
    if efficacy_df.empty:
        return pd.DataFrame(columns=RELIABILITY_COLUMNS)

    df = efficacy_df[(efficacy_df['target'] == RAIN_PROB_TARGET)
                     & efficacy_df['error'].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=RELIABILITY_COLUMNS)

    df['tier'] = _classify_tier(df)
    df['obs_value'] = df['value'] - df['error']

    edges = np.linspace(0, 100, n_bins + 1)
    df['decile_lo'] = pd.cut(df['value'].clip(0, 100), bins=edges,
                             include_lowest=True, right=False, labels=edges[:-1]).astype(float)
    # Final bin (90-100] is inclusive on the right
    df.loc[df['value'] >= edges[-1], 'decile_lo'] = edges[-2]
    df['decile_hi'] = df['decile_lo'] + (100 / n_bins)

    grouped = (df.groupby(['tier', 'target', 'decile_lo', 'decile_hi'], dropna=False)
                 .agg(n_predictions=('value', 'size'),
                      mean_predicted=('value', 'mean'),
                      observed_rate=('obs_value', lambda s: float((s > 0).mean() * 100)))
                 .reset_index())
    grouped['source'] = 'all'  # aggregated across sources for the calibration curve
    return grouped[RELIABILITY_COLUMNS]


# ---------------------------------------------------------------------------
# ops metadata
# ---------------------------------------------------------------------------

def build_ops_metadata(engine, metrics_json_path: Path = Path('metrics.json')) -> pd.DataFrame:
    rows: list[dict] = []
    now = datetime.now(timezone.utc)

    # Model freshness
    if metrics_json_path.exists():
        try:
            with metrics_json_path.open() as f:
                meta = json.load(f)
            trained_at = pd.to_datetime(meta.get('trained_at')).tz_convert('UTC')
            rows.append({'metric': 'model_trained_at_utc',
                         'value': trained_at.isoformat(timespec='seconds'), 'unit': 'timestamp'})
            rows.append({'metric': 'model_age_hours',
                         'value': round((now - trained_at).total_seconds() / 3600, 1),
                         'unit': 'hours'})
        except Exception:
            pass

    # Source freshness (latest ForecastTaken per model in hourly_forecast)
    src_df = pd.read_sql(text("""
        SELECT "Model" AS source, MAX("ForecastTaken") AS latest
        FROM hourly_forecast
        WHERE "ForecastTaken" > NOW() - INTERVAL '48 hours'
        GROUP BY "Model"
    """), engine)
    for _, r in src_df.iterrows():
        latest = pd.to_datetime(r['latest']).tz_localize('UTC') \
            if pd.to_datetime(r['latest']).tzinfo is None else pd.to_datetime(r['latest'])
        rows.append({
            'metric': f'latest_forecast_{r["source"]}_hours_ago',
            'value': round((now - latest).total_seconds() / 3600, 1),
            'unit': 'hours',
        })

    # Observation freshness
    obs_latest = pd.read_sql(text("""
        SELECT MAX(("Date" + "Time")::timestamp) AS latest FROM observations
    """), engine).iat[0, 0]
    if obs_latest is not None:
        latest = pd.to_datetime(obs_latest)
        if latest.tzinfo is None:
            latest = latest.tz_localize('UTC')
        rows.append({
            'metric': 'latest_observation_hours_ago',
            'value': round((now - latest).total_seconds() / 3600, 1),
            'unit': 'hours',
        })

    return pd.DataFrame(rows, columns=OPS_COLUMNS)

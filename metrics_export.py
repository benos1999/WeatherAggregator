"""Roll the enriched efficacy slice into per-day model-quality tables
and assemble the metadata footer for the Tableau dashboard.

Three top-level builders:
  build_metrics_daily(efficacy_df) -> DataFrame
    per (date, tier, measure, source): n, mae, bias, coverage_80,
    skill_vs_best_source, brier (rain prob), cond_mae_rainy (rain volume)
  build_reliability(efficacy_df) -> DataFrame
    Per (tier, measure=RainProbability, decile_lo..decile_hi):
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

RAIN_PROB_MEASURE = 'RainProbability'
RAIN_VOL_MEASURE = 'RainVolume'
MPH_TO_KMH = 1.609344

METRICS_DAILY_COLUMNS = [
    'date', 'tier', 'measure', 'source',
    'n', 'mae', 'bias', 'coverage_80',
    'skill_vs_best_source', 'brier', 'cond_mae_rainy',
]
RELIABILITY_COLUMNS = [
    'tier', 'measure', 'source', 'decile_lo', 'decile_hi',
    'n_predictions', 'mean_predicted', 'observed_rate',
]
OPS_COLUMNS = ['metric', 'value', 'unit']
HORIZON_COLUMNS = ['tier', 'city', 'source', 'measure', 'lead_bucket',
                   'n', 'mae', 'bias']


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

    grouped = df.groupby(['date', 'tier', 'measure', 'source'], dropna=False)

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
    cov = (df.groupby(['date', 'tier', 'measure', 'source'], dropna=False)
             .apply(_coverage, include_groups=False)
             .reset_index(name='coverage_80'))
    metrics = metrics.merge(cov, on=['date', 'tier', 'measure', 'source'], how='left')
    # Coverage only meaningful for Ensemble; NULL elsewhere
    metrics.loc[metrics['source'] != 'Ensemble', 'coverage_80'] = np.nan

    # skill_vs_best_source: per (date, tier, measure), compare Ensemble MAE
    # to the best source MAE. Attached only to the Ensemble row.
    src_mae = metrics[metrics['source'] != 'Ensemble'].copy()
    if not src_mae.empty:
        best = (src_mae.groupby(['date', 'tier', 'measure'])['mae']
                       .min().reset_index(name='best_source_mae'))
        metrics = metrics.merge(best, on=['date', 'tier', 'measure'], how='left')
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
    brier_df = (df[df['measure'] == RAIN_PROB_MEASURE]
                .groupby(['date', 'tier', 'measure', 'source'], dropna=False)
                .apply(_brier, include_groups=False)
                .reset_index(name='brier'))
    metrics = metrics.merge(brier_df, on=['date', 'tier', 'measure', 'source'], how='left')

    # Conditional MAE on rainy rows for RainVolume — only rows where obs > 0
    rainvol = df[(df['measure'] == RAIN_VOL_MEASURE) & (df['obs_value'] > 0)]
    if not rainvol.empty:
        cond = (rainvol.groupby(['date', 'tier', 'measure', 'source'], dropna=False)
                       .apply(lambda g: float(np.mean(np.abs(g['error']))),
                              include_groups=False)
                       .reset_index(name='cond_mae_rainy'))
        metrics = metrics.merge(cond, on=['date', 'tier', 'measure', 'source'], how='left')
    else:
        metrics['cond_mae_rainy'] = np.nan

    metrics['date'] = pd.to_datetime(metrics['date']).dt.strftime('%Y-%m-%d')
    return metrics[METRICS_DAILY_COLUMNS]


# ---------------------------------------------------------------------------
# reliability (rain probability calibration)
# ---------------------------------------------------------------------------

def build_reliability(efficacy_df: pd.DataFrame, n_bins: int = 10) -> pd.DataFrame:
    """Probability-calibration table for RainProbability over the last 7d.

    Emits one row per (tier, source, decile bin) so that the reliability
    diagram can be drawn separately for each of MetOffice / OpenMeteo-ECMWF /
    OpenMeteo-GFSHRRR / AccuWeather / Ensemble. Aggregate to "all sources" in
    Tableau if you want a single curve.
    """
    if efficacy_df.empty:
        return pd.DataFrame(columns=RELIABILITY_COLUMNS)

    df = efficacy_df[(efficacy_df['measure'] == RAIN_PROB_MEASURE)
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

    grouped = (df.groupby(['tier', 'measure', 'source', 'decile_lo', 'decile_hi'],
                          dropna=False)
                 .agg(n_predictions=('value', 'size'),
                      mean_predicted=('value', 'mean'),
                      observed_rate=('obs_value', lambda s: float((s > 0).mean() * 100)))
                 .reset_index())
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


# ---------------------------------------------------------------------------
# horizon_accuracy
# ---------------------------------------------------------------------------

# Forecast-vs-observation join, aggregated server-side by
# (city, source, measure, lead_bucket). Aggregating in Postgres keeps the
# result bounded (~13k rows) even as years of history accumulate — pulling
# the raw join rows into pandas would scale linearly with retention.
#
# The compass_points VALUES list is duplicated from ensemble_lib.HOURLY_JOIN_SQL
# because the latter wraps it in a CTE and nested WITHs aren't valid in
# Postgres. Keep both in sync if compass-point degrees ever change.

_COMPASS_POINTS = """
  (VALUES
    ('N', 0.0),   ('NNE', 22.5),  ('NE', 45.0),  ('ENE', 67.5),
    ('E', 90.0),  ('ESE', 112.5), ('SE', 135.0), ('SSE', 157.5),
    ('S', 180.0), ('SSW', 202.5), ('SW', 225.0), ('WSW', 247.5),
    ('W', 270.0), ('WNW', 292.5), ('NW', 315.0), ('NNW', 337.5)
  ) AS t("Direction", "CenterDegrees")
"""

HORIZON_HOURLY_SQL = f"""
WITH compass_points AS (
  SELECT * FROM {_COMPASS_POINTS}
),
joined AS (
  SELECT
    hf.city,
    hf."Model" AS source,
    ROUND(EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600)::int AS lead_bucket,
    hf."Temperature"     AS fc_temperature,
    CASE WHEN hf."Model" = 'MetOffice' THEN hf."WindSpeed" * %(mph_to_kmh)s
         ELSE hf."WindSpeed" END                          AS fc_windspeed_kmh,
    hf."WindDirection"   AS fc_winddir,
    hf."RainProbability" AS fc_rainprob,
    hf."RainVolume"      AS fc_rainvol,
    obs."Temperature"    AS obs_temperature,
    obs."WindSpeed" * %(mph_to_kmh)s                       AS obs_windspeed_kmh,
    cp."CenterDegrees"   AS obs_winddir,
    obs."HourlyRainfall" AS obs_hourly_rain
  FROM hourly_forecast hf
  JOIN observations obs
    ON hf.city = obs.city AND hf."Date" = obs."Date" AND hf."Time" = obs."Time"
  JOIN compass_points cp ON obs."WindDirection" = cp."Direction"
  WHERE EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 > 0
),
errors AS (
  SELECT city, source, lead_bucket, 'Temperature'::text AS measure,
         (fc_temperature - obs_temperature)::real AS err
  FROM joined WHERE fc_temperature IS NOT NULL AND obs_temperature IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'WindSpeed',
         (fc_windspeed_kmh - obs_windspeed_kmh)::real
  FROM joined WHERE fc_windspeed_kmh IS NOT NULL AND obs_windspeed_kmh IS NOT NULL
  UNION ALL
  -- Circular signed error in (-180, 180]. Postgres mod() preserves the sign of
  -- the dividend, so we wrap twice: ((diff+180) mod 360 + 360) mod 360 - 180.
  SELECT city, source, lead_bucket, 'WindDirection',
         (mod(mod((fc_winddir - obs_winddir + 180)::numeric, 360.0) + 360.0, 360.0) - 180)::real
  FROM joined WHERE fc_winddir IS NOT NULL AND obs_winddir IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'RainProbability',
         (fc_rainprob - (CASE WHEN obs_hourly_rain > 0 THEN 100 ELSE 0 END))::real
  FROM joined WHERE fc_rainprob IS NOT NULL AND obs_hourly_rain IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'RainVolume',
         (fc_rainvol - obs_hourly_rain)::real
  FROM joined WHERE fc_rainvol IS NOT NULL AND obs_hourly_rain IS NOT NULL
)
SELECT 'Hourly'::text AS tier, city, source, measure, lead_bucket,
       COUNT(*)::int AS n,
       AVG(ABS(err))::real AS mae,
       AVG(err)::real      AS bias
FROM errors
GROUP BY city, source, measure, lead_bucket
"""

HORIZON_DAILY_SQL = """
WITH daily_obs AS (
  SELECT city, "Date",
         MIN("Temperature")        AS obs_mintemp,
         MAX("Temperature")        AS obs_maxtemp,
         AVG("WindSpeed") * %(mph_to_kmh)s AS obs_windspeed_avg,
         MAX("HourlyRainfall")     AS obs_rain_any,
         SUM("HourlyRainfall")     AS obs_rain_total
  FROM observations
  GROUP BY city, "Date"
),
joined AS (
  SELECT
    df.city, df."Model" AS source,
    (df."Date" - df."ForecastTaken"::date)::int AS lead_bucket,
    df."MinTemperature" AS fc_mintemp,
    df."MaxTemperature" AS fc_maxtemp,
    CASE WHEN df."Model" = 'MetOffice' THEN df."WindSpeed" * %(mph_to_kmh)s
         ELSE df."WindSpeed" END  AS fc_windspeed_kmh,
    df."RainProbability"          AS fc_rainprob,
    df."RainVolume"               AS fc_rainvol,
    d_obs.obs_mintemp, d_obs.obs_maxtemp, d_obs.obs_windspeed_avg,
    d_obs.obs_rain_any, d_obs.obs_rain_total
  FROM daily_forecast df
  JOIN daily_obs d_obs ON df.city = d_obs.city AND df."Date" = d_obs."Date"
  WHERE (df."Date" - df."ForecastTaken"::date) > 0
),
errors AS (
  SELECT city, source, lead_bucket, 'MinTemperature'::text AS measure,
         (fc_mintemp - obs_mintemp)::real AS err
  FROM joined WHERE fc_mintemp IS NOT NULL AND obs_mintemp IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'MaxTemperature',
         (fc_maxtemp - obs_maxtemp)::real
  FROM joined WHERE fc_maxtemp IS NOT NULL AND obs_maxtemp IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'WindSpeed',
         (fc_windspeed_kmh - obs_windspeed_avg)::real
  FROM joined WHERE fc_windspeed_kmh IS NOT NULL AND obs_windspeed_avg IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'RainProbability',
         (fc_rainprob - (CASE WHEN obs_rain_any > 0 THEN 100 ELSE 0 END))::real
  FROM joined WHERE fc_rainprob IS NOT NULL AND obs_rain_any IS NOT NULL
  UNION ALL
  SELECT city, source, lead_bucket, 'RainVolume',
         (fc_rainvol - obs_rain_total)::real
  FROM joined WHERE fc_rainvol IS NOT NULL AND obs_rain_total IS NOT NULL
)
SELECT 'Daily'::text AS tier, city, source, measure, lead_bucket,
       COUNT(*)::int AS n,
       AVG(ABS(err))::real AS mae,
       AVG(err)::real      AS bias
FROM errors
GROUP BY city, source, measure, lead_bucket
"""


def build_horizon_accuracy(engine) -> pd.DataFrame:
    """Per (tier, city, source, measure, lead_bucket) forecast accuracy.

    lead_bucket is the integer lead time at issue:
      Hourly tier — hours_ahead (1..~48)
      Daily  tier — days_ahead  (1..~14)

    'mae' is the mean absolute error; 'bias' is the mean signed error.
    WindDirection uses circular error in (-180, 180].

    Aggregation runs server-side, so output is bounded at roughly
    (11 cities × 4 sources × 5 measures × ~48 lead buckets) + (11 × 4 × 5 × 14)
    ≈ 13,700 rows regardless of how much history has accumulated.
    """
    h = pd.read_sql(HORIZON_HOURLY_SQL, engine, params={'mph_to_kmh': MPH_TO_KMH})
    d = pd.read_sql(HORIZON_DAILY_SQL,  engine, params={'mph_to_kmh': MPH_TO_KMH})
    return pd.concat([h, d], ignore_index=True)[HORIZON_COLUMNS]

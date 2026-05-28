"""Shared helpers for the ensemble forecast pipeline.

Extracted from ensemble_model.ipynb so that train_ensemble.py,
predict_ensemble.py and app.py all use identical feature engineering.

Public API (used by other Phase 3 scripts):
    MODELS, MODELS_LONG, MPH_TO_KMH, EVO_VARS, EVO_VARS_DAILY
    INDEX_COLS, VALUE_COLS, DAILY_INDEX_COLS, DAILY_VALUE_COLS
    HOURLY_JOIN_SQL, DAILY_JOIN_SQL, EVOLUTION_SQL, EVOLUTION_DAILY_SQL
    LIVE_HOURLY_SQL, LIVE_DAILY_SQL   (forecast-only, no obs join — for prediction)
    build_pivot, add_time_features, add_winddir_features, add_city_features
    add_presence_indicators, add_rain_targets
    load_observations, add_lag_features
    fetch_evolution_hourly, fetch_evolution_daily
    merge_evolution, time_split, feature_columns
    build_source_feature_map, aggregate_shap_by_source, predict_quantiles
    circular_mae
"""

from __future__ import annotations

import numpy as np
import pandas as pd

try:
    from lightgbm import LGBMRegressor
    HAS_LGBM = True
except ImportError:
    HAS_LGBM = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODELS = ['MetOffice', 'OpenMeteo-ECMWF', 'OpenMeteo-GFSHRRR', 'AccuWeather']
MODELS_LONG = ['MetOffice', 'OpenMeteo-ECMWF', 'OpenMeteo-GFSHRRR']
MPH_TO_KMH = 1.609344

INDEX_COLS = ['city', 'Date', 'Time', 'ForecastTaken', 'hours_ahead',
              'obs_temperature', 'obs_windspeed_kmh', 'obs_winddir', 'obs_hourly_rain']
VALUE_COLS = ['fc_temperature', 'fc_windspeed_kmh', 'fc_winddir', 'fc_rainprob', 'fc_rainvol']

DAILY_INDEX_COLS = ['city', 'Date', 'ForecastTaken', 'days_ahead',
                    'obs_mintemp', 'obs_maxtemp', 'obs_windspeed_avg',
                    'obs_rain_any', 'obs_rain_total']
DAILY_VALUE_COLS = ['fc_mintemp', 'fc_maxtemp', 'fc_windspeed_kmh',
                    'fc_winddir', 'fc_rainprob', 'fc_rainvol']

# Live (forecast-only, no observation join) index columns used at predict time.
LIVE_INDEX_COLS_HOURLY = ['city', 'Date', 'Time', 'ForecastTaken', 'hours_ahead']
LIVE_INDEX_COLS_DAILY  = ['city', 'Date', 'ForecastTaken', 'days_ahead']

EVO_VARS = ['temp_stddev_24', 'temp_range_24', 'temp_trend_12',
            'wind_stddev_24', 'wind_range_24', 'wind_trend_12',
            'rainprob_stddev_24', 'rainprob_trend_12']

EVO_VARS_DAILY = ['mintemp_stddev_24', 'mintemp_trend_12',
                  'maxtemp_stddev_24', 'maxtemp_trend_12',
                  'wind_stddev_24', 'wind_trend_12',
                  'rainprob_stddev_24', 'rainprob_trend_12']


# ---------------------------------------------------------------------------
# SQL — joined training matrices (forecast + observation)
# ---------------------------------------------------------------------------

HOURLY_JOIN_SQL = """
WITH compass_points AS (
  SELECT * FROM (VALUES
    ('N', 0.0),   ('NNE', 22.5),  ('NE', 45.0),  ('ENE', 67.5),
    ('E', 90.0),  ('ESE', 112.5), ('SE', 135.0), ('SSE', 157.5),
    ('S', 180.0), ('SSW', 202.5), ('SW', 225.0), ('WSW', 247.5),
    ('W', 270.0), ('WNW', 292.5), ('NW', 315.0), ('NNW', 337.5)
  ) AS t("Direction", "CenterDegrees")
)
SELECT
  hf.city,
  hf."Date", hf."Time", hf."ForecastTaken",
  hf."Model",
  EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 AS hours_ahead,
  hf."Temperature"     AS fc_temperature,
  CASE WHEN hf."Model" = 'MetOffice' THEN hf."WindSpeed" * %(mph_to_kmh)s
       ELSE hf."WindSpeed" END                                AS fc_windspeed_kmh,
  hf."WindDirection"   AS fc_winddir,
  hf."RainProbability" AS fc_rainprob,
  hf."RainVolume"      AS fc_rainvol,
  obs."Temperature"    AS obs_temperature,
  obs."WindSpeed" * %(mph_to_kmh)s AS obs_windspeed_kmh,
  cp."CenterDegrees"   AS obs_winddir,
  obs."HourlyRainfall" AS obs_hourly_rain
FROM hourly_forecast hf
JOIN observations obs
  ON hf.city = obs.city
 AND hf."Date" = obs."Date"
 AND hf."Time" = obs."Time"
JOIN compass_points cp ON obs."WindDirection" = cp."Direction"
WHERE EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 > 0
"""

DAILY_JOIN_SQL = """
WITH daily_obs AS (
  SELECT city, "Date",
         MIN("Temperature")        AS obs_mintemp,
         MAX("Temperature")        AS obs_maxtemp,
         AVG("WindSpeed") * %(mph_to_kmh)s AS obs_windspeed_avg,
         MAX("HourlyRainfall")     AS obs_rain_any,
         SUM("HourlyRainfall")     AS obs_rain_total
  FROM observations
  GROUP BY city, "Date"
)
SELECT
  df.city, df."Date", df."Model", df."ForecastTaken",
  (df."Date" - df."ForecastTaken"::date) AS days_ahead,
  df."MinTemperature" AS fc_mintemp,
  df."MaxTemperature" AS fc_maxtemp,
  CASE WHEN df."Model" = 'MetOffice' THEN df."WindSpeed" * %(mph_to_kmh)s
       ELSE df."WindSpeed" END AS fc_windspeed_kmh,
  df."WindDirection"  AS fc_winddir,
  df."RainProbability" AS fc_rainprob,
  df."RainVolume"     AS fc_rainvol,
  d_obs.obs_mintemp, d_obs.obs_maxtemp, d_obs.obs_windspeed_avg,
  d_obs.obs_rain_any, d_obs.obs_rain_total
FROM daily_forecast df
JOIN daily_obs d_obs ON df.city = d_obs.city AND df."Date" = d_obs."Date"
WHERE (df."Date" - df."ForecastTaken"::date) > 0
"""


# ---------------------------------------------------------------------------
# SQL — forecast-only "live" pulls used at prediction time
# (no observation join; pulls the latest ForecastTaken per (city, Model))
# ---------------------------------------------------------------------------

LIVE_HOURLY_SQL = """
WITH latest AS (
  SELECT city, "Model", MAX("ForecastTaken") AS "ForecastTaken"
  FROM hourly_forecast
  WHERE "ForecastTaken" >= NOW() - INTERVAL '6 hours'
  GROUP BY city, "Model"
)
SELECT
  hf.city,
  hf."Date", hf."Time", hf."ForecastTaken",
  hf."Model",
  EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 AS hours_ahead,
  hf."Temperature"     AS fc_temperature,
  CASE WHEN hf."Model" = 'MetOffice' THEN hf."WindSpeed" * %(mph_to_kmh)s
       ELSE hf."WindSpeed" END                                AS fc_windspeed_kmh,
  hf."WindDirection"   AS fc_winddir,
  hf."RainProbability" AS fc_rainprob,
  hf."RainVolume"      AS fc_rainvol
FROM hourly_forecast hf
JOIN latest l
  ON hf.city = l.city AND hf."Model" = l."Model" AND hf."ForecastTaken" = l."ForecastTaken"
WHERE EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 > 0
"""

LIVE_DAILY_SQL = """
WITH latest AS (
  SELECT city, "Model", MAX("ForecastTaken") AS "ForecastTaken"
  FROM daily_forecast
  WHERE "ForecastTaken" >= NOW() - INTERVAL '6 hours'
  GROUP BY city, "Model"
)
SELECT
  df.city, df."Date", df."Model", df."ForecastTaken",
  (df."Date" - df."ForecastTaken"::date) AS days_ahead,
  df."MinTemperature" AS fc_mintemp,
  df."MaxTemperature" AS fc_maxtemp,
  CASE WHEN df."Model" = 'MetOffice' THEN df."WindSpeed" * %(mph_to_kmh)s
       ELSE df."WindSpeed" END AS fc_windspeed_kmh,
  df."WindDirection"  AS fc_winddir,
  df."RainProbability" AS fc_rainprob,
  df."RainVolume"     AS fc_rainvol
FROM daily_forecast df
JOIN latest l
  ON df.city = l.city AND df."Model" = l."Model" AND df."ForecastTaken" = l."ForecastTaken"
WHERE (df."Date" - df."ForecastTaken"::date) > 0
"""


# ---------------------------------------------------------------------------
# Forecast-evolution SQL
# ---------------------------------------------------------------------------

EVOLUTION_SQL = """
SELECT
  city,
  "Model",
  "Date", "Time", "ForecastTaken",
  STDDEV("Temperature") OVER w24      AS temp_stddev_24,
  MAX("Temperature")    OVER w24
    - MIN("Temperature") OVER w24     AS temp_range_24,
  "Temperature" - LAG("Temperature", 12) OVER w_ordered AS temp_trend_12,
  STDDEV("WindSpeed") OVER w24        AS wind_stddev_24,
  MAX("WindSpeed") OVER w24
    - MIN("WindSpeed") OVER w24       AS wind_range_24,
  "WindSpeed" - LAG("WindSpeed", 12) OVER w_ordered      AS wind_trend_12,
  STDDEV("RainProbability") OVER w24  AS rainprob_stddev_24,
  "RainProbability" - LAG("RainProbability", 12) OVER w_ordered AS rainprob_trend_12
FROM hourly_forecast
WINDOW
  w_ordered AS (PARTITION BY city, "Model", "Date", "Time" ORDER BY "ForecastTaken"),
  w24       AS (PARTITION BY city, "Model", "Date", "Time" ORDER BY "ForecastTaken"
                ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
"""

EVOLUTION_DAILY_SQL = """
SELECT
  city,
  "Model",
  "Date", "ForecastTaken",
  STDDEV("MinTemperature") OVER w24   AS mintemp_stddev_24,
  "MinTemperature" - LAG("MinTemperature", 12) OVER w_ordered AS mintemp_trend_12,
  STDDEV("MaxTemperature") OVER w24   AS maxtemp_stddev_24,
  "MaxTemperature" - LAG("MaxTemperature", 12) OVER w_ordered AS maxtemp_trend_12,
  STDDEV("WindSpeed") OVER w24        AS wind_stddev_24,
  "WindSpeed" - LAG("WindSpeed", 12) OVER w_ordered           AS wind_trend_12,
  STDDEV("RainProbability") OVER w24  AS rainprob_stddev_24,
  "RainProbability" - LAG("RainProbability", 12) OVER w_ordered AS rainprob_trend_12
FROM daily_forecast
WINDOW
  w_ordered AS (PARTITION BY city, "Model", "Date" ORDER BY "ForecastTaken"),
  w24       AS (PARTITION BY city, "Model", "Date" ORDER BY "ForecastTaken"
                ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
"""


# ---------------------------------------------------------------------------
# Pivot / feature helpers
# ---------------------------------------------------------------------------

def _flatten_pivot_cols(df):
    df.columns = [
        f'{c[1]}_{c[0]}' if isinstance(c, tuple) and c[1] else (c[0] if isinstance(c, tuple) else c)
        for c in df.columns
    ]
    return df


def build_pivot(raw_df, models_to_pivot, require_models=None, hours_filter=None,
                index_cols=INDEX_COLS, value_cols=VALUE_COLS):
    """Pivot the raw long join into one row per (city, target, ForecastTaken).

    models_to_pivot: only these models become columns
    require_models: drop rows where any of these models lacks the temperature-like column
    hours_filter: (lo, hi) inclusive range on hours_ahead, or None
    """
    df = raw_df[raw_df['Model'].isin(models_to_pivot)]
    pivoted = (df.pivot_table(index=index_cols, columns='Model',
                              values=value_cols, aggfunc='first')
                 .reset_index())
    pivoted = _flatten_pivot_cols(pivoted)

    if require_models:
        temp_like = next((c for c in ('fc_temperature', 'fc_mintemp') if c in value_cols), value_cols[0])
        required = [f'{m}_{temp_like}' for m in require_models]
        pivoted = pivoted.dropna(subset=required)

    if hours_filter is not None:
        lo, hi = hours_filter
        pivoted = pivoted[(pivoted['hours_ahead'] >= lo) & (pivoted['hours_ahead'] <= hi)]

    return pivoted.reset_index(drop=True)


def add_time_features(df, date_col='Date', time_col='Time'):
    """Cyclical encodings for hour-of-day (if time_col present) and day-of-year."""
    if time_col in df.columns:
        target_dt = pd.to_datetime(df[date_col].astype(str) + ' ' + df[time_col].astype(str))
        hour = target_dt.dt.hour + target_dt.dt.minute / 60
        df['hour_sin'] = np.sin(2 * np.pi * hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * hour / 24)
    else:
        target_dt = pd.to_datetime(df[date_col])
    doy = target_dt.dt.dayofyear
    df['doy_sin'] = np.sin(2 * np.pi * doy / 365.25)
    df['doy_cos'] = np.cos(2 * np.pi * doy / 365.25)
    return df


def add_winddir_features(df, models):
    """sin/cos columns for each model's forecast wind direction + observed direction."""
    for m in models:
        col = f'{m}_fc_winddir'
        if col in df.columns:
            rad = np.deg2rad(df[col])
            df[f'{m}_winddir_sin'] = np.sin(rad)
            df[f'{m}_winddir_cos'] = np.cos(rad)
    if 'obs_winddir' in df.columns:
        rad = np.deg2rad(df['obs_winddir'])
        df['obs_winddir_sin'] = np.sin(rad)
        df['obs_winddir_cos'] = np.cos(rad)
    return df


def add_city_features(df, cities=None):
    """One-hot encoding for city. If `cities` is provided, ensure those columns exist
    (and order is stable) — important at predict time so feature columns match training."""
    city_oh = pd.get_dummies(df['city'], prefix='city', dtype=float)
    if cities is not None:
        for c in cities:
            col = f'city_{c}'
            if col not in city_oh.columns:
                city_oh[col] = 0.0
        city_oh = city_oh[[f'city_{c}' for c in cities]]
    return pd.concat([df, city_oh], axis=1)


def add_presence_indicators(df, models, ref_col='fc_temperature'):
    """Add per-model `_present` indicator columns based on whether `{model}_{ref_col}` is non-null."""
    for m in models:
        col = f'{m}_{ref_col}'
        if col in df.columns:
            df[f'{m}_present'] = df[col].notna().astype(float)
        else:
            df[f'{m}_present'] = 0.0
    return df


def add_rain_targets(df):
    """obs_rainprob (binary 0/100), obs_rainvol (raw)."""
    if 'obs_hourly_rain' in df.columns:
        df['obs_rainprob'] = (df['obs_hourly_rain'] > 0).astype(float) * 100
        df['obs_rainvol'] = df['obs_hourly_rain']
    elif 'obs_rain_any' in df.columns:
        df['obs_rainprob'] = (df['obs_rain_any'] > 0).astype(float) * 100
        df['obs_rainvol'] = df['obs_rain_total']
    return df


# ---------------------------------------------------------------------------
# Observation lag features
# ---------------------------------------------------------------------------

def load_observations(engine):
    """Pull all observations and dedupe on (city, hour). Returns DataFrame indexed by (city, ts)."""
    sql = """
    SELECT city,
           ("Date" + "Time")::timestamp AS obs_ts,
           "Temperature"    AS obs_temp,
           "WindSpeed" * %(mph_to_kmh)s AS obs_wind_kmh,
           "HourlyRainfall" AS obs_rain
    FROM observations
    """
    obs = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH})
    obs = obs.drop_duplicates(subset=['city', 'obs_ts'], keep='last')
    return obs.set_index(['city', 'obs_ts']).sort_index()


def add_lag_features(df, obs_all, hours_list=(1, 3, 6), unit='hours'):
    """Look up observations at ForecastTaken − N (hours or days) for each row.

    Anchoring on ForecastTaken (not the target datetime) prevents future leakage.
    """
    for h in hours_list:
        delta = pd.Timedelta(**{unit: h})
        target_ts = pd.to_datetime(df['ForecastTaken']) - delta
        keys = list(zip(df['city'], target_ts))
        lagged = obs_all.reindex(keys).reset_index(drop=True)
        prefix = f'lag{h}{unit[0]}'
        df[f'{prefix}_temp'] = lagged['obs_temp'].values
        df[f'{prefix}_wind'] = lagged['obs_wind_kmh'].values
        df[f'{prefix}_rain'] = lagged['obs_rain'].values
    return df


# ---------------------------------------------------------------------------
# Forecast-evolution
# ---------------------------------------------------------------------------

def fetch_evolution_hourly(engine, models_to_include=None):
    """Run the hourly window query and pivot models -> columns."""
    evo_raw = pd.read_sql(EVOLUTION_SQL, engine)
    if models_to_include is not None:
        evo_raw = evo_raw[evo_raw['Model'].isin(models_to_include)]
    evo_pivot = (evo_raw.pivot_table(
        index=['city', 'Date', 'Time', 'ForecastTaken'],
        columns='Model',
        values=EVO_VARS,
        aggfunc='first',
    ).reset_index())
    return _flatten_pivot_cols(evo_pivot)


def fetch_evolution_daily(engine, models_to_include=None):
    """Run the daily window query and pivot models -> columns."""
    evo_raw = pd.read_sql(EVOLUTION_DAILY_SQL, engine)
    if models_to_include is not None:
        evo_raw = evo_raw[evo_raw['Model'].isin(models_to_include)]
    evo_pivot = (evo_raw.pivot_table(
        index=['city', 'Date', 'ForecastTaken'],
        columns='Model',
        values=EVO_VARS_DAILY,
        aggfunc='first',
    ).reset_index())
    return _flatten_pivot_cols(evo_pivot)


def merge_evolution(df, evo_pivot, on=None):
    """Left-merge the forecast-evolution feature pivot onto the training matrix.

    For hourly tiers the merge keys are (city, Date, Time, ForecastTaken);
    for daily tiers, (city, Date, ForecastTaken). Auto-detected if `on` is None.
    """
    if on is None:
        on = ['city', 'Date', 'Time', 'ForecastTaken'] if 'Time' in evo_pivot.columns \
            else ['city', 'Date', 'ForecastTaken']
    return df.merge(evo_pivot, on=on, how='left')


# ---------------------------------------------------------------------------
# Training/eval helpers
# ---------------------------------------------------------------------------

def time_split(df, sort_col='ForecastTaken', test_frac=0.2):
    """Older (1-test_frac) train, newest test_frac test."""
    df = df.copy()
    df[sort_col] = pd.to_datetime(df[sort_col])
    df = df.sort_values(sort_col).reset_index(drop=True)
    cutoff_idx = int(len(df) * (1 - test_frac))
    cutoff_ts = df.loc[cutoff_idx, sort_col]
    train = df[df[sort_col] < cutoff_ts].reset_index(drop=True)
    test = df[df[sort_col] >= cutoff_ts].reset_index(drop=True)
    return train, test, cutoff_ts


def feature_columns(df, models, drop_extra=()):
    """Pick training feature columns: drop identifiers, observation targets, raw wind-dir."""
    drop = {
        'city', 'Date', 'Time', 'ForecastTaken',
        'obs_temperature', 'obs_windspeed_kmh', 'obs_winddir', 'obs_hourly_rain',
        'obs_winddir_sin', 'obs_winddir_cos',
        'obs_rainprob', 'obs_rainvol',
        'obs_mintemp', 'obs_maxtemp', 'obs_windspeed_avg', 'obs_rain_any', 'obs_rain_total',
    } | {f'{m}_fc_winddir' for m in models} | set(drop_extra)
    return [c for c in df.columns if c not in drop]


def circular_mae(true_deg, pred_deg):
    """MAE on angles in degrees, with wrap-around (max error is 180)."""
    true_deg = np.asarray(true_deg)
    pred_deg = np.asarray(pred_deg)
    diff = ((pred_deg - true_deg + 180) % 360) - 180
    return float(np.mean(np.abs(diff)))


# ---------------------------------------------------------------------------
# Prediction-time helpers (used by train_ensemble + predict_ensemble + app)
# ---------------------------------------------------------------------------

def build_source_feature_map(feature_cols, models=MODELS):
    """Bucket every feature column under the source model it belongs to.

    Any feature starting with `{Model}_` is attributed to that model; everything
    else (lag obs, time/cyclical, city dummies, presence flags) goes to 'other'.
    """
    sources = list(models) + ['other']
    mapping = {s: [] for s in sources}
    for feat in feature_cols:
        owner = 'other'
        for m in models:
            if feat.startswith(f'{m}_'):
                owner = m
                break
        mapping[owner].append(feat)
    return mapping


def aggregate_shap_by_source(shap_values, feature_names, source_feature_map):
    """Roll feature-level SHAPs (1D or 2D array) into per-source contributions.

    shap_values:    np.ndarray, shape (n_rows, n_features) or (n_features,)
    feature_names:  list aligning with the last axis of shap_values
    source_feature_map: {source: [feature_name, ...]} (output of build_source_feature_map)

    Returns: dict {source -> np.ndarray} where each array sums |SHAP| across the source's
    features, per row. The signed mean SHAP per source is also useful but |SHAP| is what
    you want for "how much did this source push the prediction".
    """
    shap_values = np.asarray(shap_values)
    if shap_values.ndim == 1:
        shap_values = shap_values[None, :]
    name_to_idx = {n: i for i, n in enumerate(feature_names)}
    out = {}
    for source, feats in source_feature_map.items():
        idx = [name_to_idx[f] for f in feats if f in name_to_idx]
        if not idx:
            out[source] = np.zeros(len(shap_values))
        else:
            out[source] = shap_values[:, idx].sum(axis=1)
    return out


def predict_quantiles(models_dict, X):
    """models_dict has keys 'p10','p50','p90'. Returns dict of arrays."""
    return {q: models_dict[q].predict(X) for q in ('p10', 'p50', 'p90')}

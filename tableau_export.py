"""Build the denormalised long-format CSV that feeds the Tableau dashboard.

Output schema (one row per (city, target_datetime, measure, source)):
    city               TEXT     — UK city name
    target_datetime    TIMESTAMP — the time the row describes
    date               DATE      — convenience for Tableau date hierarchies
    time               TIME      — NULL for daily-tier rows
    tier               TEXT     — Observed | Source-Hourly | Source-Daily |
                                  Ensemble-Hourly | Ensemble-Daily | Efficacy
    measure             TEXT     — Temperature | WindSpeed | WindDirection |
                                  RainProbability | RainVolume |
                                  MinTemperature | MaxTemperature | HourlyRainfall
    source             TEXT     — MetOffice | OpenMeteo-ECMWF | OpenMeteo-GFSHRRR |
                                  AccuWeather | Ensemble | Observation
    value              REAL     — point estimate (p50 for Ensemble; raw for Source/Observation)
    p10                REAL     — only populated for tier in (Ensemble-Hourly, Ensemble-Daily, Efficacy with source='Ensemble')
    p90                REAL     — same as p10
    forecast_taken     TIMESTAMP — when the forecast was issued; NULL for Observation
    hours_ahead        REAL     — lead time at issue; NULL for Observation
    error              REAL     — value − observation, populated on Efficacy rows when an obs exists;
                                  NULL on forward-looking rows and Observation rows

Reads only from the four production tables. Idempotent and read-only.
"""

from __future__ import annotations

import logging as log
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

MPH_TO_KMH = 1.609344
OUTPUT_PATH = Path('tableau_export.csv')

OBS_LOOKBACK_HOURS = 168       # 1 week of past observations
EFFICACY_LOOKBACK_HOURS = 168  # 1 week of past forecasts compared to obs
LEAD_HOURLY_DEFAULT = 6        # hourly per-source comparison: latest forecast >= 6h ahead
LEAD_DAILY_DEFAULT = 24        # daily per-source comparison: latest forecast >= 24h ahead

COLUMNS = ['city', 'target_datetime', 'date', 'time',
           'tier', 'measure', 'source', 'value',
           'p10', 'p90', 'forecast_taken', 'hours_ahead', 'error']


def _empty():
    return pd.DataFrame(columns=COLUMNS)


def _shape(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all COLUMNS exist (filling missing with None) and reorder."""
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[COLUMNS]


# Pull last 7 days of hourly observation data

def _observations(engine) -> pd.DataFrame:
    sql = """
    SELECT city,
           ("Date" + "Time") AS target_datetime,
           "Date" AS date,
           "Time" AS time,
           "Temperature"    AS temperature,
           "WindSpeed" * %(mph_to_kmh)s AS windspeed,
           "HourlyRainfall" AS rainfall
    FROM observations
    WHERE ("Date" + "Time")::timestamp > NOW() - (%(h)s::text || ' hours')::interval
    """
    raw = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH, 'h': str(OBS_LOOKBACK_HOURS)})
    long_format_df = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time'],
        value_vars=['temperature', 'windspeed', 'rainfall'],
        var_name='measure', value_name='value')
    long_format_df['measure'] = long_format_df['measure'].map({'temperature': 'Temperature','windspeed': 'WindSpeed','rainfall': 'HourlyRainfall'})
    long_format_df['tier'] = 'Observed'
    long_format_df['source'] = 'Observation'
    return _shape(long_format_df)


# ---------------------------------------------------------------------------
# Source forecasts — latest issue per (city, source)  [forward-looking only]
# ---------------------------------------------------------------------------

def _source_hourly_latest(engine) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT city, "Model", MAX("ForecastTaken") AS ft
      FROM hourly_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '6 hours'
      GROUP BY city, "Model"
    )
    SELECT hourly_forecast.city,
           ("Date" + "Time") AS target_datetime,
           hourly_forecast."Date" AS date, hourly_forecast."Time" AS time,
           hourly_forecast."Model" AS source,
           hourly_forecast."ForecastTaken" AS forecast_taken,
           EXTRACT(EPOCH FROM (("Date" + "Time") - "ForecastTaken")) / 3600 AS hours_ahead,
           "Temperature" AS temperature,
           CASE WHEN hourly_forecast."Model" = 'MetOffice' THEN "WindSpeed" * %(mph_to_kmh)s
                ELSE "WindSpeed" END AS windspeed,
           "WindDirection"   AS winddirection,
           "RainProbability" AS rainprobability,
           "RainVolume"      AS rainvolume
    FROM hourly_forecast
    JOIN latest
      ON hourly_forecast.city = latest.city AND hourly_forecast."Model" = latest."Model" AND hourly_forecast."ForecastTaken" = latest.ft
    WHERE ("Date" + "Time") > NOW() - INTERVAL '2 hours'
    """
    raw = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH})
    if raw.empty:
        return _empty()
    long_format_df = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['temperature', 'windspeed', 'winddirection', 'rainprobability', 'rainvolume'],
        var_name='measure', value_name='value',
    )
    long_format_df['measure'] = long_format_df['measure'].map({'temperature': 'Temperature',
                                           'windspeed': 'WindSpeed',
                                           'winddirection': 'WindDirection',
                                           'rainprobability': 'RainProbability',
                                           'rainvolume': 'RainVolume'})
    long_format_df['tier'] = 'Source-Hourly'
    return _shape(long_format_df)


def _source_daily_latest(engine) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT city, "Model", MAX("ForecastTaken") AS ft
      FROM daily_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '12 hours'
      GROUP BY city, "Model"
    )
    SELECT daily_forecast.city,
           daily_forecast."Date" AS target_datetime,
           daily_forecast."Date" AS date,
           NULL::time AS time,
           daily_forecast."Model" AS source,
           daily_forecast."ForecastTaken" AS forecast_taken,
           (daily_forecast."Date" - daily_forecast."ForecastTaken"::date) * 24.0 AS hours_ahead,
           "MinTemperature" AS mintemperature,
           "MaxTemperature" AS maxtemperature,
           CASE WHEN daily_forecast."Model" = 'MetOffice' THEN "WindSpeed" * %(mph_to_kmh)s
                ELSE "WindSpeed" END AS windspeed,
           "WindDirection"   AS winddirection,
           "RainProbability" AS rainprobability,
           "RainVolume"      AS rainvolume
    FROM daily_forecast JOIN latest
      ON daily_forecast.city=latest.city AND daily_forecast."Model"=latest."Model" AND daily_forecast."ForecastTaken"=latest.ft
    WHERE daily_forecast."Date" >= CURRENT_DATE
    """
    raw = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH})
    if raw.empty:
        return _empty()
    
    long_format_df = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['mintemperature', 'maxtemperature', 'windspeed',
                    'winddirection', 'rainprobability', 'rainvolume'],
        var_name='measure', value_name='value')
    
    long_format_df['measure'] = long_format_df['measure'].map({'mintemperature': 'MinTemperature',
                                           'maxtemperature': 'MaxTemperature',
                                           'windspeed': 'WindSpeed',
                                           'winddirection': 'WindDirection',
                                           'rainprobability': 'RainProbability',
                                           'rainvolume': 'RainVolume'})
    long_format_df['tier'] = 'Source-Daily'
    return _shape(long_format_df)


# ---------------------------------------------------------------------------
# Ensemble forecasts — latest issue per city [forward-looking only]
# ---------------------------------------------------------------------------

def _ensemble_latest(engine) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT city, measure, "Time" IS NULL AS is_daily, MAX("ForecastTaken") AS ft
      FROM ensemble_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '6 hours'
      GROUP BY city, measure, "Time" IS NULL
    ),
    ef_future AS (
      SELECT ef.*
      FROM ensemble_forecast ef JOIN latest l
        ON ef.city=l.city AND ef.measure=l.measure
       AND (ef."Time" IS NULL)=l.is_daily AND ef."ForecastTaken"=l.ft
      WHERE (
        ef."Time" IS NULL AND ef."Date" >= CURRENT_DATE
      ) OR (
        ef."Time" IS NOT NULL AND (ef."Date" + ef."Time")::timestamp > NOW()
      )
    )
    SELECT city,
           CASE WHEN "Time" IS NULL THEN "Date"::timestamp
                ELSE ("Date" + "Time")::timestamp END AS target_datetime,
           "Date" AS date, "Time" AS time, measure,
           "ForecastTaken" AS forecast_taken,
           CASE WHEN "Time" IS NULL
                THEN ("Date" - "ForecastTaken"::date) * 24.0
                ELSE EXTRACT(EPOCH FROM (("Date" + "Time") - "ForecastTaken")) / 3600
           END AS hours_ahead,
           tier, p50 AS value, p10, p90
    FROM ef_future
    """
    raw = pd.read_sql(sql, engine)
    if raw.empty:
        return _empty()
    raw['source'] = 'Ensemble'
    raw['tier'] = raw['tier'].map({
        'Hourly-Short':  'Ensemble-Hourly',
        'Hourly-Long':   'Ensemble-Hourly',
        'Daily':         'Ensemble-Daily',
    })
    return _shape(raw)


# ---------------------------------------------------------------------------
# Efficacy — past Ensemble + per-source forecasts joined to obs, with error
# ---------------------------------------------------------------------------

def _hourly_obs_long(engine, lookback_hours: int = EFFICACY_LOOKBACK_HOURS) -> pd.DataFrame:
    """Hourly observations in long format keyed on (city, target_datetime, measure).

    Three obs metrics (Temperature, WindSpeed, RainVolume) plus a synthesised
    RainProbability obs (100 if it rained, 0 otherwise). WindDirection
    observations are stored as compass strings and need a CTE join; we skip
    WindDirection efficacy for now.
    """
    sql = """
    WITH base AS (
      SELECT city, ("Date" + "Time")::timestamp AS target_datetime,
             "Temperature" AS temp,
             "WindSpeed" * %(mph)s AS wind,
             "HourlyRainfall" AS rain
      FROM observations
      WHERE ("Date" + "Time")::timestamp > NOW() - (%(h)s::text || ' hours')::interval
        AND ("Date" + "Time")::timestamp <= NOW()
    )
    SELECT city, target_datetime, 'Temperature' AS measure, temp AS obs_value FROM base WHERE temp IS NOT NULL
    UNION ALL
    SELECT city, target_datetime, 'WindSpeed', wind FROM base WHERE wind IS NOT NULL
    UNION ALL
    SELECT city, target_datetime, 'RainVolume', rain FROM base WHERE rain IS NOT NULL
    UNION ALL
    SELECT city, target_datetime, 'RainProbability',
           CASE WHEN rain > 0 THEN 100.0 ELSE 0.0 END
    FROM base WHERE rain IS NOT NULL
    """
    return pd.read_sql(sql, engine, params={'mph': MPH_TO_KMH, 'h': str(lookback_hours)})


def _daily_obs_long(engine, lookback_days: int = 8) -> pd.DataFrame:
    """Daily observation aggregates in long format keyed on (city, date, measure)."""
    sql = """
    WITH daily_obs AS (
      SELECT city, "Date" AS date,
             MIN("Temperature") AS mintemp,
             MAX("Temperature") AS maxtemp,
             AVG("WindSpeed") * %(mph)s AS windspeed,
             SUM("HourlyRainfall") AS rainvol,
             MAX("HourlyRainfall") > 0 AS rainy
      FROM observations
      WHERE "Date" > CURRENT_DATE - (%(d)s::text || ' days')::interval
        AND "Date" <= CURRENT_DATE
      GROUP BY city, "Date"
    )
    SELECT city, date, 'MinTemperature' AS measure, mintemp AS obs_value FROM daily_obs WHERE mintemp IS NOT NULL
    UNION ALL
    SELECT city, date, 'MaxTemperature', maxtemp FROM daily_obs WHERE maxtemp IS NOT NULL
    UNION ALL
    SELECT city, date, 'WindSpeed', windspeed FROM daily_obs WHERE windspeed IS NOT NULL
    UNION ALL
    SELECT city, date, 'RainVolume', rainvol FROM daily_obs WHERE rainvol IS NOT NULL
    UNION ALL
    SELECT city, date, 'RainProbability', CASE WHEN rainy THEN 100.0 ELSE 0.0 END
    FROM daily_obs WHERE rainy IS NOT NULL
    """
    return pd.read_sql(sql, engine, params={'mph': MPH_TO_KMH, 'd': str(lookback_days)})


def _past_ensemble_hourly(engine, lead_hourly: int = LEAD_HOURLY_DEFAULT) -> pd.DataFrame:
    """Past Ensemble hourly predictions at >= lead_hourly hours ahead.

    DISTINCT ON picks the latest qualifying ForecastTaken per
    (city, target_datetime, measure) so the comparison to per-source efficacy
    is apples-to-apples (both filtered to the same lead time)."""
    sql = """
    SELECT DISTINCT ON (city, target_datetime, measure)
           city,
           ("Date" + "Time")::timestamp AS target_datetime,
           "Date" AS date, "Time" AS time,
           measure, p50 AS value, p10, p90,
           "ForecastTaken" AS forecast_taken,
           EXTRACT(EPOCH FROM (("Date" + "Time") - "ForecastTaken")) / 3600 AS hours_ahead
    FROM ensemble_forecast
    WHERE "Time" IS NOT NULL
      AND ("Date" + "Time")::timestamp > NOW() - (%(h)s::text || ' hours')::interval
      AND ("Date" + "Time")::timestamp <= NOW()
      AND "ForecastTaken" <= ("Date" + "Time") - (%(lead)s::text || ' hours')::interval
    ORDER BY city, target_datetime, measure, "ForecastTaken" DESC
    """
    df = pd.read_sql(sql, engine, params={
        'h': str(EFFICACY_LOOKBACK_HOURS),
        'lead': str(lead_hourly),
    })
    if df.empty:
        return _empty()
    df['source'] = 'Ensemble'
    df['tier'] = 'Efficacy'
    return _shape(df)


def _past_ensemble_daily(engine, lead_daily: int = LEAD_DAILY_DEFAULT) -> pd.DataFrame:
    sql = """
    SELECT DISTINCT ON (city, "Date", measure)
           city,
           "Date"::timestamp AS target_datetime,
           "Date" AS date, NULL::time AS time,
           measure, p50 AS value, p10, p90,
           "ForecastTaken" AS forecast_taken,
           ("Date" - "ForecastTaken"::date) * 24.0 AS hours_ahead
    FROM ensemble_forecast
    WHERE "Time" IS NULL
      AND "Date" > CURRENT_DATE - (%(d)s::text || ' days')::interval
      AND "Date" <= CURRENT_DATE
      AND "ForecastTaken" <= "Date"::timestamp - (%(lead)s::text || ' hours')::interval
    ORDER BY city, "Date", measure, "ForecastTaken" DESC
    """
    df = pd.read_sql(sql, engine, params={
        'd': '8',
        'lead': str(lead_daily),
    })
    if df.empty:
        return _empty()
    df['source'] = 'Ensemble'
    df['tier'] = 'Efficacy'
    return _shape(df)


def _past_source_hourly(engine, lead_hourly: int = LEAD_HOURLY_DEFAULT) -> pd.DataFrame:
    """Per-source hourly forecasts at >= lead_hourly hours ahead, last 7d of targets.
    DISTINCT ON picks the most recent ForecastTaken satisfying the lead constraint."""
    sql = """
    SELECT DISTINCT ON (hf.city, target_datetime, hf."Model")
        hf.city,
        (hf."Date" + hf."Time")::timestamp AS target_datetime,
        hf."Date" AS date, hf."Time" AS time,
        hf."Model" AS source,
        hf."ForecastTaken" AS forecast_taken,
        EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 AS hours_ahead,
        hf."Temperature" AS temperature,
        CASE WHEN hf."Model" = 'MetOffice' THEN hf."WindSpeed" * %(mph)s
             ELSE hf."WindSpeed" END AS windspeed,
        hf."WindDirection"   AS winddirection,
        hf."RainProbability" AS rainprobability,
        hf."RainVolume"      AS rainvolume
    FROM hourly_forecast hf
    WHERE (hf."Date" + hf."Time")::timestamp > NOW() - (%(h)s::text || ' hours')::interval
      AND (hf."Date" + hf."Time")::timestamp <= NOW()
      AND hf."ForecastTaken" <= (hf."Date" + hf."Time") - (%(lead)s::text || ' hours')::interval
    ORDER BY hf.city, target_datetime, hf."Model", hf."ForecastTaken" DESC
    """
    raw = pd.read_sql(sql, engine, params={
        'mph': MPH_TO_KMH,
        'h': str(EFFICACY_LOOKBACK_HOURS),
        'lead': str(lead_hourly),
    })
    if raw.empty:
        return _empty()
    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source',
                 'forecast_taken', 'hours_ahead'],
        value_vars=['temperature', 'windspeed', 'winddirection',
                    'rainprobability', 'rainvolume'],
        var_name='measure', value_name='value',
    )
    long_['measure'] = long_['measure'].map({'temperature': 'Temperature',
                                           'windspeed': 'WindSpeed',
                                           'winddirection': 'WindDirection',
                                           'rainprobability': 'RainProbability',
                                           'rainvolume': 'RainVolume'})
    long_['tier'] = 'Efficacy'
    return _shape(long_)


def _past_source_daily(engine, lead_daily: int = LEAD_DAILY_DEFAULT) -> pd.DataFrame:
    sql = """
    SELECT DISTINCT ON (df.city, df."Date", df."Model")
        df.city,
        df."Date"::timestamp AS target_datetime,
        df."Date" AS date,
        NULL::time AS time,
        df."Model" AS source,
        df."ForecastTaken" AS forecast_taken,
        (df."Date" - df."ForecastTaken"::date) * 24.0 AS hours_ahead,
        "MinTemperature" AS mintemperature,
        "MaxTemperature" AS maxtemperature,
        CASE WHEN df."Model" = 'MetOffice' THEN "WindSpeed" * %(mph)s
             ELSE "WindSpeed" END AS windspeed,
        "WindDirection"   AS winddirection,
        "RainProbability" AS rainprobability,
        "RainVolume"      AS rainvolume
    FROM daily_forecast df
    WHERE df."Date" > CURRENT_DATE - (%(d)s::text || ' days')::interval
      AND df."Date" <= CURRENT_DATE
      AND df."ForecastTaken" <= df."Date"::timestamp - (%(lead)s::text || ' hours')::interval
    ORDER BY df.city, df."Date", df."Model", df."ForecastTaken" DESC
    """
    raw = pd.read_sql(sql, engine, params={
        'mph': MPH_TO_KMH,
        'd': '8',
        'lead': str(lead_daily),
    })
    if raw.empty:
        return _empty()
    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source',
                 'forecast_taken', 'hours_ahead'],
        value_vars=['mintemperature', 'maxtemperature', 'windspeed',
                    'winddirection', 'rainprobability', 'rainvolume'],
        var_name='measure', value_name='value',
    )
    long_['measure'] = long_['measure'].map({'mintemperature': 'MinTemperature',
                                           'maxtemperature': 'MaxTemperature',
                                           'windspeed': 'WindSpeed',
                                           'winddirection': 'WindDirection',
                                           'rainprobability': 'RainProbability',
                                           'rainvolume': 'RainVolume'})
    long_['tier'] = 'Efficacy'
    return _shape(long_)


def _efficacy_with_sources(engine,
                           lead_hourly: int = LEAD_HOURLY_DEFAULT,
                           lead_daily: int = LEAD_DAILY_DEFAULT) -> pd.DataFrame:
    """Past Ensemble + per-source forecasts whose measure time has passed, joined
    to observations to produce a signed error column.

    For hourly: error keys on (city, target_datetime, measure).
    For daily:  error keys on (city, date, measure).
    """
    # Hourly slice
    h_parts = [_past_ensemble_hourly(engine, lead_hourly),
               _past_source_hourly(engine, lead_hourly)]
    h_parts = [p for p in h_parts if not p.empty]
    hourly_eff = pd.concat(h_parts, ignore_index=True) if h_parts else _empty()
    if not hourly_eff.empty:
        obs_h = _hourly_obs_long(engine)
        if not obs_h.empty:
            hourly_eff['target_datetime'] = pd.to_datetime(hourly_eff['target_datetime'])
            obs_h['target_datetime'] = pd.to_datetime(obs_h['target_datetime'])
            hourly_eff = hourly_eff.merge(
                obs_h, on=['city', 'target_datetime', 'measure'], how='left',
            )
            hourly_eff['error'] = hourly_eff['value'] - hourly_eff['obs_value']
            hourly_eff = hourly_eff.drop(columns=['obs_value'])

    # Daily slice
    d_parts = [_past_ensemble_daily(engine, lead_daily),
               _past_source_daily(engine, lead_daily)]
    d_parts = [p for p in d_parts if not p.empty]
    daily_eff = pd.concat(d_parts, ignore_index=True) if d_parts else _empty()
    if not daily_eff.empty:
        obs_d = _daily_obs_long(engine)
        if not obs_d.empty:
            daily_eff['date'] = pd.to_datetime(daily_eff['date']).dt.date
            obs_d['date'] = pd.to_datetime(obs_d['date']).dt.date
            daily_eff = daily_eff.merge(
                obs_d, on=['city', 'date', 'measure'], how='left',
            )
            daily_eff['error'] = daily_eff['value'] - daily_eff['obs_value']
            daily_eff = daily_eff.drop(columns=['obs_value'])

    out_parts = [p for p in (hourly_eff, daily_eff) if not p.empty]
    return pd.concat(out_parts, ignore_index=True) if out_parts else _empty()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def build_parts(engine) -> dict:
    """Build the individual slices that make up the long DataFrame.

    Returns a dict with the assembled `long` DataFrame plus the `efficacy`
    slice exposed separately so downstream code (metrics_export) can roll
    it up without recomputing.
    """
    efficacy = _efficacy_with_sources(engine)
    parts = [
        _observations(engine),
        _source_hourly_latest(engine),
        _source_daily_latest(engine),
        _ensemble_latest(engine),
        efficacy,
    ]
    parts = [p for p in parts if not p.empty]
    long_df = pd.concat(parts, ignore_index=True) if parts else _empty()
    long_df = long_df.dropna(subset=['value'])
    return {'long': _shape(long_df), 'efficacy': efficacy}


def build_long_dataframe(engine) -> pd.DataFrame:
    return build_parts(engine)['long']


def write_csv(engine, output_path: Path = OUTPUT_PATH) -> Path:
    df = build_long_dataframe(engine)
    df.to_csv(output_path, index=False)
    log.info(f'wrote {len(df):,} rows to {output_path}')
    return output_path


def main():
    from dotenv import load_dotenv
    load_dotenv()
    log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    engine = create_engine(os.environ['DATABASE_URL'])
    write_csv(engine)


if __name__ == '__main__':
    main()

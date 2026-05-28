"""Build the denormalised long-format CSV that feeds the Tableau dashboard.

Output schema (one row per (city, target_datetime, target, source)):
    city               TEXT     — UK city name
    target_datetime    TIMESTAMP — the time the row describes
    date               DATE      — convenience for Tableau date hierarchies
    time               TIME      — NULL for daily-tier rows
    tier               TEXT     — Observed | Source-Hourly | Source-Daily | Ensemble-Hourly | Ensemble-Daily | Efficacy
    target             TEXT     — Temperature | WindSpeed | WindDirection | RainProbability | RainVolume |
                                  MinTemperature | MaxTemperature | HourlyRainfall
    source             TEXT     — MetOffice | OpenMeteo-ECMWF | OpenMeteo-GFSHRRR | AccuWeather |
                                  Ensemble | Observation
    value              REAL     — point estimate (p50 for Ensemble; raw for Source/Observation)
    p10                REAL     — only populated for tier in (Ensemble-Hourly, Ensemble-Daily)
    p90                REAL     — same as p10
    forecast_taken     TIMESTAMP — when the forecast was issued; NULL for Observation
    hours_ahead        REAL     — lead time at issue; NULL for Observation

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
EFFICACY_LOOKBACK_HOURS = 168  # 1 week of past ensemble predictions to compare to obs

COLUMNS = ['city', 'target_datetime', 'date', 'time',
           'tier', 'target', 'source', 'value',
           'p10', 'p90', 'forecast_taken', 'hours_ahead']


def _empty():
    return pd.DataFrame(columns=COLUMNS)


# ---------------------------------------------------------------------------
# Observations (last 72h)
# ---------------------------------------------------------------------------

def _observations(engine) -> pd.DataFrame:
    sql = """
    SELECT city,
           ("Date" + "Time")::timestamp AS target_datetime,
           "Date" AS date,
           "Time" AS time,
           "Temperature"    AS temperature,
           "WindSpeed" * %(mph_to_kmh)s AS windspeed,
           "HourlyRainfall" AS rainfall
    FROM observations
    WHERE ("Date" + "Time")::timestamp > NOW() - (%(h)s::text || ' hours')::interval
    """
    raw = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH, 'h': str(OBS_LOOKBACK_HOURS)})
    if raw.empty:
        return _empty()
    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time'],
        value_vars=['temperature', 'windspeed', 'rainfall'],
        var_name='target', value_name='value',
    )
    long_['target'] = long_['target'].map({'temperature': 'Temperature',
                                           'windspeed': 'WindSpeed',
                                           'rainfall': 'HourlyRainfall'})
    long_['tier'] = 'Observed'
    long_['source'] = 'Observation'
    long_['p10'] = None
    long_['p90'] = None
    long_['forecast_taken'] = None
    long_['hours_ahead'] = None
    return long_[COLUMNS]


# ---------------------------------------------------------------------------
# Source forecasts — latest issue per (city, source)
# ---------------------------------------------------------------------------

def _source_hourly_latest(engine) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT city, "Model", MAX("ForecastTaken") AS ft
      FROM hourly_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '6 hours'
      GROUP BY city, "Model"
    )
    SELECT hf.city,
           ("Date" + "Time")::timestamp AS target_datetime,
           hf."Date" AS date, hf."Time" AS time,
           hf."Model" AS source,
           hf."ForecastTaken" AS forecast_taken,
           EXTRACT(EPOCH FROM (("Date" + "Time") - "ForecastTaken")) / 3600 AS hours_ahead,
           "Temperature" AS temperature,
           CASE WHEN hf."Model" = 'MetOffice' THEN "WindSpeed" * %(mph_to_kmh)s
                ELSE "WindSpeed" END AS windspeed,
           "WindDirection"   AS winddirection,
           "RainProbability" AS rainprobability,
           "RainVolume"      AS rainvolume
    FROM hourly_forecast hf JOIN latest l
      ON hf.city=l.city AND hf."Model"=l."Model" AND hf."ForecastTaken"=l.ft
    WHERE ("Date" + "Time")::timestamp > NOW() - INTERVAL '2 hours'
    """
    raw = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH})
    if raw.empty:
        return _empty()
    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['temperature', 'windspeed', 'winddirection', 'rainprobability', 'rainvolume'],
        var_name='target', value_name='value',
    )
    long_['target'] = long_['target'].map({'temperature': 'Temperature',
                                           'windspeed': 'WindSpeed',
                                           'winddirection': 'WindDirection',
                                           'rainprobability': 'RainProbability',
                                           'rainvolume': 'RainVolume'})
    long_['tier'] = 'Source-Hourly'
    long_['p10'] = None
    long_['p90'] = None
    return long_[COLUMNS]


def _source_daily_latest(engine) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT city, "Model", MAX("ForecastTaken") AS ft
      FROM daily_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '12 hours'
      GROUP BY city, "Model"
    )
    SELECT df.city,
           df."Date"::timestamp AS target_datetime,
           df."Date" AS date,
           NULL::time AS time,
           df."Model" AS source,
           df."ForecastTaken" AS forecast_taken,
           (df."Date" - df."ForecastTaken"::date) * 24.0 AS hours_ahead,
           "MinTemperature" AS mintemperature,
           "MaxTemperature" AS maxtemperature,
           CASE WHEN df."Model" = 'MetOffice' THEN "WindSpeed" * %(mph_to_kmh)s
                ELSE "WindSpeed" END AS windspeed,
           "WindDirection"   AS winddirection,
           "RainProbability" AS rainprobability,
           "RainVolume"      AS rainvolume
    FROM daily_forecast df JOIN latest l
      ON df.city=l.city AND df."Model"=l."Model" AND df."ForecastTaken"=l.ft
    WHERE df."Date" >= CURRENT_DATE
    """
    raw = pd.read_sql(sql, engine, params={'mph_to_kmh': MPH_TO_KMH})
    if raw.empty:
        return _empty()
    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['mintemperature', 'maxtemperature', 'windspeed',
                    'winddirection', 'rainprobability', 'rainvolume'],
        var_name='target', value_name='value',
    )
    long_['target'] = long_['target'].map({'mintemperature': 'MinTemperature',
                                           'maxtemperature': 'MaxTemperature',
                                           'windspeed': 'WindSpeed',
                                           'winddirection': 'WindDirection',
                                           'rainprobability': 'RainProbability',
                                           'rainvolume': 'RainVolume'})
    long_['tier'] = 'Source-Daily'
    long_['p10'] = None
    long_['p90'] = None
    return long_[COLUMNS]


# ---------------------------------------------------------------------------
# Ensemble forecasts — latest issue per city
# ---------------------------------------------------------------------------

def _ensemble_latest(engine) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT city, target, "Time" IS NULL AS is_daily, MAX("ForecastTaken") AS ft
      FROM ensemble_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '6 hours'
      GROUP BY city, target, "Time" IS NULL
    )
    SELECT ef.city,
           CASE WHEN ef."Time" IS NULL THEN ef."Date"::timestamp
                ELSE (ef."Date" + ef."Time")::timestamp END AS target_datetime,
           ef."Date" AS date,
           ef."Time" AS time,
           ef.target,
           ef."ForecastTaken" AS forecast_taken,
           CASE WHEN ef."Time" IS NULL
                THEN (ef."Date" - ef."ForecastTaken"::date) * 24.0
                ELSE EXTRACT(EPOCH FROM ((ef."Date" + ef."Time") - ef."ForecastTaken")) / 3600
           END AS hours_ahead,
           ef.tier,
           ef.p50 AS value,
           ef.p10,
           ef.p90
    FROM ensemble_forecast ef
    JOIN latest l
      ON ef.city = l.city AND ef.target = l.target
     AND (ef."Time" IS NULL) = l.is_daily
     AND ef."ForecastTaken" = l.ft
    """
    raw = pd.read_sql(sql, engine)
    if raw.empty:
        return _empty()
    raw['source'] = 'Ensemble'
    # Rename Hourly-Short / Hourly-Long / Daily tier labels to the export vocabulary
    raw['tier'] = raw['tier'].map({
        'Hourly-Short':  'Ensemble-Hourly',
        'Hourly-Long':   'Ensemble-Hourly',
        'Daily':         'Ensemble-Daily',
    })
    return raw[COLUMNS]


# ---------------------------------------------------------------------------
# Efficacy panel — past ensemble predictions paired with observation hours
# ---------------------------------------------------------------------------

def _efficacy(engine) -> pd.DataFrame:
    sql = """
    SELECT ef.city,
           (ef."Date" + ef."Time")::timestamp AS target_datetime,
           ef."Date" AS date,
           ef."Time" AS time,
           ef.target,
           ef."ForecastTaken" AS forecast_taken,
           EXTRACT(EPOCH FROM ((ef."Date" + ef."Time") - ef."ForecastTaken")) / 3600 AS hours_ahead,
           ef.p10, ef.p50 AS value, ef.p90
    FROM ensemble_forecast ef
    WHERE ef."Time" IS NOT NULL
      AND (ef."Date" + ef."Time")::timestamp > NOW() - (%(h)s::text || ' hours')::interval
      AND (ef."Date" + ef."Time")::timestamp <= NOW()
    """
    raw = pd.read_sql(sql, engine, params={'h': str(EFFICACY_LOOKBACK_HOURS)})
    if raw.empty:
        return _empty()
    raw['source'] = 'Ensemble'
    raw['tier'] = 'Efficacy'
    return raw[COLUMNS]


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def build_long_dataframe(engine) -> pd.DataFrame:
    parts = [
        _observations(engine),
        _source_hourly_latest(engine),
        _source_daily_latest(engine),
        _ensemble_latest(engine),
        _efficacy(engine),
    ]
    df = pd.concat([p for p in parts if not p.empty], ignore_index=True)
    df = df.dropna(subset=['value'])
    return df[COLUMNS]


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

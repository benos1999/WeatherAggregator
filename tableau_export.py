# Fetches recent forecast and observation data from Postgres database hosted on Railway (constructed from weather_api_export), shapes it into a long/tidy format for Tableau, and writes to CSV. CSV then fed to Google Sheets (via sheets_export.py) for Tableau to pull in.

import logging as log
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

MPH_TO_KMH = 1.609344
OUTPUT_PATH = Path('tableau_export.csv')

# tuning knobs — change these to widen the data window or shift the per-source comparison lead
obs_lookback_hours = 672        # 4 weeks of obs for the live tab
efficacy_lookback_hours = 672   # 4 weeks of past forecasts for the efficacy join
daily_lookback_days = 28
lead_hourly_default = 12        # latest forecast >= 12h ahead for the per-source comparison
lead_daily_default = 24

def _shape(df: pd.DataFrame):
    COLUMNS = ['city', 'target_datetime', 'date', 'time',
           'tier', 'measure', 'source', 'value',
           'p10', 'p90', 'forecast_taken', 'hours_ahead', 'error']
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[COLUMNS]

# Pull last 7 days of hourly observation data

def _observations(engine):
    sql = f"""
    SELECT city,
           ("Date" + "Time") AS target_datetime,
           "Date" AS date,
           "Time" AS time,
           "Temperature" AS temperature,
           "WindSpeed" * {MPH_TO_KMH} AS windspeed,
           "HourlyRainfall" AS rainfall
    FROM observations
    WHERE ("Date" + "Time")::timestamp > NOW() - INTERVAL '{obs_lookback_hours} hours'
    """
    raw = pd.read_sql(sql, engine)
    long_format_df = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time'],
        value_vars=['temperature', 'windspeed', 'rainfall'],
        var_name='measure', value_name='value')
    long_format_df['measure'] = long_format_df['measure'].map({'temperature': 'Temperature','windspeed': 'WindSpeed','rainfall': 'HourlyRainfall'})
    long_format_df['tier'] = 'Observed'
    long_format_df['source'] = 'Observation'
    return _shape(long_format_df)

# Pull hourly forecasts recently made by sources

def _source_hourly_latest(engine):
    sql = f"""
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
           CASE WHEN hourly_forecast."Model" = 'MetOffice' THEN "WindSpeed" * {MPH_TO_KMH}
                ELSE "WindSpeed" END AS windspeed,
           "WindDirection" AS winddirection,
           "RainProbability" AS rainprobability,
           "RainVolume" AS rainvolume
    FROM hourly_forecast
    JOIN latest
      ON hourly_forecast.city = latest.city AND hourly_forecast."Model" = latest."Model" AND hourly_forecast."ForecastTaken" = latest.ft
    WHERE ("Date" + "Time") > NOW() - INTERVAL '2 hours'
    """
    raw = pd.read_sql(sql, engine)
   
    long_format_df = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['temperature', 'windspeed', 'winddirection', 'rainprobability', 'rainvolume'],
        var_name='measure', value_name='value',
    )
    long_format_df['measure'] = long_format_df['measure'].map({'temperature': 'Temperature','windspeed': 'WindSpeed','winddirection': 'WindDirection', 'rainprobability': 'RainProbability', 'rainvolume': 'RainVolume'})
    long_format_df['tier'] = 'Source-Hourly'
    return _shape(long_format_df)

# Pulls forecasts made in the last day, (daily only)

def _source_daily_latest(engine):
    sql = f"""
    WITH latest AS (
      SELECT city, "Model", MAX("ForecastTaken") AS ft
      FROM daily_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '12 hours'
      GROUP BY city, "Model"
    )
    SELECT daily_forecast.city,
           daily_forecast."Date" AS target_datetime,
           daily_forecast."Date" AS date,
           NULL AS time,
           daily_forecast."Model" AS source,
           daily_forecast."ForecastTaken" AS forecast_taken,
           (daily_forecast."Date" - daily_forecast."ForecastTaken"::date) * 24.0 AS hours_ahead,
           "MinTemperature" AS mintemperature,
           "MaxTemperature" AS maxtemperature,
           CASE WHEN daily_forecast."Model" = 'MetOffice' THEN "WindSpeed" * {MPH_TO_KMH}
                ELSE "WindSpeed" END AS windspeed,
           "WindDirection" AS winddirection,
           "RainProbability" AS rainprobability,
           "RainVolume" AS rainvolume
    FROM daily_forecast JOIN latest
      ON daily_forecast.city=latest.city AND daily_forecast."Model"=latest."Model" AND daily_forecast."ForecastTaken"=latest.ft
    WHERE daily_forecast."Date" >= CURRENT_DATE
    """
    raw = pd.read_sql(sql, engine)
    
    long_format_df = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['mintemperature', 'maxtemperature', 'windspeed',
                    'winddirection', 'rainprobability', 'rainvolume'],
        var_name='measure', value_name='value')
    
    long_format_df['measure'] = long_format_df['measure'].map({'mintemperature': 'MinTemperature','maxtemperature': 'MaxTemperature','windspeed': 'WindSpeed','winddirection': 'WindDirection','rainprobability': 'RainProbability','rainvolume': 'RainVolume'})
    long_format_df['tier'] = 'Source-Daily'
    return _shape(long_format_df)


# Pulls all forecasts from ensemble model made in the last day, both hourly and daily (distinguished by time IS NULL), no lead-time filtering

def _ensemble_latest(engine):
    sql = """
    WITH latest AS (
      SELECT city, measure, "Time" IS NULL AS is_daily, MAX("ForecastTaken") AS ft
      FROM ensemble_forecast
      WHERE "ForecastTaken" > NOW() - INTERVAL '6 hours'
      GROUP BY city, measure, "Time" IS NULL),
    ensemble_future AS (
      SELECT ensemble_forecast.*
      FROM ensemble_forecast JOIN latest l
        ON ensemble_forecast.city=l.city AND ensemble_forecast.measure=l.measure
       AND (ensemble_forecast."Time" IS NULL)=l.is_daily AND ensemble_forecast."ForecastTaken"=l.ft
      WHERE (
        ensemble_forecast."Time" IS NULL AND ensemble_forecast."Date" >= CURRENT_DATE)
        OR (ensemble_forecast."Time" IS NOT NULL AND (ensemble_forecast."Date" + ensemble_forecast."Time")::timestamp > NOW()))
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
    FROM ensemble_future
    """
    raw = pd.read_sql(sql, engine)
   
    raw['source'] = 'Ensemble'
    raw['tier'] = raw['tier'].map({'Hourly-Short':'Ensemble-Hourly','Hourly-Long': 'Ensemble-Hourly','Daily':'Ensemble-Daily'})
    return _shape(raw)

# Efficacy - join previous ML model forecasts to observations to compute error, keyed on (city, target_datetime, measure)

def _hourly_obs_long(engine, lookback_hours=efficacy_lookback_hours):
    sql = f"""
    SELECT city, ("Date" + "Time") AS target_datetime,
           "Temperature" AS "Temperature",
           "WindSpeed" * {MPH_TO_KMH} AS "WindSpeed",
           "HourlyRainfall" AS "RainVolume"
    FROM observations
    WHERE ("Date" + "Time")::timestamp > NOW() - INTERVAL '{lookback_hours} hours'
      AND ("Date" + "Time")::timestamp <= NOW()
    """
    raw = pd.read_sql(sql, engine)
    # rain prob = 100 if it rained, 0 if not
    raw['RainProbability'] = (raw['RainVolume'] > 0).astype(float) * 100
    raw.loc[raw['RainVolume'].isna(), 'RainProbability'] = None
    long_ = raw.melt(
        id_vars=['city', 'target_datetime'],
        value_vars=['Temperature', 'WindSpeed', 'RainVolume', 'RainProbability'],
        var_name='measure', value_name='obs_value')
    return long_.dropna(subset=['obs_value'])

# Pull daily observations and format to tidy format

def _daily_obs_long(engine, lookback_days=daily_lookback_days):
    sql = f"""
    SELECT city, "Date" AS date,
           MIN("Temperature") AS "MinTemperature",
           MAX("Temperature") AS "MaxTemperature",
           AVG("WindSpeed") * {MPH_TO_KMH} AS "WindSpeed",
           SUM("HourlyRainfall") AS "RainVolume",
           MAX("HourlyRainfall") > 0 AS rainy
    FROM observations
    WHERE "Date" > CURRENT_DATE - INTERVAL '{lookback_days} days' AND "Date" <= CURRENT_DATE
    GROUP BY city, "Date"
    """
    raw = pd.read_sql(sql, engine)
    raw['RainProbability'] = raw['rainy'].astype(float) * 100
    raw.loc[raw['rainy'].isna(), 'RainProbability'] = None
    raw = raw.drop(columns=['rainy'])
    long_ = raw.melt(
        id_vars=['city', 'date'],
        value_vars=['MinTemperature', 'MaxTemperature', 'WindSpeed', 'RainVolume', 'RainProbability'],
        var_name='measure', value_name='obs_value')
    return long_.dropna(subset=['obs_value'])

# Pulls ensemble ML model hourly forecasts in last 12 hours

def _past_ensemble_hourly(engine, lead_hourly=lead_hourly_default):
    sql = f"""
    WITH ranked AS (
      SELECT city,
             ("Date" + "Time") AS target_datetime,
             "Date" AS date, "Time" AS time,
             measure, p50 AS value, p10, p90,
             "ForecastTaken" AS forecast_taken,
             EXTRACT(EPOCH FROM (("Date" + "Time") - "ForecastTaken")) / 3600 AS hours_ahead,
             ROW_NUMBER() OVER (PARTITION BY city, ("Date" + "Time"), measure
               ORDER BY "ForecastTaken" DESC) AS rn
      FROM ensemble_forecast
      WHERE "Time" IS NOT NULL
        AND ("Date" + "Time")::timestamp > NOW() - INTERVAL '{efficacy_lookback_hours} hours'
        AND ("Date" + "Time")::timestamp <= NOW()
        AND "ForecastTaken" <= ("Date" + "Time") - INTERVAL '{lead_hourly} hours'
    )
    SELECT city, target_datetime, date, time, 
            measure, value, p10, p90, forecast_taken, hours_ahead
    FROM ranked
    WHERE rn = 1
    """
    df = pd.read_sql(sql, engine)

    df['source'] = 'Ensemble'
    df['tier'] = 'Efficacy'
    return _shape(df)

# Pulls ensemble ML model daily forecast history

def _past_ensemble_daily(engine, lead_daily=lead_daily_default):
    sql = f"""
    SELECT DISTINCT ON (city, "Date", measure)
           city,
           "Date"::timestamp AS target_datetime,
           "Date" AS date, NULL AS time,
           measure, p50 AS value, p10, p90,
           "ForecastTaken" AS forecast_taken,
           ("Date" - "ForecastTaken"::date) * 24.0 AS hours_ahead
    FROM ensemble_forecast
    WHERE "Time" IS NULL
      AND "Date" > CURRENT_DATE - INTERVAL '{daily_lookback_days} days'
      AND "Date" <= CURRENT_DATE
      AND "ForecastTaken" <= "Date" - INTERVAL '{lead_daily} hours'
    ORDER BY city, "Date", measure, "ForecastTaken" DESC
    """
    df = pd.read_sql(sql, engine)
  
    df['source'] = 'Ensemble'
    df['tier'] = 'Efficacy'
    return _shape(df)

# Pulls per-source forecast history with same lead-time and target-time constraints as ensemblE

def _past_source_hourly(engine, lead_hourly=lead_hourly_default):
    sql = f"""
    SELECT DISTINCT ON (hf.city, target_datetime, hf."Model")
        hf.city,
        (hf."Date" + hf."Time") AS target_datetime,
        hf."Date" AS date, hf."Time" AS time,
        hf."Model" AS source,
        hf."ForecastTaken" AS forecast_taken,
        EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 AS hours_ahead,
        hf."Temperature" AS temperature,
        CASE WHEN hf."Model" = 'MetOffice' THEN hf."WindSpeed" * {MPH_TO_KMH}   
             ELSE hf."WindSpeed" END AS windspeed,
        hf."WindDirection"   AS winddirection,
        hf."RainProbability" AS rainprobability,
        hf."RainVolume"      AS rainvolume
    FROM hourly_forecast hf
    WHERE (hf."Date" + hf."Time")::timestamp > NOW() - INTERVAL '{efficacy_lookback_hours} hours'
      AND (hf."Date" + hf."Time")::timestamp <= NOW()
      AND hf."ForecastTaken" <= (hf."Date" + hf."Time") - INTERVAL '{lead_hourly} hours'
    ORDER BY hf.city, target_datetime, hf."Model", hf."ForecastTaken" DESC
    """
    raw = pd.read_sql(sql, engine)

    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['temperature', 'windspeed', 'winddirection','rainprobability', 'rainvolume'],
        var_name='measure', 
        value_name='value')
    long_['measure'] = long_['measure'].map({'temperature': 'Temperature','windspeed': 'WindSpeed', 'winddirection': 'WindDirection','rainprobability': 'RainProbability', 'rainvolume': 'RainVolume'})
    long_['tier'] = 'Efficacy'
    return _shape(long_)

# Pulls per-source daily forecasts with same lead-time and target-time constraints as ensemble

def _past_source_daily(engine, lead_daily=lead_daily_default):
    sql =  f"""
    SELECT DISTINCT ON (df.city, df."Date", df."Model")
        df.city,
        df."Date"::timestamp AS target_datetime,
        df."Date" AS date,
        NULL AS time,
        df."Model" AS source,
        df."ForecastTaken" AS forecast_taken,
        (df."Date" - df."ForecastTaken"::date) * 24.0 AS hours_ahead,
        "MinTemperature" AS mintemperature,
        "MaxTemperature" AS maxtemperature,
        CASE WHEN df."Model" = 'MetOffice' THEN "WindSpeed" * {MPH_TO_KMH}
             ELSE "WindSpeed" END AS windspeed,
        "WindDirection"   AS winddirection,
        "RainProbability" AS rainprobability,
        "RainVolume"      AS rainvolume
    FROM daily_forecast df
    WHERE df."Date" > CURRENT_DATE - INTERVAL '{daily_lookback_days} days'
      AND df."Date" <= CURRENT_DATE
      AND df."ForecastTaken" <= df."Date" - INTERVAL '{lead_daily} hours'
    ORDER BY df.city, df."Date", df."Model", df."ForecastTaken" DESC
    """
    raw = pd.read_sql(sql, engine)

    long_ = raw.melt(
        id_vars=['city', 'target_datetime', 'date', 'time', 'source', 'forecast_taken', 'hours_ahead'],
        value_vars=['mintemperature', 'maxtemperature', 'windspeed','winddirection', 'rainprobability', 'rainvolume'],
        var_name='measure',
        value_name='value',
    )
    long_['measure'] = long_['measure'].map({'mintemperature': 'MinTemperature','maxtemperature': 'MaxTemperature','windspeed': 'WindSpeed', 'winddirection': 'WindDirection','rainprobability': 'RainProbability','rainvolume': 'RainVolume'})
    long_['tier'] = 'Efficacy'
    return _shape(long_)

# Combines output of above functions into long tidy dataframe

def _efficacy_with_sources(engine):
    # Hourly
    h_parts = [_past_ensemble_hourly(engine, 12), _past_source_hourly(engine, 12)]
    h_parts = [p for p in h_parts if not p.empty]
    hourly_eff = pd.concat(h_parts, ignore_index=True) 
    if not hourly_eff.empty:
        obs_h = _hourly_obs_long(engine)
        if not obs_h.empty:
            hourly_eff['target_datetime'] = pd.to_datetime(hourly_eff['target_datetime'])
            obs_h['target_datetime'] = pd.to_datetime(obs_h['target_datetime'])
            hourly_eff = hourly_eff.merge(
                obs_h, on=['city', 'target_datetime', 'measure'], how='left')
            hourly_eff['error'] = hourly_eff['value'] - hourly_eff['obs_value']
            hourly_eff = hourly_eff.drop(columns=['obs_value'])

    # Daily
    d_parts = [_past_ensemble_daily(engine, 24),_past_source_daily(engine, 24)]
    d_parts = [p for p in d_parts if not p.empty]
    daily_eff = pd.concat(d_parts, ignore_index=True) 
    if not daily_eff.empty:
        obs_d = _daily_obs_long(engine)
        if not obs_d.empty:
            daily_eff['date'] = pd.to_datetime(daily_eff['date']).dt.date
            obs_d['date'] = pd.to_datetime(obs_d['date']).dt.date
            daily_eff = daily_eff.merge(
                obs_d, on=['city', 'date', 'measure'], how='left')
            daily_eff['error'] = daily_eff['value'] - daily_eff['obs_value']
            daily_eff = daily_eff.drop(columns=['obs_value'])

    out_parts = [p for p in (hourly_eff, daily_eff) if not p.empty]
    return pd.concat(out_parts, ignore_index=True)

##########################################
# Metrics and rain reliability functions #
##########################################

# Per-day MAE/bias for each (date, tier, measure, source)

def build_metrics_daily(efficacy_df):
    cols = ['date', 'tier', 'measure', 'source', 'n', 'mae', 'bias']
    if efficacy_df.empty:
        return pd.DataFrame(columns=cols)

    df = efficacy_df.copy()
    df = df[df['error'].notna()]
    if df.empty:
        return pd.DataFrame(columns=cols)

    is_hourly = df['time'].notna() & (df['time'].astype(str) != 'None')
    df['tier'] = np.where(is_hourly, 'Hourly', 'Daily')
    df['date'] = pd.to_datetime(df['date']).dt.date

    metrics = df.groupby(['date', 'tier', 'measure', 'source'], dropna=False).agg(
        n=('error', 'size'),
        mae=('error', lambda s: float(np.mean(np.abs(s)))),
        bias=('error', lambda s: float(np.mean(s)))).reset_index()

    metrics['date'] = pd.to_datetime(metrics['date']).dt.strftime('%Y-%m-%d')
    return metrics[cols]


# Calibration table for RainProbability — one row per (tier, source, decile bin)

def build_reliability(efficacy_df, n_bins=10):
    cols = ['tier', 'measure', 'source', 'decile_lo', 'decile_hi',
            'n_predictions', 'mean_predicted', 'observed_rate']
    if efficacy_df.empty:
        return pd.DataFrame(columns=cols)

    df = efficacy_df[(efficacy_df['measure'] == 'RainProbability') & efficacy_df['error'].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    is_hourly = df['time'].notna() & (df['time'].astype(str) != 'None')
    df['tier'] = np.where(is_hourly, 'Hourly', 'Daily')
    df['obs_value'] = df['value'] - df['error']

    edges = np.linspace(0, 100, n_bins + 1)
    df['decile_lo'] = pd.cut(df['value'].clip(0, 100), bins=edges,include_lowest=True, right=False, labels=edges[:-1]).astype(float)
    df.loc[df['value'] >= edges[-1], 'decile_lo'] = edges[-2]
    df['decile_hi'] = df['decile_lo'] + (100 / n_bins)

    grouped = df.groupby(['tier', 'measure', 'source', 'decile_lo', 'decile_hi'], dropna=False).agg(n_predictions=('value', 'size'), mean_predicted=('value', 'mean'),  observed_rate=('obs_value', lambda s: float((s > 0).mean() * 100))).reset_index()
    return grouped[cols]

# Per (city, source, measure, lead_bucket) n/MAE/bias. SQL just pulls the wide join;
# error + aggregation are done in pandas to match the rest of this file.

def build_horizon_accuracy(engine):
    hourly_sql = f"""
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
            hf."Model" AS source,
            ROUND(EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600) AS lead_bucket,
            hf."Temperature" AS fc_Temperature,
            CASE WHEN hf."Model" = 'MetOffice' THEN hf."WindSpeed" * {MPH_TO_KMH} ELSE hf."WindSpeed" END AS fc_WindSpeed,
            hf."WindDirection" AS fc_WindDirection,
            hf."RainProbability" AS fc_RainProbability,
            hf."RainVolume" AS fc_RainVolume,
            obs."Temperature" AS obs_Temperature,
            obs."WindSpeed" * {MPH_TO_KMH} AS obs_WindSpeed,
            cp."CenterDegrees" AS obs_WindDirection,
            CASE WHEN obs."HourlyRainfall" > 0 THEN 100 ELSE 0 END AS obs_RainProbability,
            obs."HourlyRainfall" AS obs_RainVolume
        FROM hourly_forecast hf
        JOIN observations obs ON hf.city = obs.city AND hf."Date" = obs."Date" AND hf."Time" = obs."Time"
        JOIN compass_points cp ON obs."WindDirection" = cp."Direction"
        WHERE EXTRACT(EPOCH FROM ((hf."Date" + hf."Time") - hf."ForecastTaken")) / 3600 > 0
    """
    hourly = pd.read_sql(hourly_sql, engine)
    hourly['err_Temperature']     = hourly['fc_Temperature'] - hourly['obs_Temperature']
    hourly['err_WindSpeed']       = hourly['fc_WindSpeed']   - hourly['obs_WindSpeed']
    diff = hourly['fc_WindDirection'] - hourly['obs_WindDirection']
    hourly['err_WindDirection']   = (diff + 180) % 360 - 180
    hourly['err_RainProbability'] = hourly['fc_RainProbability'] - hourly['obs_RainProbability']
    hourly['err_RainVolume']      = hourly['fc_RainVolume']      - hourly['obs_RainVolume']

    h_long = hourly.melt(
        id_vars=['city', 'source', 'lead_bucket'],
        value_vars=['err_Temperature', 'err_WindSpeed', 'err_WindDirection', 'err_RainProbability', 'err_RainVolume'],
        var_name='measure', value_name='err',
    )
    h_long['measure'] = h_long['measure'].str[4:]
    h_long = h_long.dropna(subset=['err'])
    h_agg = h_long.groupby(['city', 'source', 'measure', 'lead_bucket']).agg(
        n=('err', 'size'),
        mae=('err', lambda s: float(np.mean(np.abs(s)))),
        bias=('err', 'mean'),
    ).reset_index()
    h_agg['tier'] = 'Hourly'

    daily_sql = f"""
        WITH daily_obs AS (
            SELECT city, "Date",
                   MIN("Temperature") AS obs_MinTemperature,
                   MAX("Temperature") AS obs_MaxTemperature,
                   AVG("WindSpeed") * {MPH_TO_KMH} AS obs_WindSpeed,
                   MAX("HourlyRainfall") AS rain_any,
                   SUM("HourlyRainfall") AS obs_RainVolume
            FROM observations
            GROUP BY city, "Date"
        )
        SELECT
            df.city, df."Model" AS source,
            (df."Date" - df."ForecastTaken"::date) AS lead_bucket,
            df."MinTemperature" AS fc_MinTemperature,
            df."MaxTemperature" AS fc_MaxTemperature,
            CASE WHEN df."Model" = 'MetOffice' THEN df."WindSpeed" * {MPH_TO_KMH} ELSE df."WindSpeed" END AS fc_WindSpeed,
            df."RainProbability" AS fc_RainProbability,
            df."RainVolume" AS fc_RainVolume,
            d_obs.obs_MinTemperature, d_obs.obs_MaxTemperature, d_obs.obs_WindSpeed,
            CASE WHEN d_obs.rain_any > 0 THEN 100 ELSE 0 END AS obs_RainProbability,
            d_obs.obs_RainVolume
        FROM daily_forecast df
        JOIN daily_obs d_obs ON df.city = d_obs.city AND df."Date" = d_obs."Date"
        WHERE (df."Date" - df."ForecastTaken"::date) > 0
    """
    daily = pd.read_sql(daily_sql, engine)
    daily['err_MinTemperature']  = daily['fc_MinTemperature']  - daily['obs_MinTemperature']
    daily['err_MaxTemperature']  = daily['fc_MaxTemperature']  - daily['obs_MaxTemperature']
    daily['err_WindSpeed']       = daily['fc_WindSpeed']       - daily['obs_WindSpeed']
    daily['err_RainProbability'] = daily['fc_RainProbability'] - daily['obs_RainProbability']
    daily['err_RainVolume']      = daily['fc_RainVolume']      - daily['obs_RainVolume']

    d_long = daily.melt(
        id_vars=['city', 'source', 'lead_bucket'],
        value_vars=['err_MinTemperature', 'err_MaxTemperature', 'err_WindSpeed', 'err_RainProbability', 'err_RainVolume'],
        var_name='measure', value_name='err',
    )
    d_long['measure'] = d_long['measure'].str[4:]
    d_long = d_long.dropna(subset=['err'])
    d_agg = d_long.groupby(['city', 'source', 'measure', 'lead_bucket']).agg(
        n=('err', 'size'),
        mae=('err', lambda s: float(np.mean(np.abs(s)))),
        bias=('err', 'mean'),
    ).reset_index()
    d_agg['tier'] = 'Daily'

    return pd.concat([h_agg, d_agg], ignore_index=True)[
        ['tier', 'city', 'source', 'measure', 'lead_bucket', 'n', 'mae', 'bias']
    ]


# Convert DataFrame to list-of-lists with formatted datetimes and no NaNs, for pushing to Google Sheets

def build_parts(engine):

    efficacy = _efficacy_with_sources(engine)
    parts = [_observations(engine), _source_hourly_latest(engine), _source_daily_latest(engine), _ensemble_latest(engine), efficacy]
    parts = [p for p in parts if not p.empty]
    long_df = pd.concat(parts, ignore_index=True) 
    long_df = long_df.dropna(subset=['value'])
    return {'long': _shape(long_df), 'efficacy': efficacy}

def build_long_dataframe(engine):
    return build_parts(engine)['long']

def main():
    from dotenv import load_dotenv
    load_dotenv()
    log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    engine = create_engine(os.environ['DATABASE_URL'])
    df = build_long_dataframe(engine)
    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f'wrote {len(df):,} rows to {OUTPUT_PATH}')
    return OUTPUT_PATH

if __name__ == '__main__':
    main()

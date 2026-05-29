"""Streamlit visualisation for the ensemble forecast.

Three panels:
  A) Forward hourly forecast (1-48h) — ensemble p50 with p10-p90 band, source
     forecasts overlaid, plus per-source SHAP for the selected hour.
  B) Forward daily forecast (1-14d) — same shape, on day granularity.
  C) Last 48h efficacy — observation vs ensemble vs sources at a fixed lead time.

Reads from the Postgres tables populated by:
  weather_api_export.py (hourly observations + source forecasts)
  predict_ensemble.py    (ensemble_forecast rows)
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
st.set_page_config(page_title='Weather Ensemble', layout='wide')

MPH_TO_KMH = 1.609344
SOURCES = ['MetOffice', 'OpenMeteo-ECMWF', 'OpenMeteo-GFSHRRR', 'AccuWeather']
SHAP_COLS = {
    'MetOffice':         'shap_MetOffice',
    'OpenMeteo-ECMWF':   'shap_OpenMeteo_ECMWF',
    'OpenMeteo-GFSHRRR': 'shap_OpenMeteo_GFSHRRR',
    'AccuWeather':       'shap_AccuWeather',
    'other':             'shap_other',
}
HOURLY_TARGETS  = ['Temperature', 'WindSpeed', 'WindDirection', 'RainProbability', 'RainVolume']
DAILY_TARGETS   = ['MinTemperature', 'MaxTemperature', 'WindSpeed', 'RainProbability', 'RainVolume']

SOURCE_FC_HOURLY = {
    'Temperature':     'Temperature',
    'WindSpeed':       'WindSpeed',          # needs mph->kmh for MetOffice
    'WindDirection':   'WindDirection',
    'RainProbability': 'RainProbability',
    'RainVolume':      'RainVolume',
}
SOURCE_FC_DAILY = {
    'MinTemperature':  'MinTemperature',
    'MaxTemperature':  'MaxTemperature',
    'WindSpeed':       'WindSpeed',
    'RainProbability': 'RainProbability',
    'RainVolume':      'RainVolume',
}


@st.cache_resource
def get_engine():
    return create_engine(os.environ['DATABASE_URL'])


@st.cache_data(ttl=300)
def list_cities() -> list[str]:
    return pd.read_sql('SELECT DISTINCT city FROM observations ORDER BY city', get_engine())['city'].tolist()


@st.cache_data(ttl=300)
def latest_ensemble_hourly(city: str, target: str) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT MAX("ForecastTaken") AS ft FROM ensemble_forecast
      WHERE city = %(city)s AND target = %(target)s AND "Time" IS NOT NULL
    )
    SELECT "Date", "Time", p10, p50, p90,
           "shap_MetOffice", "shap_OpenMeteo_ECMWF", "shap_OpenMeteo_GFSHRRR",
           "shap_AccuWeather", "shap_other"
    FROM ensemble_forecast, latest
    WHERE city = %(city)s AND target = %(target)s AND "Time" IS NOT NULL
      AND "ForecastTaken" = latest.ft
    ORDER BY "Date", "Time"
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city, 'target': target})
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    return df


@st.cache_data(ttl=300)
def latest_ensemble_daily(city: str, target: str) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT MAX("ForecastTaken") AS ft FROM ensemble_forecast
      WHERE city = %(city)s AND target = %(target)s AND "Time" IS NULL
    )
    SELECT "Date", p10, p50, p90,
           "shap_MetOffice", "shap_OpenMeteo_ECMWF", "shap_OpenMeteo_GFSHRRR",
           "shap_AccuWeather", "shap_other"
    FROM ensemble_forecast, latest
    WHERE city = %(city)s AND target = %(target)s AND "Time" IS NULL
      AND "ForecastTaken" = latest.ft
    ORDER BY "Date"
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city, 'target': target})
    if not df.empty:
        df['ts'] = pd.to_datetime(df['Date'])
    return df


@st.cache_data(ttl=300)
def latest_source_hourly(city: str, fc_col: str) -> pd.DataFrame:
    sql = f"""
    WITH latest AS (
      SELECT "Model", MAX("ForecastTaken") AS ft FROM hourly_forecast
      WHERE city = %(city)s AND "ForecastTaken" > NOW() - INTERVAL '6 hours'
      GROUP BY "Model"
    )
    SELECT hf."Model", hf."Date", hf."Time", hf."{fc_col}" AS v
    FROM hourly_forecast hf JOIN latest l USING ("Model")
    WHERE hf.city = %(city)s AND hf."ForecastTaken" = l.ft
      AND (hf."Date" + hf."Time")::timestamp > NOW()
    ORDER BY hf."Model", hf."Date", hf."Time"
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city})
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    if fc_col == 'WindSpeed':
        df.loc[df['Model'] == 'MetOffice', 'v'] *= MPH_TO_KMH
    return df


@st.cache_data(ttl=300)
def latest_source_daily(city: str, fc_col: str) -> pd.DataFrame:
    sql = f"""
    WITH latest AS (
      SELECT "Model", MAX("ForecastTaken") AS ft FROM daily_forecast
      WHERE city = %(city)s AND "ForecastTaken" > NOW() - INTERVAL '12 hours'
      GROUP BY "Model"
    )
    SELECT df."Model", df."Date", df."{fc_col}" AS v
    FROM daily_forecast df JOIN latest l USING ("Model")
    WHERE df.city = %(city)s AND df."ForecastTaken" = l.ft
      AND df."Date" >= CURRENT_DATE
    ORDER BY df."Model", df."Date"
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city})
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['Date'])
    if fc_col == 'WindSpeed':
        df.loc[df['Model'] == 'MetOffice', 'v'] *= MPH_TO_KMH
    return df


@st.cache_data(ttl=300)
def recent_observations(city: str, target_to_col: dict) -> pd.DataFrame:
    sql = """
    SELECT "Date", "Time", "Temperature", "WindSpeed", "HourlyRainfall"
    FROM observations
    WHERE city = %(city)s AND ("Date" + "Time")::timestamp > NOW() - INTERVAL '48 hours'
    ORDER BY "Date", "Time"
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city})
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    df['WindSpeed'] = df['WindSpeed'] * MPH_TO_KMH
    return df


@st.cache_data(ttl=300)
def efficacy_panel(city: str, target: str, lead_hours: int = 6) -> pd.DataFrame:
    """For each of the last 48 obs hours, find the most-recent ensemble p50 forecast that
    was issued at least `lead_hours` before the target time. Source forecasts at the same
    lead are joined on the side."""
    sql = """
    WITH targets AS (
      SELECT "Date", "Time", ("Date" + "Time")::timestamp AS target_ts
      FROM observations
      WHERE city = %(city)s
        AND ("Date" + "Time")::timestamp > NOW() - INTERVAL '48 hours'
    ),
    matched_ens AS (
      SELECT t."Date", t."Time", t.target_ts,
             ef.p50,
             ef."ForecastTaken"
      FROM targets t
      LEFT JOIN LATERAL (
        SELECT p50, "ForecastTaken"
        FROM ensemble_forecast ef
        WHERE ef.city = %(city)s AND ef.target = %(target)s
          AND ef."Time" IS NOT NULL
          AND (ef."Date" + ef."Time")::timestamp = t.target_ts
          AND ef."ForecastTaken" <= t.target_ts - (%(lead)s::text || ' hours')::interval
        ORDER BY ef."ForecastTaken" DESC LIMIT 1
      ) ef ON TRUE
    )
    SELECT * FROM matched_ens ORDER BY target_ts
    """
    df = pd.read_sql(sql, get_engine(),
                     params={'city': city, 'target': target, 'lead': str(lead_hours)})
    if not df.empty:
        df['target_ts'] = pd.to_datetime(df['target_ts'])
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title('UK weather ensemble')

cities = list_cities()
if not cities:
    st.error('No observations yet — check that the worker is running.')
    st.stop()

col_a, col_b = st.columns([1, 1])
with col_a:
    city = st.selectbox('City', cities)
with col_b:
    target = st.selectbox('Target variable (hourly)', HOURLY_TARGETS)


# ---- Section A: forward hourly ----
st.header(f'A. Forward hourly forecast — {target}')

ens_h = latest_ensemble_hourly(city, target)
src_h = latest_source_hourly(city, SOURCE_FC_HOURLY[target])

if ens_h.empty:
    st.warning('No ensemble forecast yet for this (city, target).')
else:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ens_h['ts'], y=ens_h['p90'], mode='lines',
                              line=dict(color='rgba(0,100,200,0)'), showlegend=False, hoverinfo='skip'))
    fig.add_trace(go.Scatter(x=ens_h['ts'], y=ens_h['p10'], mode='lines',
                              line=dict(color='rgba(0,100,200,0)'),
                              fill='tonexty', fillcolor='rgba(0,100,200,0.2)',
                              name='Ensemble p10-p90'))
    fig.add_trace(go.Scatter(x=ens_h['ts'], y=ens_h['p50'], mode='lines+markers',
                              line=dict(color='rgb(0,80,180)', width=3),
                              name='Ensemble p50'))
    for src in SOURCES:
        s = src_h[src_h['Model'] == src]
        if not s.empty:
            fig.add_trace(go.Scatter(x=s['ts'], y=s['v'], mode='lines',
                                      line=dict(width=1, dash='dot'),
                                      opacity=0.7, name=src))
    fig.update_layout(height=420, margin=dict(l=30, r=20, t=30, b=20),
                       xaxis_title='Target hour', yaxis_title=target)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader('SHAP contribution by source (next hour)')
    next_row = ens_h.iloc[0]
    shap_data = pd.Series(
        {src: float(next_row[col]) for src, col in SHAP_COLS.items()},
        name='shap',
    ).sort_values()
    fig_shap = go.Figure(go.Bar(x=shap_data.values, y=shap_data.index, orientation='h',
                                  marker_color=['#c33' if v < 0 else '#39c' for v in shap_data.values]))
    fig_shap.update_layout(height=220, margin=dict(l=30, r=20, t=10, b=20),
                            xaxis_title='Contribution to p50')
    st.plotly_chart(fig_shap, use_container_width=True)


# ---- Section B: forward daily ----
st.header('B. Forward daily forecast')

daily_target = st.selectbox('Target variable (daily)', DAILY_TARGETS, key='daily_target')

ens_d = latest_ensemble_daily(city, daily_target)
src_d = latest_source_daily(city, SOURCE_FC_DAILY[daily_target])

if ens_d.empty:
    st.warning('No daily ensemble forecast yet for this (city, target).')
else:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ens_d['ts'], y=ens_d['p90'], mode='lines',
                              line=dict(color='rgba(200,80,0,0)'), showlegend=False, hoverinfo='skip'))
    fig.add_trace(go.Scatter(x=ens_d['ts'], y=ens_d['p10'], mode='lines',
                              line=dict(color='rgba(200,80,0,0)'),
                              fill='tonexty', fillcolor='rgba(200,80,0,0.2)',
                              name='Ensemble p10-p90'))
    fig.add_trace(go.Scatter(x=ens_d['ts'], y=ens_d['p50'], mode='lines+markers',
                              line=dict(color='rgb(180,80,0)', width=3),
                              name='Ensemble p50'))
    for src in SOURCES:
        s = src_d[src_d['Model'] == src]
        if not s.empty:
            fig.add_trace(go.Scatter(x=s['ts'], y=s['v'], mode='lines',
                                      line=dict(width=1, dash='dot'),
                                      opacity=0.7, name=src))
    fig.update_layout(height=420, margin=dict(l=30, r=20, t=30, b=20),
                       xaxis_title='Date', yaxis_title=daily_target)
    st.plotly_chart(fig, use_container_width=True)


# ---- Section C: 48h efficacy ----
st.header(f'C. Last 48h efficacy — {target}')

lead = st.slider('Forecast lead time (hours ahead)', min_value=1, max_value=24, value=6)
obs = recent_observations(city, {})

obs_col = {'Temperature': 'Temperature', 'WindSpeed': 'WindSpeed',
           'RainProbability': 'HourlyRainfall', 'RainVolume': 'HourlyRainfall',
           'WindDirection': None}.get(target)

if obs_col is None:
    st.info('Efficacy panel not shown for WindDirection (no observed-degrees column).')
elif obs.empty:
    st.warning('No recent observations.')
else:
    eff = efficacy_panel(city, target, lead_hours=lead)
    if obs_col == 'HourlyRainfall' and target == 'RainProbability':
        # Plot rainfall amount alongside p50 probability (different units) — best
        # treated as a 2-axis chart. For now show rainfall and the ensemble prob
        # together on a single axis but note the unit mismatch.
        obs_plot = obs[['ts', obs_col]].rename(columns={obs_col: 'obs'})
    else:
        obs_plot = obs[['ts', obs_col]].rename(columns={obs_col: 'obs'})

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=obs_plot['ts'], y=obs_plot['obs'], mode='lines+markers',
                              name='Observed', line=dict(color='black', width=2)))
    if not eff.empty:
        fig.add_trace(go.Scatter(x=eff['target_ts'], y=eff['p50'], mode='lines+markers',
                                  name=f'Ensemble p50 (lead {lead}h)',
                                  line=dict(color='rgb(0,80,180)', width=2, dash='dash')))
    fig.update_layout(height=420, margin=dict(l=30, r=20, t=30, b=20),
                       xaxis_title='Hour', yaxis_title=target)
    st.plotly_chart(fig, use_container_width=True)


# ---- Model metadata footer ----
metrics_path = Path('metrics.json')
if metrics_path.exists():
    with metrics_path.open() as f:
        meta = json.load(f)
    st.caption(f'Models trained at {meta.get("trained_at", "?")} ({len(meta.get("metrics", []))} heads). '
                f'Train duration: {meta.get("duration_s", "?")} s.')

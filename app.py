"""Streamlit visualisation for the ensemble forecast.

User-facing forecast dashboard:
  - Daily forecast cards (14 days, scrollable). Click a day to expand details.
  - Forward hourly forecast chart (last 12h to next 24h).

Model-quality / SHAP analytics live in Tableau, not here.

Reads from the Postgres tables populated by:
  weather_api_export.py (hourly observations + source forecasts)
  predict_ensemble.py    (ensemble_forecast rows)
"""

from __future__ import annotations

import json
import os
from datetime import datetime
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
HOURLY_MEASURES = ['Temperature', 'WindSpeed', 'WindDirection', 'RainProbability', 'RainVolume']

SOURCE_FC_HOURLY = {
    'Temperature':     'Temperature',
    'WindSpeed':       'WindSpeed',          # needs mph->kmh for MetOffice
    'WindDirection':   'WindDirection',
    'RainProbability': 'RainProbability',
    'RainVolume':      'RainVolume',
}

# Observed-series column for each hourly measure (None = no direct obs).
OBS_COL_FOR_MEASURE = {
    'Temperature': 'Temperature',
    'WindSpeed':   'WindSpeed',
    'RainVolume':  'HourlyRainfall',
}

DAILY_CARD_SCROLL_CSS = """
<style>
div[data-testid="stHorizontalBlock"]:has(> div .daily-card-row-marker) {
  overflow-x: auto;
  flex-wrap: nowrap !important;
  padding-bottom: 6px;
}
div[data-testid="stHorizontalBlock"]:has(> div .daily-card-row-marker) > div {
  flex: 0 0 auto !important;
  min-width: 140px;
  max-width: 160px;
}

/* Smooth fade-in for the expanded per-day details panel.
   The marker .daily-details-anchor scopes the rule to one stVerticalBlock. */
@keyframes daily-details-fadein {
  from { opacity: 0; transform: translateY(-8px); }
  to   { opacity: 1; transform: translateY(0); }
}
div[data-testid="stVerticalBlock"]:has(> div .daily-details-anchor) {
  animation: daily-details-fadein 0.35s ease-out;
}
</style>
"""


_SYMBOL_PX = 56

# Inline SVG glyphs. viewBox is 0 0 24 24 so width/height attrs scale them.
# _SVG_TEMP uses currentColor so the wrapping div's `color:` tints it
# red/grey/blue. Rain/wind hardcode their own colours.
_SVG_TEMP = (
    f'<svg viewBox="0 0 24 24" width="{_SYMBOL_PX}" height="{_SYMBOL_PX}" '
    'fill="none" stroke="currentColor" stroke-width="2" '
    'stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M12 3 a2.5 2.5 0 0 0 -2.5 2.5 v9 a4.5 4.5 0 1 0 5 0 '
    'V5.5 a2.5 2.5 0 0 0 -2.5 -2.5 z"/>'
    '<circle cx="12" cy="17" r="2.5" fill="currentColor" stroke="none"/>'
    '<rect x="11" y="8" width="2" height="9" fill="currentColor" stroke="none"/>'
    '</svg>'
)
_SVG_WIND = (
    f'<svg viewBox="0 0 24 24" width="{_SYMBOL_PX}" height="{_SYMBOL_PX}" '
    'fill="none" stroke="#7dc8e0" stroke-width="2.2" '
    'stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M3 7 h11 a2.5 2.5 0 1 0 -2.5 -2.5"/>'
    '<path d="M3 12 h15 l-2.5 -2.5 m2.5 2.5 l-2.5 2.5"/>'
    '<path d="M3 17 h11 a2.5 2.5 0 1 1 -2.5 2.5"/>'
    '</svg>'
)
_SVG_RAIN_ONE = (
    f'<svg viewBox="0 0 24 24" width="{_SYMBOL_PX}" height="{_SYMBOL_PX}">'
    '<path d="M12 3 C8 9 6 13 6 16 a6 6 0 0 0 12 0 C18 13 16 9 12 3 Z" '
    'fill="#2d7dd2"/>'
    '<path d="M9 18 a3 3 0 0 0 3 2" fill="none" stroke="#e0e0e0" '
    'stroke-width="1.2" stroke-linecap="round" opacity="0.7"/>'
    '</svg>'
)
_SVG_RAIN_TWO = (
    f'<svg viewBox="0 0 24 24" width="{_SYMBOL_PX}" height="{_SYMBOL_PX}">'
    '<path d="M17 3 C15 6 13.5 8.5 13.5 10.5 a3.5 3.5 0 0 0 7 0 '
    'C20.5 8.5 19 6 17 3 Z" fill="#1f4e8c"/>'
    '<path d="M10 8 C7 12 5 16 5 18 a5 5 0 0 0 10 0 C15 16 13 12 10 8 Z" '
    'fill="#1f4e8c"/>'
    '<path d="M7.5 19 a2.5 2.5 0 0 0 2.5 1.5" fill="none" stroke="#a8b0c0" '
    'stroke-width="1.2" stroke-linecap="round" opacity="0.7"/>'
    '</svg>'
)


def _symbol_row_html(hi, rp, rv, ws) -> str:
    """One symbol per card via priority cascade (first match wins):
      1. wind > 40 km/h          -> wind        (overrides rain)
      2. rp > 70 AND rv > 2      -> heavy rain
      3. rp > 50 AND rv > 0.5    -> light rain
      4. wind > 20 km/h          -> wind
      5. default                 -> temperature, tinted by max-temp
    """
    tint = None
    if ws is not None and ws > 40:
        svg = _SVG_WIND
    elif (rp is not None and rv is not None
          and rp > 70 and rv > 2):
        svg = _SVG_RAIN_TWO
    elif (rp is not None and rv is not None
          and rp > 50 and rv > 0.5):
        svg = _SVG_RAIN_ONE
    elif ws is not None and ws > 20:
        svg = _SVG_WIND
    else:
        if hi is None:
            return (
                '<div style="display:flex;align-items:center;'
                'justify-content:center;height:'
                f'{_SYMBOL_PX}px;margin:8px 0 6px 0;">&nbsp;</div>'
            )
        if hi > 20:
            tint = '#d63b3b'
        elif hi < 10:
            tint = '#3b7bd6'
        else:
            tint = '#888'
        svg = _SVG_TEMP
    color_style = f'color:{tint};' if tint else ''
    return (
        '<div style="display:flex;align-items:center;justify-content:center;'
        f'margin:8px 0 6px 0;{color_style}">{svg}</div>'
    )


@st.cache_resource
def get_engine():
    return create_engine(
        os.environ['DATABASE_URL'],
        pool_pre_ping=True,
        pool_recycle=1800,
    )


@st.cache_data(ttl=300)
def list_cities() -> list[str]:
    return pd.read_sql('SELECT DISTINCT city FROM observations ORDER BY city', get_engine())['city'].tolist()


@st.cache_data(ttl=300)
def latest_ensemble_hourly(city: str, measure: str) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT MAX("ForecastTaken") AS ft FROM ensemble_forecast
      WHERE city = %(city)s AND measure = %(measure)s AND "Time" IS NOT NULL
    )
    SELECT DISTINCT ON ("Date", "Time")
           "Date", "Time", p10, p50, p90
    FROM ensemble_forecast, latest
    WHERE city = %(city)s AND measure = %(measure)s AND "Time" IS NOT NULL
      AND "ForecastTaken" = latest.ft
    ORDER BY "Date", "Time", id DESC
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city, 'measure': measure})
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    return df


@st.cache_data(ttl=300)
def latest_ensemble_daily_all(city: str) -> pd.DataFrame:
    """All daily ensemble measures for the latest ForecastTaken per measure.

    Returns long format: one row per (Date, measure) with p10/p50/p90.
    """
    sql = """
    WITH latest AS (
      SELECT measure, MAX("ForecastTaken") AS ft FROM ensemble_forecast
      WHERE city = %(city)s AND "Time" IS NULL
      GROUP BY measure
    )
    SELECT DISTINCT ON (ef."Date", ef.measure)
           ef."Date", ef.measure, ef.p10, ef.p50, ef.p90
    FROM ensemble_forecast ef
    JOIN latest l USING (measure)
    WHERE ef.city = %(city)s AND ef."Time" IS NULL
      AND ef."ForecastTaken" = l.ft
    ORDER BY ef."Date", ef.measure, ef.id DESC
    """
    return pd.read_sql(sql, get_engine(), params={'city': city})


@st.cache_data(ttl=300)
def latest_source_daily_all(city: str) -> pd.DataFrame:
    """Latest daily source forecast per Model, all measures, dates >= today.

    No freshness filter on ForecastTaken — if the worker stops running, the
    `Date >= CURRENT_DATE` clause naturally drops stale issues whose forecasts
    are all in the past, while a recent-but-not-fresh issue still surfaces."""
    sql = """
    WITH latest AS (
      SELECT "Model", MAX("ForecastTaken") AS ft FROM daily_forecast
      WHERE city = %(city)s
      GROUP BY "Model"
    )
    SELECT df."Model", df."Date",
           df."MinTemperature", df."MaxTemperature", df."WindSpeed",
           df."RainProbability", df."RainVolume"
    FROM daily_forecast df JOIN latest l USING ("Model")
    WHERE df.city = %(city)s AND df."ForecastTaken" = l.ft
      AND df."Date" >= CURRENT_DATE
    ORDER BY df."Model", df."Date"
    """
    df = pd.read_sql(sql, get_engine(), params={'city': city})
    if not df.empty:
        df.loc[df['Model'] == 'MetOffice', 'WindSpeed'] *= MPH_TO_KMH
    return df


@st.cache_data(ttl=300)
def latest_source_hourly(city: str, fc_col: str,
                          t_min: str, t_max: str) -> pd.DataFrame:
    """For each (Model, target hour) in [t_min, t_max], take the most-recent
    forecast issued AT OR BEFORE that target hour. Past targets show what the
    source actually predicted at the last opportunity before they occurred;
    future targets show the latest forecast.

    `t_min`/`t_max` are ISO timestamp strings (UTC, naive). Passing them as
    explicit params instead of relying on NOW() means the window can slide
    with the ensemble's data range — important when the worker is paused and
    the freshest data is hours old."""
    sql = f"""
    SELECT DISTINCT ON ("Model", "Date", "Time")
      "Model", "Date", "Time", "{fc_col}" AS v
    FROM hourly_forecast
    WHERE city = %(city)s
      AND ("Date" + "Time")::timestamp >= %(t_min)s::timestamp
      AND ("Date" + "Time")::timestamp <= %(t_max)s::timestamp
      AND "ForecastTaken" >= %(t_min)s::timestamp - INTERVAL '72 hours'
      AND "ForecastTaken" <= ("Date" + "Time")::timestamp
    ORDER BY "Model", "Date", "Time", "ForecastTaken" DESC
    """
    df = pd.read_sql(sql, get_engine(),
                     params={'city': city, 't_min': t_min, 't_max': t_max})
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    if fc_col == 'WindSpeed':
        df.loc[df['Model'] == 'MetOffice', 'v'] *= MPH_TO_KMH
    return df


@st.cache_data(ttl=300)
def daily_history_weeks() -> float | None:
    """How many weeks of Daily ensemble history have accumulated. Used to
    surface a 'model is under-trained' banner until ~6 weeks of varied weather
    have been seen. Returns None if the table is empty."""
    sql = 'SELECT MIN("ForecastTaken") AS earliest FROM ensemble_forecast WHERE tier = \'Daily\''
    res = pd.read_sql(sql, get_engine())
    if res.empty or pd.isna(res.iloc[0]['earliest']):
        return None
    earliest = pd.to_datetime(res.iloc[0]['earliest'])
    return (datetime.utcnow() - earliest).total_seconds() / (7 * 86400)


@st.cache_data(ttl=300)
def recent_observations(city: str, measure_to_col: dict) -> pd.DataFrame:
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


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title('UK weather ensemble')

cities = list_cities()
if not cities:
    st.error('No observations yet — check that the worker is running.')
    st.stop()

city = st.selectbox('City', cities)


# ---- Daily forecast cards (top of page) ----
st.header('Daily forecast (next 14 days)')

_weeks = daily_history_weeks()
if _weeks is not None and _weeks < 6:
    st.warning(
        f'The daily ensemble is under-trained — only **{_weeks:.1f} weeks** of '
        'forecast history have accumulated since the model went live. Until ~6 '
        'weeks of varied weather are in the training set, the ensemble behaves '
        'more like a climatology lookup than a true bias correction; trust the '
        'individual source forecasts (below) over the ensemble for now.'
    )

daily_all = latest_ensemble_daily_all(city)
src_daily_all = latest_source_daily_all(city)

if daily_all.empty:
    st.warning('No daily ensemble forecast yet.')
else:
    p50_by_date = daily_all.pivot(index='Date', columns='measure', values='p50')
    dates = sorted(p50_by_date.index.tolist())

    # Selected day persists per-city. None = collapsed (no day expanded).
    sel_key = f'daily_sel::{city}'
    if sel_key not in st.session_state:
        st.session_state[sel_key] = None
    elif (st.session_state[sel_key] is not None
          and st.session_state[sel_key] not in dates):
        st.session_state[sel_key] = None

    def _get(date_, col):
        if col not in p50_by_date.columns:
            return None
        v = p50_by_date.at[date_, col]
        return None if pd.isna(v) else v

    # Single horizontally-scrollable row of cards.
    st.markdown(DAILY_CARD_SCROLL_CSS, unsafe_allow_html=True)
    cols = st.columns(len(dates))
    for i, d in enumerate(dates):
        with cols[i]:
            with st.container(border=True):
                if i == 0:
                    # Sentinel for the CSS :has() selector — scopes the
                    # scroll rules to this stHorizontalBlock only.
                    st.markdown('<span class="daily-card-row-marker"></span>',
                                unsafe_allow_html=True)

                weekday = pd.to_datetime(d).strftime('%a')
                datestr = pd.to_datetime(d).strftime('%d %b')
                is_today = (pd.to_datetime(d).date() == pd.Timestamp.utcnow().date())
                label = 'Today' if is_today else weekday
                st.markdown(f"**{label}**")
                st.caption(datestr)

                hi, lo = _get(d, 'MaxTemperature'), _get(d, 'MinTemperature')
                rp, rv = _get(d, 'RainProbability'), _get(d, 'RainVolume')
                ws = _get(d, 'WindSpeed')

                st.markdown(_symbol_row_html(hi, rp, rv, ws),
                            unsafe_allow_html=True)

                if hi is not None and lo is not None:
                    st.markdown(f"### {hi:.0f}° / {lo:.0f}°")
                elif hi is not None:
                    st.markdown(f"### {hi:.0f}°")

                if rp is not None:
                    rv_part = f' &middot; {rv:.1f} mm' if rv is not None else ''
                    st.markdown(f"**{rp:.0f}%**{rv_part}")

                if ws is not None:
                    st.markdown(f"{ws:.0f} km/h")

                selected = (d == st.session_state[sel_key])
                if st.button('Close' if selected else 'Details',
                             key=f'card_{d}', use_container_width=True):
                    st.session_state[sel_key] = None if selected else d
                    st.rerun()

    # ---- Collapsible per-day details (smooth fade-in on expand) ----
    sel = st.session_state[sel_key]
    if sel is not None:
        with st.container():
            # Anchor for the CSS :has() animation selector.
            st.markdown('<div class="daily-details-anchor"></div>',
                        unsafe_allow_html=True)
            st.markdown('---')
            st.subheader(
                f"Details — {pd.to_datetime(sel).strftime('%A %d %B')}"
            )

            day_rows = daily_all[daily_all['Date'] == sel].set_index('measure')
            measure_order = ['MinTemperature', 'MaxTemperature', 'WindSpeed',
                             'RainProbability', 'RainVolume']
            available = [t for t in measure_order if t in day_rows.index]

            # Table: rows = measure, cols = Ensemble p50 + each source's p50.
            ensemble_col = day_rows.loc[available, 'p50']
            table = pd.DataFrame({'Ensemble': ensemble_col})
            src_for_day = src_daily_all[src_daily_all['Date'] == sel]
            for src in SOURCES:
                s = src_for_day[src_for_day['Model'] == src]
                if s.empty:
                    table[src] = pd.NA
                else:
                    row = s.iloc[0]
                    table[src] = [row.get(t) if t in s.columns else pd.NA
                                  for t in available]

            table = table.apply(pd.to_numeric, errors='coerce')
            st.dataframe(table.style.format('{:.2f}', na_rep='—'),
                         use_container_width=True)


# ---- Source forecasts (next 14 days) ----
if not daily_all.empty:
    st.header('Source forecasts (next 14 days)')
    st.caption('Greyed lines are the four input models; the ensemble combines '
                'and bias-corrects these.')
    measure_order = ['MinTemperature', 'MaxTemperature', 'WindSpeed',
                     'RainProbability', 'RainVolume']
    chart_measures = [t for t in measure_order if t in p50_by_date.columns]
    chart_cols = st.columns(2)
    grey_palette = {
        'MetOffice':         'rgba(80,80,80,0.55)',
        'OpenMeteo-ECMWF':   'rgba(110,110,110,0.55)',
        'OpenMeteo-GFSHRRR': 'rgba(140,140,140,0.55)',
        'AccuWeather':       'rgba(170,170,170,0.55)',
    }
    for i, t in enumerate(chart_measures):
        with chart_cols[i % 2]:
            fig = go.Figure()
            for src in SOURCES:
                s = src_daily_all[src_daily_all['Model'] == src]
                if not s.empty and t in s.columns and s[t].notna().any():
                    fig.add_trace(go.Scatter(
                        x=pd.to_datetime(s['Date']), y=s[t],
                        mode='lines+markers', name=src,
                        line=dict(color=grey_palette[src], width=1, dash='dot'),
                        marker=dict(size=4),
                    ))
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(p50_by_date.index), y=p50_by_date[t],
                mode='lines+markers', name='Ensemble p50',
                line=dict(color='rgb(180,80,0)', width=3),
            ))
            fig.update_layout(height=260,
                               margin=dict(l=30, r=20, t=40, b=20),
                               title=t, showlegend=(i == 0),
                               legend=dict(orientation='h', y=-0.2))
            st.plotly_chart(fig, use_container_width=True)


# ---- Hourly forecast ----
measure = st.selectbox('Measure (hourly)', HOURLY_MEASURES,
                       key='hourly_measure')
st.header(f'Hourly forecast — {measure}')

ens_h = latest_ensemble_hourly(city, measure)

if ens_h.empty:
    st.warning('No ensemble forecast yet for this (city, measure).')
else:
    now_ts = datetime.utcnow()
    # Anchor the visible window to the ensemble data range (with the usual
    # [now-12h, now+24h] preferred when data is fresh). Snap to whole hours so
    # the cache key for source-hourly stays stable across reruns.
    now_hour = pd.Timestamp(now_ts).floor('h')
    data_min = pd.Timestamp(ens_h['ts'].min())
    data_max = pd.Timestamp(ens_h['ts'].max())
    x_min = min(now_hour - pd.Timedelta(hours=12), data_min)
    x_max = max(now_hour + pd.Timedelta(hours=25), data_max)

    stale_hours = (now_hour - data_max).total_seconds() / 3600
    if stale_hours > 6:
        st.info(
            f'Latest ensemble forecast is **{stale_hours:.0f} h** old — '
            'the worker may not be running. Showing what data exists; '
            'the "now" line will be off to the right of the data.'
        )

    src_h = latest_source_hourly(city, SOURCE_FC_HOURLY[measure],
                                  x_min.isoformat(), x_max.isoformat())
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ens_h['ts'], y=ens_h['p90'], mode='lines',
                              line=dict(color='rgba(0,100,200,0)'),
                              showlegend=False, hoverinfo='skip'))
    fig.add_trace(go.Scatter(x=ens_h['ts'], y=ens_h['p10'], mode='lines',
                              line=dict(color='rgba(0,100,200,0)'),
                              fill='tonexty', fillcolor='rgba(0,100,200,0.2)',
                              name='Ensemble p10-p90'))
    fig.add_trace(go.Scatter(x=ens_h['ts'], y=ens_h['p50'],
                              mode='lines+markers',
                              line=dict(color='rgb(0,80,180)', width=3),
                              name='Ensemble p50'))
    for src in SOURCES:
        s = src_h[src_h['Model'] == src]
        if not s.empty:
            fig.add_trace(go.Scatter(x=s['ts'], y=s['v'], mode='lines',
                                      line=dict(width=1, dash='dot'),
                                      opacity=0.7, name=src))

    obs_col = OBS_COL_FOR_MEASURE.get(measure)
    if obs_col is not None:
        obs_df = recent_observations(city, {})
        if not obs_df.empty:
            obs_past = obs_df[obs_df['ts'] <= pd.Timestamp(now_ts)]
            if not obs_past.empty:
                fig.add_trace(go.Scatter(
                    x=obs_past['ts'], y=obs_past[obs_col],
                    mode='lines+markers', name='Observed',
                    line=dict(color='white', width=2),
                ))

    fig.add_shape(type='line', x0=now_ts, x1=now_ts, y0=0, y1=1, yref='paper',
                   line=dict(color='gray', width=1, dash='dash'))
    fig.add_annotation(x=now_ts, y=1, yref='paper', text='now',
                       showarrow=False, xanchor='left', yanchor='bottom',
                       font=dict(color='gray', size=11))

    fig.update_xaxes(range=[x_min, x_max])

    fig.update_layout(height=420, margin=dict(l=30, r=20, t=30, b=20),
                       xaxis_title='Target hour', yaxis_title=measure)
    st.plotly_chart(fig, use_container_width=True)


# ---- Model metadata footer ----
metrics_path = Path('metrics.json')
if metrics_path.exists():
    with metrics_path.open() as f:
        meta = json.load(f)
    st.caption(f'Models trained at {meta.get("trained_at", "?")} ({len(meta.get("metrics", []))} heads). '
                f'Train duration: {meta.get("duration_s", "?")} s.')

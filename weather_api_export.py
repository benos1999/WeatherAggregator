import os
import requests
import time
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from statistics import mean
import logging as log
from dotenv import load_dotenv
import json

# Load API keys from .env file

load_dotenv()

METOFFICE_FORECAST_KEY = os.environ["METOFFICE_FORECAST_KEY"]
METOFFICE_OBS_KEY = os.environ["METOFFICE_OBS_KEY"]
ACCUWEATHER_KEY = os.environ["ACCUWEATHER_KEY"]
NRW_KEY = os.environ["NRW_KEY"]

# Set up logging

log.basicConfig(filename='weather_log.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

# Load location data from JSON file

with open('locations.json') as f:
        locations = json.load(f)

# Get current hour for forecast retrieval time reference

hour_now = time.strftime("%Y-%m-%d %H:00:00", time.localtime())

### Funtion to collate API call parameters. Feeder for retrieve_forecast

def get_params(timestep,city, source):
    
    headers = {'MetOffice':{'accept': "application/json", 'apikey': METOFFICE_FORECAST_KEY},
           'OpenMeteo-ECMWF':{},
           'OpenMeteo-GFSHRRR':{},
           'AccuWeather':{'Authorization': f'Bearer {ACCUWEATHER_KEY}'}}

    base_url = {'MetOffice': "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/",
            'OpenMeteo-ECMWF': "https://api.open-meteo.com/v1/forecast",
            'OpenMeteo-GFSHRRR': "https://api.open-meteo.com/v1/forecast",
            'AccuWeather': "https://dataservice.accuweather.com/forecasts/v1/"}
    
    if timestep == 'hourly':
        if source == 'MetOffice':
            url = base_url[source] + 'hourly'
        elif source == 'AccuWeather':
            url = base_url[source] + 'hourly/12hour/' + locations[city]['locationkey']    
        else:
            url = base_url[source]
    elif timestep == 'daily':
        if source == 'MetOffice':
            url = base_url[source] + 'daily'
        elif source == 'AccuWeather':
            url = base_url[source] + 'daily/5day/' + locations[city]['locationkey']
        else:
            url = base_url[source]
    else:
        log.error(f"Invalid timestep {timestep} for city {city} and source {source}.", exc_info=True)
        raise ValueError("Invalid timestep. Must be 'hourly' or 'daily'.")

    params_dict = {'MetOffice':{'excludeParameterMetadata' : True,
                                    'includeLocationName' : True,
                                    'latitude' : locations[city]['latitude'], 
                                    'longitude' : locations[city]['longitude']},
                 'OpenMeteo-ECMWF':{'latitude' : locations[city]['latitude'], 
                                    'longitude' : locations[city]['longitude'],
                                    "models" : "ecmwf_ifs"},
                 'OpenMeteo-GFSHRRR':{'latitude' : locations[city]['latitude'], 
                                      'longitude' : locations[city]['longitude'],
                                      "models" : "gfs_seamless"},
                'AccuWeather':{'details': 'true', 'metric': 'true'}}
    
    params = params_dict[source]
    
    if source == 'OpenMeteo-ECMWF' or source == 'OpenMeteo-GFSHRRR':
            if timestep == 'hourly':
                params.update({
                    "hourly": ["temperature_2m", "precipitation_probability", "rain", "wind_speed_10m", "wind_direction_10m"],
                    "forecast_days": 2,
                    "timezone": "GMT"
                })
            elif timestep == 'daily':
                params.update({
                    "daily": ["temperature_2m_max", "temperature_2m_min",  "precipitation_probability_max","rain_sum", "wind_speed_10m_max", "wind_direction_10m_dominant"],
                    "forecast_days": 14
                })
            else:
                log.error(f"Invalid timestep {timestep} for city {city} and source {source}.", exc_info=True)
                raise ValueError("Invalid timestep. Must be 'hourly' or 'daily'.")
            
    return url, params, headers[source]

### General API caller function 

def retrieve_forecast(url, params, headers):
    
    success = False
    retries = 5

    while not success and retries >0:
        try:
            r = requests.get(url, headers=headers, params=params)
            if r.status_code == 200:
                success = True
            else:
                log.warning(f"Unsuccessful API call for source {url} with status code {r.status_code}.", exc_info=True)
                raise Exception(f"Unsuccessful API call with status code {r.status_code}")
        except Exception as e:
            log.warning(f"Exception occurred while fetching forecast for source {url}: {e}.", exc_info=True)
            retries -= 1
            time.sleep(10)
            if retries == 0:
                log.error("Retries exceeded", exc_info=True)
                sys.exit()
        

    r.encoding = 'utf-8'

    return r.json()

### Call APIs for true weather data for last hour

def get_hourly_data(city):

    # Get METOFFICE observation hourly data
    try:
        r = requests.get(f"https://data.hub.api.metoffice.gov.uk/observation-land/1/{locations[city]['geohash']}", headers={'accept': "application/json", 'apikey':METOFFICE_OBS_KEY})
        met_office_data = r.json()[-1]
        if r.status_code != 200:
            log.warning(f"Unsuccessful API call for source MetOffice Observations in {city} with status code {r.status_code}.", exc_info=True)
            raise Exception(f"Unsuccessful API call with status code {r.status_code}")
    except Exception as e:
            log.warning(f"Exception occurred while fetching forecast for source MetOffice Observations in {city}: {e}.", exc_info=True)
                
    max_rainfall = []
    min_rainfall = []
    avg_rainfall = []
    hourly_rainfall = []  # per-station total mm over the last hour (sum of 4 15-min readings)

    # Get DEFRA rainfall hourly data

    if locations[city]['country'] == 'England':
        for station in locations[city]['rainfall_stations']:
            try:
                # parameter=rainfall is silently ignored by /stations/{id}/readings on
                # multi-measure stations, so we over-fetch (limit 20) and filter
                # client-side on the measure URI. _sorted gives descending order; we
                # take the most recent 4 rainfall readings = last hour.
                r = requests.get(
                    "https://environment.data.gov.uk/flood-monitoring/id/stations/" + station + "/readings",
                    params={"_sorted": "", "_limit": "20", "parameter": "rainfall"},
                )
                if r.status_code != 200:
                    log.warning(f"Unsuccessful API call for source DEFRA rainfall with status code {r.status_code}.", exc_info=True)
                    raise Exception(f"Unsuccessful API call with status code {r.status_code}")
            except Exception as e:
                log.warning(f"Exception occurred while fetching forecast for city {city} and source DEFRA: {e}", exc_info=True)
                continue


            try:
                items = r.json()['items']
                rainfall_items = [
                    it for it in items
                    if 'rainfall' in str(it.get('measure', '')).lower()
                ]
                readings = rainfall_items[:4]
                if not readings:
                    log.warning(f"No rainfall readings for station {station} in {city} after filtering.")
                    continue
                values = [reading['value'] for reading in readings]
                max_rainfall.append(max(values))
                min_rainfall.append(min(values))
                avg_rainfall.append(mean(values))
                hourly_rainfall.append(sum(values))
            except ValueError:
                log.warning(f"ValueError occurred for station {station} in {city}.", exc_info=True)
                continue

    # Get welsh rainfall hourly data

    elif locations[city]['country'] == 'Wales':
        for station in locations[city]['rainfall_stations']:
            try:
                r = requests.get(f"https://api.naturalresources.wales/rivers-and-seas/v1/api/StationData/historical?location={station}&parameter=10100", headers = {
                'Cache-Control': 'no-cache',
                'Ocp-Apim-Subscription-Key': NRW_KEY})
                if r.status_code != 200:
                    log.warning(f"Unsuccessful API call for source Welsh rainfall with status code {r.status_code}.", exc_info=True)
                    raise Exception(f"Unsuccessful API call with status code {r.status_code}")
            except Exception as e:
                log.warning(f"Exception occurred while fetching forecast for city {city} and source Welsh: {e}", exc_info=True)
                continue


            try:
                readings = r.json()['parameterReadings'][-4:]
                values = [reading['value'] for reading in readings]
                max_rainfall.append(max(values))
                min_rainfall.append(min(values))
                avg_rainfall.append(mean(values))
                hourly_rainfall.append(sum(values))
            except ValueError:
                log.warning(f"ValueError occurred for station {station}.", exc_info=True)
                continue

    # Get SEPA rainfall hourly data

    elif locations[city]['country'] == 'Scotland':
        for station in locations[city]['rainfall_stations']:
            try:
                r = requests.get("https://www2.sepa.org.uk/rainfall/api/Hourly/" + station + "?all=true")
                if r.status_code != 200:
                    log.warning(f"Unsuccessful API call for source SEPA rainfall with status code {r.status_code}.", exc_info=True)
                    raise Exception(f"Unsuccessful API call with status code {r.status_code}")
            except Exception as e:
                log.warning(f"Exception occurred while fetching forecast for city {city} and source SEPA: {e}", exc_info=True)
                continue

            try:
                readings = r.json()[-4:]
                values = [float(reading['Value']) for reading in readings]
                max_rainfall.append(max(values))
                min_rainfall.append(min(values))
                avg_rainfall.append(mean(values))
                # SEPA's /Hourly/ endpoint returns mm per HOUR (not per 15 min like
                # DEFRA/NRW). The most recent reading IS the hourly total — don't sum.
                hourly_rainfall.append(values[-1])
            except ValueError:
                log.warning(f"ValueError occurred for station {station}.", exc_info=True)
                continue
    else:
        log.warning(f"Invalid country for city {city}.", exc_info=True)

    # Aggregate across stations: max/min are extremes, avg/hourly are city-mean.
    try:
        max_rainfall = max(max_rainfall)
        min_rainfall = min(min_rainfall)
        avg_rainfall = mean(avg_rainfall)
        hourly_rainfall = mean(hourly_rainfall)
    except ValueError:
        log.warning(f"ValueError occurred while calculating rainfall statistics for city {city}.", exc_info=True)
        max_rainfall = None
        min_rainfall = None
        avg_rainfall = None
        hourly_rainfall = None

    observation_df = pd.DataFrame(met_office_data, index = [0])[['datetime','temperature', 'wind_speed', 'wind_direction']].rename(columns={ 'temperature': 'Temperature', 'wind_speed': 'WindSpeed', 'wind_direction': 'WindDirection'})

    observation_df['city'] = city
    observation_df['MaxRainfall'] = max_rainfall
    observation_df['MinRainfall'] = min_rainfall
    observation_df['AvgRainfall'] = avg_rainfall
    observation_df['HourlyRainfall'] = hourly_rainfall

    _dt = pd.to_datetime(observation_df['datetime'], format='ISO8601')
    observation_df['Date'] = _dt.dt.date
    observation_df['Time'] = _dt.dt.time
    observation_df = observation_df.drop(columns=['datetime'])

    return observation_df

### ACCUWEATHER HOURLY forecast parser

def parse_hourly_accuweather(raw_forecast_data):
    temp_df = pd.DataFrame(raw_forecast_data)
    _dt = pd.to_datetime(temp_df['DateTime'], format='ISO8601')
    mask = (_dt.dt.tz_convert('UTC').dt.tz_localize(None) >= pd.Timestamp(hour_now)).to_numpy()
    temp_df = temp_df[mask].reset_index(drop=True)
    _dt = _dt[mask].reset_index(drop=True)
    hourly_accuweather = pd.DataFrame()
    hourly_accuweather['Date'] = _dt.dt.date
    hourly_accuweather['Time'] = _dt.dt.time
    hourly_accuweather['ForecastTaken'] = hour_now

    hourly_accuweather['Temperature'] = temp_df['Temperature'].str.get('Value').astype(float)
    hourly_accuweather['WindSpeed'] = temp_df['Wind'].str.get('Speed').str.get('Value').astype(float)
    hourly_accuweather['WindDirection'] = temp_df['Wind'].str.get('Direction').str.get('Degrees').astype(float)
    hourly_accuweather['RainProbability'] = temp_df['RainProbability'].astype(float)
    hourly_accuweather['RainVolume'] = temp_df['Rain'].str.get('Value').astype(float)

    return hourly_accuweather

### ACCUWEATHER DAILY forecast parser

def parse_daily_accuweather(raw_forecast_data):

    temp_df = pd.DataFrame(raw_forecast_data['DailyForecasts'])
    daily_accuweather = pd.DataFrame()
    daily_accuweather['Date'] = pd.to_datetime(temp_df['Date'], format='ISO8601').dt.date
    daily_accuweather['ForecastTaken'] = hour_now

    # Temperature.Minimum/Maximum are already full-day extrema, no Day/Night split.
    daily_accuweather['MinTemperature'] = temp_df['Temperature'].str.get('Minimum').str.get('Value').astype(float)
    daily_accuweather['MaxTemperature'] = temp_df['Temperature'].str.get('Maximum').str.get('Value').astype(float)

    # AccuWeather splits each day into Day (~6am-6pm) and Night (~6pm-6am) blocks.
    # Combine into single daily figures consistent with the observation aggregates:
    #   - WindSpeed: mean of day and night (matches obs daily mean)
    #   - WindDirection: resultant vector via sin/cos average then atan2
    #   - RainProbability: max of day and night (matches MetOffice convention; the
    #     observation rain target is `> 0` so a daytime-only probability would
    #     under-represent overnight rain)
    #   - RainVolume: day + night total (full 24h rainfall accumulation)
    day_ws   = temp_df.Day.str.get('Wind').str.get('Speed').str.get('Value').astype(float)
    night_ws = temp_df.Night.str.get('Wind').str.get('Speed').str.get('Value').astype(float)
    daily_accuweather['WindSpeed'] = pd.concat([day_ws, night_ws], axis=1).mean(axis=1)

    day_dir   = temp_df.Day.str.get('Wind').str.get('Direction').str.get('Degrees').astype(float)
    night_dir = temp_df.Night.str.get('Wind').str.get('Direction').str.get('Degrees').astype(float)
    avg_sin = (np.sin(np.deg2rad(day_dir)) + np.sin(np.deg2rad(night_dir))) / 2
    avg_cos = (np.cos(np.deg2rad(day_dir)) + np.cos(np.deg2rad(night_dir))) / 2
    daily_accuweather['WindDirection'] = (np.rad2deg(np.arctan2(avg_sin, avg_cos)) % 360).astype(float)

    day_rp   = temp_df.Day.str.get('RainProbability').astype(float)
    night_rp = temp_df.Night.str.get('RainProbability').astype(float)
    daily_accuweather['RainProbability'] = pd.concat([day_rp, night_rp], axis=1).max(axis=1)

    day_rv   = temp_df.Day.str.get('Rain').str.get('Value').astype(float)
    night_rv = temp_df.Night.str.get('Rain').str.get('Value').astype(float)
    daily_accuweather['RainVolume'] = pd.concat([day_rv, night_rv], axis=1).sum(axis=1, min_count=1)

    return daily_accuweather

### OPENMETEO HOURLY forecast parser

open_meteo_cols = {"temperature_2m": "Temperature",
        "precipitation_probability": "RainProbability", 
        "rain": "RainVolume", 
        "wind_speed_10m": "WindSpeed", 
        "wind_direction_10m": "WindDirection",
        "temperature_2m_max": "MaxTemperature", 
        "temperature_2m_min": "MinTemperature", 
        "precipitation_probability_max": "RainProbability", 
        "rain_sum": "RainVolume", 
        "wind_speed_10m_max": "WindSpeed", 
        "wind_direction_10m_dominant": "WindDirection",
        "time":"Time"}

def parse_hourly_openmeteo(raw_forecast_data):

    hourly_OpenMeteo = pd.DataFrame(raw_forecast_data['hourly']).rename(mapper=open_meteo_cols, axis=1)
    _dt = pd.to_datetime(hourly_OpenMeteo['Time'])
    mask = (_dt >= pd.Timestamp(hour_now)).to_numpy()
    hourly_OpenMeteo = hourly_OpenMeteo[mask].reset_index(drop=True)
    _dt = _dt[mask].reset_index(drop=True)
    hourly_OpenMeteo['Date'] = _dt.dt.date
    hourly_OpenMeteo['Time'] = _dt.dt.time
    hourly_OpenMeteo['ForecastTaken'] = hour_now
    return hourly_OpenMeteo

### OPENMETEO DAILY forecast parser

def parse_daily_openmeteo(raw_forecast_data):
    daily_OpenMeteo = pd.DataFrame(raw_forecast_data['daily']).rename(mapper=open_meteo_cols, axis=1).rename(mapper={'Time': 'Date'}, axis=1)
    daily_OpenMeteo['ForecastTaken'] = hour_now
    return daily_OpenMeteo

### METOFFICE HOURLY forecast parser

metoffice_cols = {"screenTemperature": "Temperature",
        "probOfPrecipitation": "RainProbability", 
        "windSpeed10m": "WindSpeed", 
        "windDirectionFrom10m": "WindDirection",
        "time":"Time",
        "dayMaxScreenTemperature": "MaxTemperature",
        "nightMinScreenTemperature": "MinTemperature",
        "totalPrecipAmount": "RainVolume", 
        "midday10MWindSpeed": "WindSpeed", 
        "midday10MWindDirection": "WindDirection"}

def parse_hourly_metoffice(raw_forecast_data):
    data = raw_forecast_data['features'][0]['properties']['timeSeries']
    hourly_MetOffice = pd.DataFrame(data)[['time','screenTemperature', 'windSpeed10m','windDirectionFrom10m','totalPrecipAmount','probOfPrecipitation']].rename(mapper = metoffice_cols, axis=1)
    _dt = pd.to_datetime(hourly_MetOffice['Time'], format='ISO8601')
    mask = (_dt.dt.tz_convert('UTC').dt.tz_localize(None) >= pd.Timestamp(hour_now)).to_numpy()
    hourly_MetOffice = hourly_MetOffice[mask].reset_index(drop=True)
    _dt = _dt[mask].reset_index(drop=True)
    hourly_MetOffice['Date'] = _dt.dt.date
    hourly_MetOffice['Time'] = _dt.dt.time
    hourly_MetOffice['ForecastTaken'] = hour_now
    return hourly_MetOffice

### METOFFICE DAILY forecast parser

def parse_daily_metoffice(raw_forecast_data):
    data = raw_forecast_data['features'][0]['properties']['timeSeries']
    daily_MetOffice = pd.DataFrame(data)[['time','dayMaxScreenTemperature', 'nightMinScreenTemperature', 'midday10MWindSpeed','midday10MWindDirection','dayProbabilityOfPrecipitation', 'nightProbabilityOfPrecipitation']].rename(mapper = metoffice_cols, axis=1)
    
    daily_MetOffice['Date'] = pd.to_datetime(daily_MetOffice['Time'], format='ISO8601').dt.date
    daily_MetOffice['ForecastTaken'] = hour_now
    daily_MetOffice['RainProbability'] = daily_MetOffice[['dayProbabilityOfPrecipitation', 'nightProbabilityOfPrecipitation']].max(axis=1)
    daily_MetOffice = daily_MetOffice[['Date', 'ForecastTaken', 'MaxTemperature', 'MinTemperature', 'WindSpeed', 'WindDirection', 'RainProbability']]
    return daily_MetOffice

if __name__ == "__main__":

    # DataBase initialisation if not already created

    engine = create_engine(os.environ["DATABASE_URL"])

    with engine.connect() as con:
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS observations (
                id SERIAL PRIMARY KEY,
                "Date" DATE,
                "Time" TIME,
                "city" TEXT,
                "Temperature" REAL,
                "WindSpeed" REAL,
                "WindDirection" TEXT,
                "MaxRainfall" REAL,
                "MinRainfall" REAL,
                "AvgRainfall" REAL,
                "HourlyRainfall" REAL
            )
        """))
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS hourly_forecast (
                id SERIAL PRIMARY KEY,
                "Date" DATE,
                "Time" TIME,
                "city" TEXT,
                "model" TEXT,
                "ForecastTaken" TIMESTAMP,
                "Temperature" REAL,
                "WindSpeed" REAL,
                "WindDirection" REAL,
                "RainProbability" REAL,
                "RainVolume" REAL
            )
        """))
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_forecast (
                id SERIAL PRIMARY KEY,
                "Date" DATE,
                "city" TEXT,
                "model" TEXT,
                "ForecastTaken" TIMESTAMP,
                "MinTemperature" REAL,
                "MaxTemperature" REAL,
                "WindSpeed" REAL,
                "WindDirection" REAL,
                "RainProbability" REAL,
                "RainVolume" REAL
            )
        """))
        con.commit()
    
    # Commit forecast and observation data to databases

    hourly_sources = [
        ('AccuWeather', parse_hourly_accuweather),
        ('OpenMeteo-ECMWF', parse_hourly_openmeteo),
        ('OpenMeteo-GFSHRRR', parse_hourly_openmeteo),
        ('MetOffice', parse_hourly_metoffice)
        ]

    daily_sources = [
        ('AccuWeather', parse_daily_accuweather),
        ('OpenMeteo-ECMWF', parse_daily_openmeteo),
        ('OpenMeteo-GFSHRRR', parse_daily_openmeteo),
        ('MetOffice', parse_daily_metoffice)
        ]

    with engine.connect() as con:
        for city in locations.keys():
            try:
                get_hourly_data(city).to_sql("observations", con, if_exists='append', index=False)
            except Exception as e:
                log.warning(f"Exception occurred while fetching and storing observations for city {city}: {e}", exc_info=True)

            for source, parser in hourly_sources:
                try:
                    parser(retrieve_forecast(*get_params('hourly', city, source))).assign(Model=source, city=city).to_sql("hourly_forecast", con, if_exists='append', index=False)
                except Exception as e:
                    log.warning(f"Exception occurred while fetching and storing hourly forecast for city {city}: {e}", exc_info=True)

            for source, parser in daily_sources:
                try:
                    parser(retrieve_forecast(*get_params('daily', city, source))).assign(Model=source, city=city).to_sql("daily_forecast", con, if_exists='append', index=False)
                except Exception as e:
                    log.warning(f"Exception occurred while fetching and storing daily forecast for city {city}: {e}", exc_info=True)
        con.commit()
    log.info(f"Data successfully retrieved and stored in database.")

    # Retain 1 year of all data to support ensemble model training with full
    # seasonal cycle and forecast-evolution features.

    with engine.connect() as con:
        con.execute(text("DELETE FROM observations WHERE \"Date\" < NOW() - INTERVAL '365 days'"))
        con.execute(text("DELETE FROM hourly_forecast WHERE \"ForecastTaken\" < NOW() - INTERVAL '365 days'"))
        con.execute(text("DELETE FROM daily_forecast WHERE \"ForecastTaken\" < NOW() - INTERVAL '365 days'"))
        con.execute(text("DELETE FROM ensemble_forecast WHERE \"ForecastTaken\" < NOW() - INTERVAL '365 days'"))
        con.commit()
    

    if time.strftime("%H", time.localtime()) == '01':
        # Populate yesterday's MetOffice daily RainVolume by summing its hourly forecasts.
        # Correlated by (city, ForecastTaken) so each daily row gets the sum of its OWN
        # hourly forecasts, and scoped to Model = 'MetOffice' so we don't overwrite the
        # other sources' API-provided daily totals.
        with engine.connect() as con:
            con.execute(text("""
                UPDATE daily_forecast df
                SET "RainVolume" = sub.daily_sum
                FROM (
                    SELECT city, "ForecastTaken", SUM("RainVolume") AS daily_sum
                    FROM hourly_forecast
                    WHERE "Model" = 'MetOffice'
                      AND "Date" = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY city, "ForecastTaken"
                ) sub
                WHERE df."Date" = CURRENT_DATE - INTERVAL '1 day'
                  AND df."Model" = 'MetOffice'
                  AND df.city = sub.city
                  AND df."ForecastTaken" = sub."ForecastTaken"
            """))
            con.commit()


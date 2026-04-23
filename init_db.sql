CREATE TABLE IF NOT EXISTS hourly_forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Date DATE,
    Time TIME,    
    city TEXT, 
    Source TEXT, 
    ForecastTaken DATETIME,
    Temperature REAL, 
    WindSpeed REAL, 
    WindDirection REAL,
    RainProbability REAL, 
    RainVolume REAL
);

CREATE TABLE IF NOT EXISTS daily_forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Date DATE,
    city TEXT, 
    Source TEXT, 
    ForecastTaken DATETIME, 
    MinTemperature REAL, 
    MaxTemperature REAL, 
    WindSpeed REAL,
    WindDirection REAL, 
    RainProbability REAL, 
    RainVolume REAL
);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Date DATE,
    Time TIME,
    city TEXT, 
    Temperature REAL, 
    WindSpeed REAL, 
    WindDirection REAL,
    MaxRainfall REAL, 
    MinRainfall REAL, 
    AvgRainfall REAL
);
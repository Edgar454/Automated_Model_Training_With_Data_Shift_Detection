import os
import openmeteo_requests
import argparse

import pandas as pd
import requests_cache
from retry_requests import retry
from pathlib import Path

# Load environment variables from .env file
parent_dir = Path(__file__).resolve().parents[2]  
DATA_PATH = parent_dir / 'data'

# Argument parser to get start and end dates from command line
parser = argparse.ArgumentParser(description="Script to scrape weather data for Brazzaville, Congo.")
parser.add_argument("--start_date", type=str, required=True, help="Start date for weather data in YYYY-MM-DD format.")
parser.add_argument("--end_date", type=str, required=True, help="End date for weather data in YYYY-MM-DD format.")
args = parser.parse_args()

# Coordinates for Brazzaville, Congo
Brazzaville_coordinates = {
    "latitude": -4.2661,
    "longitude": 15.2832
}

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"


def get_weather_data(start_date , end_date , **kwargs):
    """
    Helper function to get weather data for Brazzaville from Open-Meteo API."""

    params = {
        "latitude": Brazzaville_coordinates["latitude"],
        "longitude": Brazzaville_coordinates["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max","temperature_2m_min","rain_sum",
                "relative_humidity_2m_max","relative_humidity_2m_min",
                "wind_speed_10m_max","wind_speed_10m_min","wind_speed_10m_mean",
                "relative_humidity_2m_min","cloudcover_mean","surface_pressure_mean","precipitation_hours"],
        "wind_speed_unit": "ms",
        "temperature_unit": "celsius"
    }

    try :
        responses = openmeteo.weather_api(url, params=params)

        # Process first and only location
        response = responses[0]

        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
        daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
        daily_rain_sum = daily.Variables(2).ValuesAsNumpy()
        daily_relative_humidity_2m_max = daily.Variables(3).ValuesAsNumpy()
        daily_relative_humidity_2m_min = daily.Variables(4).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(5).ValuesAsNumpy()
        daily_wind_speed_10m_min = daily.Variables(6).ValuesAsNumpy()
        daily_wind_speed_10m_mean = daily.Variables(7).ValuesAsNumpy()
        daily_relative_humidity_2m_mean = daily.Variables(8).ValuesAsNumpy()
        daily_cloudcover_mean = daily.Variables(9).ValuesAsNumpy()
        daily_surface_pressure_mean = daily.Variables(10).ValuesAsNumpy()
        daily_precipitation_hours = daily.Variables(11).ValuesAsNumpy()

        # Create a DataFrame with the daily data
        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}

        daily_data["temperature_2m_max (°C)"] = daily_temperature_2m_max
        daily_data["temperature_2m_min (°C)"] = daily_temperature_2m_min
        daily_data["rain_sum (mm)"] = daily_rain_sum
        daily_data["relative_humidity_2m_max (%)"] = daily_relative_humidity_2m_max
        daily_data["relative_humidity_2m_min (%)"] = daily_relative_humidity_2m_min
        daily_data["wind_speed_10m_max (m/s)"] = daily_wind_speed_10m_max
        daily_data["wind_speed_10m_min (m/s)"] = daily_wind_speed_10m_min
        daily_data["wind_speed_10m_mean (m/s)"] = daily_wind_speed_10m_mean
        daily_data["relative_humidity_2m_mean (%)"] = daily_relative_humidity_2m_mean
        daily_data["cloudcover_mean (%)"] = daily_cloudcover_mean
        daily_data["surface_pressure_mean (hPa)"] = daily_surface_pressure_mean
        daily_data["precipitation_hours"] = daily_precipitation_hours

        # Create a DataFrame and save it to CSV
        daily_dataframe = pd.DataFrame(data = daily_data)
        daily_dataframe.set_index("date", inplace = True)

        filename = f"brazzaville_weather_data_{start_date}-{end_date}.csv"
        daily_dataframe.to_csv(os.path.join(DATA_PATH, filename))

        if 'ti' in kwargs:
            ti = kwargs['ti']
            ti.xcom_push(key='weather_filename', value=filename)

    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None
    
if __name__ == "__main__":
    start_date = args.start_date
    end_date = args.end_date
    get_weather_data(start_date,end_date)
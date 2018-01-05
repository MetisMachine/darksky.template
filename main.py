# # Weather Forecast Sample Data Ingest
# This template fetches the current forecast for a particular zip code and persists those data to the Metis Machine data store.
# This example actually leverages two external APIs, one to query geographic coordinates for a given zip code, and the second to fetch weather data for those coordinates.

import os
import requests
from datetime import datetime
import pandas as pd


from skafossdk import *
print('initializing the SDK connection')
skafos = Skafos()


# Weather API Details
weather_api_key = os.environ['DARKSKY_KEY']
weather_api_url = "https://api.darksky.net/forecast/"

# ZipCode to Long, Lat API Details
location_api_key = os.environ['ZIPCODEAPI_KEY']
location_api_url = "https://www.zipcodeapi.com/rest/"   


def get_location_for_zip(zipcode):
    """ use the zipcodeapi.com endpoint
        Args:
            zipcode (int): the requested location by zip code, cast to string if not already
        Returns:
            dict: dictionary containing keys 'lng' and 'lat'
    """
    url = location_api_url + location_api_key + '/info.json/' + str(zipcode).strip() + '/degrees'
    print('fetching {}'.format(url))
    return requests.get(url).json()


def get_forecast(lon, lat):
    """ Use the darksky.net endpoint
        Args:
            lon (float): longitude (x coordinate) to request weather forecast for
            lat (float): lattitude (y coordinate) to request weather forecast for
        Returns:
            dict: the darksky forecast json as a dictionary
    """
    url = weather_api_url + weather_api_key + '/{},{}'.format(lat, lon)
    print('fetching {}'.format(url))
    return requests.get(url).json()


location_zipcodes = ["23250"]  # 23250 Richmond Airport


def forecast_rows(zipcodes):
    """ Map a list of zipcodes into individual data rows containing forecast data per day
        Args:
            zipcodes (list(str)): locations to fetch weather forecasts for
        Returns:
            list(dict): data rows per forecast day, per location
    """
    date_fetched = datetime.now()
    for zipcode in zipcodes:
        ll = get_location_for_zip(zipcode)
        forecast = get_forecast(ll['lng'], ll['lat'])
        for day in forecast['daily']['data']:
            yield {
                'source': 'Darksky',
                'date_fetched': date_fetched,
                'date': datetime.fromtimestamp(day['time']),
                'zipcode': zipcode,
                'latitude': ll['lat'],
                'longitude': ll['lng'],
                'tmax': day['temperatureHigh'],
                'tmin': day['temperatureLow'],
                'humidity': day['humidity'],
                'wind_speed': day['windSpeed'],
                'pressure': day['pressure'],
                'precip_total': day['precipIntensityMax'],
                'precip_prob': day['precipProbability'],
                'sunrise': datetime.fromtimestamp(day['sunriseTime']),
                'sunset': datetime.fromtimestamp(day['sunsetTime']),
                'cloud_cover': day['cloudCover'],
                'heat_index': day['apparentTemperatureHigh']
            }


# forecast_rows returns a list of dictionaries, which is directly convertable to a Pandas dataframe
forecast_data = pd.DataFrame(forecast_rows(location_zipcodes))


# cast datetimes to just date for persisting to the database
forecast_data['date'] = forecast_data['date'].apply(lambda d: d.date())
forecast_data['date_fetched'] = forecast_data['date_fetched'].apply(lambda d: d.date())


# validate that the returned data is what we expect
forecast_data.iloc[:3]


# ### Persist forecast data
# Save these forecast data for later use via the Skafos SDK. This requires specifying a schema for how we want to store these records.

# types here are as-specified in SQL (CQL really) rather than python
schema = {
    "table_name": "weather_forecast_by_zip",
    "options": {
        "primary_key": ['date', 'date_fetched', 'zipcode', 'source'],
        "order_by": ['date_fetched desc']
    },
    "columns": {
        'source': 'text',
        'date_fetched': 'date',
        'date': 'date',
        'zipcode': 'text',
        'latitude': 'float',
        'longitude': 'float',
        'tmax': 'float',
        'tmin': 'float',
        'humidity': 'float',
        'wind_speed': 'float',
        'pressure': 'float',
        'precip_total': 'float',
        'precip_prob': 'float',
        'sunrise': 'timestamp',
        'sunset': 'timestamp',
        'cloud_cover': 'float',
        'heat_index': 'float'
    }
}


data_out = forecast_data.dropna().to_dict(orient='records')


dataresult = skafos.engine.save(schema, data_out).result()


dataresult


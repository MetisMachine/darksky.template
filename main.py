
# coding: utf-8

# In[43]:


import os
import requests
from datetime import datetime
import pandas as pd


# In[11]:


# Weather API Details
weather_api_key = os.environ['DARKSKY_KEY']
weather_api_url = "https://api.darksky.net/forecast/"

# ZipCode to Long, Lat API Details
location_api_key = os.environ['ZIPCODEAPI_KEY']
location_api_url = "https://www.zipcodeapi.com/rest/"   


# In[12]:


def get_location_for_zip(zipcode):
    url = location_api_url + location_api_key + '/info.json/' + str(zipcode).strip() + '/degrees'
    print('fetching {}'.format(url))
    return requests.get(url).json()


# In[13]:


def get_forecast(lon, lat):
    url = weather_api_url + weather_api_key + '/{},{}'.format(lat, lon)
    print('fetching {}'.format(url))
    return requests.get(url).json()


# In[15]:


location_zipcodes = ["23250"]  # 23250 Richmond Airport


# In[34]:


forecasts = get_forecast_for_zips(location_zipcodes)


# In[40]:


def forecast_rows(zipcodes):
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


# In[46]:


forecast_data = pd.DataFrame(forecast_rows(location_zipcodes))


# In[47]:


forecast_data.iloc[:3]


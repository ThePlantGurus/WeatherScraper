import pyowm
import time
import requests
from datetime import datetime
from scraper.db.private import api_key, sql_password, sql_username
from scraper.db.db_connect import db_connect_f
from scraper.db.db_transactions import ProcessExecutionGeneration, ProcessExecutionComplete, CurrentWeatherTransaction, \
                                        ForecastHourlyWeatherTransaction, ForecastDailyWeatherTransaction

owm = pyowm.OWM(api_key)


def GetProcessExecution():
    ProgramName = 'WeatherScraper'
    connection = db_connect_f(sql_username, sql_password)
    connection.execute(ProcessExecutionGeneration.format(**vars()))
    ProcessExecutionID = connection.fetchval()
    connection.commit()
    return ProcessExecutionID


class Package:
    def __init__(self, ProcessExecutionID):
        self.ProcessExecutionID = ProcessExecutionID

    def Main(self, latitude, longitude, key):
        self.RecordCount = 0
        #ProcessExecutionID = self.ProcessExecutionID
        connection = db_connect_f(sql_username, sql_password)
        print('starting current weather')
        observation = owm.weather_at_coords(latitude, longitude)
        weather = observation.get_weather()
        self.RecordCount += 1
        temperature = weather.get_temperature('fahrenheit')['temp']
        temp_max = weather.get_temperature('fahrenheit')['temp_max']
        temp_min = weather.get_temperature('fahrenheit')['temp_min']
        #reference_time = datetime.strptime(weather.get_reference_time(timeformat='iso')[:19], '%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y %H:%M:%S')
        sunrise = datetime.strptime(weather.get_sunrise_time(timeformat='iso')[:19], '%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y %H:%M:%S')
        sunset = datetime.strptime(weather.get_sunset_time(timeformat='iso')[:19], '%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y %H:%M:%S')
        clouds = weather.get_clouds()
        try:
            rain1hr = weather.get_rain()['1h']
        except:
            rain1hr = 0
        try:
            rain3hr = weather.get_rain()['3h']
        except:
            rain3hr = 0
        try:
            snow1hr = weather.get_snow()['1h']
        except:
            snow1hr = 0
        try:
            snow3hr = weather.get_snow()['3h']
        except:
            snow3hr = 0
        try:
            wind = weather.get_wind('miles_hour')['speed']
        except:
            wind = 0
        humidity = weather.get_humidity()
        status = weather.get_status()
        detailed_status = weather.get_detailed_status()
        code = weather.get_weather_code()
        retry_flag = True
        retry_count = 0
        #print(CurrentWeatherTransaction.format(**vars()))
        while retry_flag and retry_count < 5:
            try:
                connection.execute(CurrentWeatherTransaction.format(**vars()))
                connection.commit()
                retry_flag = False
            except:
                print('Retry after 1 second')
                retry_count = retry_count + 1
                time.sleep(1)

        print('starting hourly forecasts')
        url = 'https://api.openweathermap.org/data/2.5/onecall?lat={latitude}&lon=-{longitude}&units=imperial&%exclude=minutely,daily,current&appid={key}'
        resp = requests.get(url.format(**vars()))
        if resp.status_code != 200:
            raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        all_data = resp.json()
        all_data = all_data['hourly']
        for weather in all_data:
            self.RecordCount += 1
            reference_time = datetime.utcfromtimestamp(weather['dt']).strftime('%m/%d/%Y %H:%M:%S')
            temperature = weather['temp']
            clouds = weather['clouds']
            try:
                rain1hr = weather['rain']['1h']
            except:
                rain1hr = 0
            try:
                snow1hr = weather['snow']['1h']
            except:
                snow1hr = 0
            try:
                wind = weather['wind_speed']
            except:
                wind = 0
            humidity = weather['humidity']
            ws = weather['weather']
            for ob in ws:
                status = ob['main']
                detailed_status = ob['description']
                code = ob['id']
            retry_flag = True
            retry_count = 0
            #print(ForecastHourlyWeatherTransaction.format(**vars()))
            while retry_flag and retry_count < 5:
                try:
                    connection.execute(ForecastHourlyWeatherTransaction.format(**vars()))
                    connection.commit()
                    retry_flag = False
                except:
                    print('Retry after 1 second')
                    retry_count = retry_count + 1
                    time.sleep(1)

        print('starting daily forecasts')
        #work around, just calling api directly
        url = 'https://api.openweathermap.org/data/2.5/onecall?lat={latitude}&lon=-{longitude}&units=imperial&%exclude=minutely,hourly,current&appid={key}'
        resp = requests.get(url.format(**vars()))
        if resp.status_code != 200:
            raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        all_data = resp.json()
        all_data = all_data['daily']
        for weather in all_data:
            self.RecordCount += 1
            reference_time = datetime.utcfromtimestamp(weather['dt']).strftime('%m/%d/%Y %H:%M:%S')
            min_temp = weather['temp']['min']
            morn_temp = weather['temp']['morn']
            day_temp = weather['temp']['day']
            even_temp = weather['temp']['eve']
            night_temp = weather['temp']['night']
            max_temp = weather['temp']['max']
            sunrise = datetime.utcfromtimestamp(weather['sunrise']).strftime('%m/%d/%Y %H:%M:%S')
            sunset = datetime.utcfromtimestamp(weather['sunset']).strftime('%m/%d/%Y %H:%M:%S')
            clouds = weather['clouds']
            try:
                rain = weather['rain']
            except:
                rain = 0
            try:
                snow = weather['snow']
            except:
                snow = 0
            try:
                wind = weather['wind_speed']
            except:
                wind = 0
            humidity = weather['humidity']
            ws = weather['weather']
            for ob in ws:
                status = ob['main']
                detailed_status = ob['description']
                code = ob['id']
            retry_flag = True
            retry_count = 0
            while retry_flag and retry_count < 5:
                try:
                    connection.execute(ForecastDailyWeatherTransaction.format(**vars()))
                    connection.commit()
                    retry_flag = False
                except:
                    print('Retry after 1 second')
                    retry_count = retry_count + 1
                    time.sleep(1)


    def CompleteProcessExecution(self):
        connection = db_connect_f(sql_username, sql_password)
        connection.execute(ProcessExecutionComplete.format(ProcessExecutionID=self.ProcessExecutionID, RecordCount=self.RecordCount))
        connection.commit()
        connection.close()


#p = Package(1)
#p.Main(40.8299, 96.7012, api_key)
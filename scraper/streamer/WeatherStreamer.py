import pyowm
import time
from datetime import datetime
from scraper.db.private import api_key, sql_password, sql_username
from scraper.db.db_connect import db_connect_f
from scraper.db.db_transactions import ProcessExecutionGeneration, ProcessExecutionComplete, CurrentWeatherTransaction, ForecastWeatherTransaction

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

    def Main(self, zipcode):
        self.RecordCount = 0
        #ProcessExecutionID = self.ProcessExecutionID
        connection = db_connect_f(sql_username, sql_password)
        print('starting current weather')
        observation = owm.weather_at_zip_code(zipcode, 'US')
        weather = observation.get_weather()
        self.RecordCount += 1
        temperature = weather.get_temperature('fahrenheit')['temp']
        temp_max = weather.get_temperature('fahrenheit')['temp_max']
        temp_min = weather.get_temperature('fahrenheit')['temp_min']
        reference_time = datetime.strptime(weather.get_reference_time(timeformat='iso')[:19], '%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y %H:%M:%S')
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

    def CompleteProcessExecution(self):
        connection = db_connect_f(sql_username, sql_password)
        connection.execute(ProcessExecutionComplete.format(ProcessExecutionID=self.ProcessExecutionID, RecordCount=self.RecordCount))
        connection.commit()
        connection.close()


p = Package(1)
p.Main('68508')
from scraper.streamer.WeatherStreamer import Package, GetProcessExecution
from scraper.db.private import api_key

if __name__ == '__main__':
    print('starting WeatherScraper')
    p = Package(GetProcessExecution())
    print("Package ExecutionID: ", p.ProcessExecutionID)
    p.Main(40.8299, 96.7012, api_key)
    print("Records Pulled: ", p.RecordCount)
    p.CompleteProcessExecution()
    print("Process Completed")
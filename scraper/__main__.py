from scraper.streamer.WeatherStreamer import Package, GetProcessExecution

if __name__ == '__main__':
    print('starting WeatherScraper')
    p = Package(GetProcessExecution())
    print("Package ExecutionID: ", p.ProcessExecutionID)
    p.Main()
    print("Records Pulled: ", p.RecordCount)
    p.CompleteProcessExecution()
    print("Process Completed")
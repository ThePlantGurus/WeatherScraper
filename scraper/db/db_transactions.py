CurrentWeatherTransaction = """
DECLARE @DateOfWeather DATE = CAST(GETDATE() AS DATE),
		@IsCurrent BIT = 1,
		@MinTemperatureF DECIMAL(17,2) = {temp_max},
		@TemperatureF DECIMAL(17,2) = {temperature},
		@MaxTemperatureF DECIMAL(17,2) = {temp_min},
		@DateOfReference DATETIMEOFFSET(2) = CAST('{reference_time}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@SunriseTime DATETIMEOFFSET(2) = CAST('{sunrise}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@SunsetTime DATETIMEOFFSET(2) = CAST('{sunset}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@WeatherStatusID DECIMAL(21,6) = {code},
		@WindSpeedMPH INT = {wind},
		@CloudCoverage INT = {clouds},
		@HumidityPercentage INT = {humidity},
		@RainLastHourMM INT = {rain1hr},
		@RainLastThreeHoursMM INT = {rain3hr},
		@SnowLastHourMM INT = {snow1hr},
		@SnowLastThreeHoursMM INT = {snow3hr},
		@WeatherStatusName NVARCHAR(50) = '{status}',
		@WeatherStatusDetails NVARCHAR(50) = '{detailed_status}',
		--@ProcessExecutionID BIGINT = ProcessExecutionID,
		--@ProgramID INT = ProgramID,
		@CurrentWeatherRowHash VARBINARY(32),
		@ErrorMessage NVARCHAR(MAX),
		@ErrorSeverity INT,
		@ErrorState INT,
		@xstate INT

	BEGIN TRY
		BEGIN TRANSACTION
			SET @CurrentWeatherRowHash = CAST(HASHBYTES('SHA2_256', CAST(ISNULL(@DateOfWeather,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@MinTemperatureF,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@TemperatureF,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@MaxTemperatureF,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@DateOfReference,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@SunriseTime,'') AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@SunsetTime,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@WeatherStatusID,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@WindSpeedMPH,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@CloudCoverage,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@HumidityPercentage,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@RainLastHourMM,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@RainLastThreeHoursMM,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@SnowLastHourMM,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@SnowLastThreeHoursMM,0) AS NVARCHAR(50))) AS VARBINARY(32))

			IF NOT EXISTS (SELECT 1 FROM owm.WeatherStatus WHERE WeatherStatusID = @WeatherStatusID)
				INSERT INTO owm.WeatherStatus(WeatherStatusID, WeatherStatusName, WeatherStatusDetails)
				VALUES (@WeatherStatusID, @WeatherStatusName, @WeatherStatusDetails)

			IF NOT EXISTS (SELECT 1 FROM owm.CurrentWeather WHERE DateOfWeather = @DateOfWeather AND RowHash = @CurrentWeatherRowHash)
				UPDATE owm.CurrentWeather SET IsCurrent = 0, UpdatedOn = SYSDATETIMEOFFSET() WHERE DateOfWeather = @DateOfWeather

				INSERT INTO owm.CurrentWeather (DateOfWeather, IsCurrent, MinTemperatureF, TemperatureF, MaxTemperatureF, DateOfReference, SunriseTime, SunsetTime,
												WeatherStatusID, WindSpeedMPH, CloudCoverage, HumidityPercentage, RainLastHourMM, RainLastThreeHoursMM,
												SnowLastHourMM, SnowLastThreeHoursMM, RowHash)
				VALUES (@DateOfWeather, @IsCurrent, @MinTemperatureF, @TemperatureF, @MaxTemperatureF, @DateOfReference, @SunriseTime, @SunsetTime, 
						@WeatherStatusID, @WindSpeedMPH, @CloudCoverage, @HumidityPercentage, @RainLastHourMM, @RainLastThreeHoursMM,
						@SnowLastHourMM, @SnowLastThreeHoursMM, @CurrentWeatherRowHash)

		COMMIT TRANSACTION
		END TRY
		BEGIN CATCH
			SELECT @ErrorMessage = ERROR_MESSAGE() + ' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) + ' Line: ' + CAST(ERROR_LINE() AS NVARCHAR) + ' Procedure: ' + ERROR_PROCEDURE()
				 , @ErrorSeverity = ERROR_SEVERITY()
				 , @ErrorState = ERROR_STATE()
				 , @xstate = XACT_STATE()

			IF @xstate = -1
				ROLLBACK TRANSACTION	
			--INSERT INTO etl.ProcessExecutionLogs (ProgramID, ProcessExecutionID, ErrorSeverity, ErrorState, X_State, ErrorMessage)
			--VALUES (@ProgramID, @ProcessExecutionID, @ErrorSeverity, @ErrorState, @xstate, @ErrorMessage)

		END CATCH
"""

ForecastWeatherTransaction = """

"""

ProcessExecutionGeneration = """
DECLARE @ProcessExecutionID INT
EXEC @ProcessExecutionID = etl.ProcessExecutionGeneration @ProgramName = {ProgramName}
SELECT @ProcessExecutionID AS ProcessExecutionID
"""

ProcessExecutionComplete = """
EXEC etl.ProcessExecutionComplete @ProcessExecutionID = {ProcessExecutionID}, @RecordCount = {RecordCount}
"""
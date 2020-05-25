CurrentWeatherTransaction = """
DECLARE @DateOfWeather DATE = CAST(GETDATE() AS DATE),
		@IsCurrent BIT = 1,
		@MinTemperatureF DECIMAL(17,2) = {temp_max},
		@TemperatureF DECIMAL(17,2) = {temperature},
		@MaxTemperatureF DECIMAL(17,2) = {temp_min},
		@SunriseTime DATETIMEOFFSET(2) = CAST('{sunrise}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@SunsetTime DATETIMEOFFSET(2) = CAST('{sunset}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@WeatherStatusID INT = {code},
		@WindSpeedMPH DECIMAL(21,6) = {wind},
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
		@xstate INT,
		@ProgramID INT = 2,
		@ProcessExecutionID BIGINT = {ProcessExecutionID}

	BEGIN TRY
		BEGIN TRANSACTION
			SET @CurrentWeatherRowHash = CAST(HASHBYTES('SHA2_256', CAST(ISNULL(@DateOfWeather,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@MinTemperatureF,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@TemperatureF,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@MaxTemperatureF,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@SunriseTime,'') AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@SunsetTime,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@WeatherStatusID,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@WindSpeedMPH,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@CloudCoverage,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@HumidityPercentage,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@RainLastHourMM,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@RainLastThreeHoursMM,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@SnowLastHourMM,0) AS NVARCHAR(50))+'|'+
																	CAST(ISNULL(@SnowLastThreeHoursMM,0) AS NVARCHAR(50))) AS VARBINARY(32))

			IF NOT EXISTS (SELECT 1 FROM owm.WeatherStatus WHERE WeatherStatusID = @WeatherStatusID)
				INSERT INTO owm.WeatherStatus(WeatherStatusID, WeatherStatusName, WeatherStatusDetails)
				VALUES (@WeatherStatusID, @WeatherStatusName, @WeatherStatusDetails)

			IF NOT EXISTS (SELECT 1 FROM owm.CurrentWeather WHERE DateOfWeather = @DateOfWeather AND RowHash = @CurrentWeatherRowHash)
				UPDATE owm.CurrentWeather SET IsCurrent = 0, UpdatedOn = SYSDATETIMEOFFSET() --WHERE DateOfWeather = @DateOfWeather

			IF NOT EXISTS (SELECT 1 FROM owm.CurrentWeather WHERE DateOfWeather = @DateOfWeather AND RowHash = @CurrentWeatherRowHash)
				INSERT INTO owm.CurrentWeather (DateOfWeather, IsCurrent, MinTemperatureF, TemperatureF, MaxTemperatureF, SunriseTime, SunsetTime,
												WeatherStatusID, WindSpeedMPH, CloudCoverage, HumidityPercentage, RainLastHourMM, RainLastThreeHoursMM,
												SnowLastHourMM, SnowLastThreeHoursMM, RowHash)
				VALUES (@DateOfWeather, @IsCurrent, @MinTemperatureF, @TemperatureF, @MaxTemperatureF, @SunriseTime, @SunsetTime, 
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
			INSERT INTO etl.ProcessExecutionLogs (ProgramID, ProcessExecutionID, ErrorSeverity, ErrorState, X_State, ErrorMessage)
			VALUES (@ProgramID, @ProcessExecutionID, @ErrorSeverity, @ErrorState, @xstate, @ErrorMessage)

		END CATCH
"""

ForecastHourlyWeatherTransaction = """
DECLARE @DateOfWeatherForecast DATE = CAST((CAST('{reference_time}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time') AS DATE),
		@TimeOfWeatherForecast DATETIMEOFFSET(2) = CAST('{reference_time}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@IsCurrent BIT = 1,
		@TemperatureF DECIMAL(17,2) = {temperature},
		@WeatherStatusID INT = {code},
		@WindSpeedMPH DECIMAL(21,6) = {wind},
		@CloudCoverage INT = {clouds},
		@HumidityPercentage INT = {humidity},
		@RainForecastMM INT = {rain1hr},
		@SnowForecastMM INT = {snow1hr},
		@WeatherStatusName NVARCHAR(50) = '{status}',
		@WeatherStatusDetails NVARCHAR(50) = '{detailed_status}',
		--@ProcessExecutionID BIGINT = ProcessExecutionID,
		--@ProgramID INT = ProgramID,
		@HourlyWeatherForecastsRowHash VARBINARY(32),
		@ErrorMessage NVARCHAR(MAX),
		@ErrorSeverity INT,
		@ErrorState INT,
		@xstate INT,
		@ProgramID INT = 2,
		@ProcessExecutionID BIGINT = {ProcessExecutionID}

	BEGIN TRY
		BEGIN TRANSACTION
			SET @HourlyWeatherForecastsRowHash = CAST(HASHBYTES('SHA2_256', CAST(ISNULL(@DateOfWeatherForecast,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@TimeOfWeatherForecast,'') AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@TemperatureF,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@WeatherStatusID,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@WindSpeedMPH,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@CloudCoverage,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@HumidityPercentage,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@RainForecastMM,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@SnowForecastMM,0) AS NVARCHAR(50))) AS VARBINARY(32))

			IF NOT EXISTS (SELECT 1 FROM owm.WeatherStatus WHERE WeatherStatusID = @WeatherStatusID)
				INSERT INTO owm.WeatherStatus(WeatherStatusID, WeatherStatusName, WeatherStatusDetails)
				VALUES (@WeatherStatusID, @WeatherStatusName, @WeatherStatusDetails)
				
			IF NOT EXISTS (SELECT 1 FROM owm.HourlyWeatherForecasts WHERE TimeOfWeatherForecast = @TimeOfWeatherForecast AND RowHash = @HourlyWeatherForecastsRowHash)
				UPDATE owm.HourlyWeatherForecasts 
				SET IsCurrent = 0, 
					UpdatedOn = SYSDATETIMEOFFSET()
				WHERE TimeOfWeatherForecast = @TimeOfWeatherForecast
				
			IF NOT EXISTS (SELECT 1 FROM owm.HourlyWeatherForecasts WHERE TimeOfWeatherForecast = @TimeOfWeatherForecast AND RowHash = @HourlyWeatherForecastsRowHash)
				INSERT INTO owm.HourlyWeatherForecasts (DateOfWeatherForecast, TimeOfWeatherForecast, IsCurrent, TemperatureF,
												WeatherStatusID, WindSpeedMPH, CloudCoverage, HumidityPercentage, RainForecastMM,
												SnowForecastMM, RowHash)
				VALUES (@DateOfWeatherForecast, @TimeOfWeatherForecast, @IsCurrent, @TemperatureF,
						@WeatherStatusID, @WindSpeedMPH, @CloudCoverage, @HumidityPercentage, @RainForecastMM,
						@SnowForecastMM, @HourlyWeatherForecastsRowHash)

			;WITH NT AS (
				SELECT
				HourlyWeatherForecastID,
                LAG(TimeOfWeatherForecast,1,'1900-01-01') OVER (ORDER BY TimeOfWeatherForecast) AS PreviousTimeOfWeatherForecast
				FROM owm.HourlyWeatherForecasts
				WHERE IsCurrent=1
			)

			UPDATE O
			SET PreviousTimeOfWeatherForecast = NT.PreviousTimeOfWeatherForecast
			FROM owm.HourlyWeatherForecasts AS O
			INNER JOIN NT ON NT.HourlyWeatherForecastID = O.HourlyWeatherForecastID
						
		COMMIT TRANSACTION
		END TRY
		BEGIN CATCH
			SELECT @ErrorMessage = ERROR_MESSAGE() + ' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) + ' Line: ' + CAST(ERROR_LINE() AS NVARCHAR) + ' Procedure: ' + ERROR_PROCEDURE()
				 , @ErrorSeverity = ERROR_SEVERITY()
				 , @ErrorState = ERROR_STATE()
				 , @xstate = XACT_STATE()

			IF @xstate = -1
				ROLLBACK TRANSACTION	
			INSERT INTO etl.ProcessExecutionLogs (ProgramID, ProcessExecutionID, ErrorSeverity, ErrorState, X_State, ErrorMessage)
			VALUES (@ProgramID, @ProcessExecutionID, @ErrorSeverity, @ErrorState, @xstate, @ErrorMessage)

		END CATCH		
"""

ForecastDailyWeatherTransaction = """
DECLARE @DateOfWeatherForecast DATE = CAST((CAST('{reference_time}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time') AS DATE),
		@IsCurrent BIT = 1,
		@MorningTemperatureF DECIMAL(17,2) = {morn_temp},
		@DayTemperatureF DECIMAL(17,2) = {day_temp},
		@EveningTemperatureF DECIMAL(17,2) = {even_temp},
		@NightTemperatureF DECIMAL(17,2) = {night_temp},
		@MinTemperatureF DECIMAL(17,2) = {min_temp},
		@MaxTemperatureF DECIMAL(17,2) = {max_temp},
		@SunriseTime DATETIMEOFFSET(2) = CAST('{sunrise}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@SunsetTime DATETIMEOFFSET(2) = CAST('{sunset}' AS DATETIMEOFFSET(2)) AT TIME ZONE 'Central Standard Time',
		@WeatherStatusID INT = {code},
		@WindSpeedMPH DECIMAL(21,6) = {wind},
		@CloudCoverage INT = {clouds},
		@HumidityPercentage INT = {humidity},
		@RainForecastMM INT = {rain},
		@SnowForecastMM INT = {snow},
		@WeatherStatusName NVARCHAR(50) = '{status}',
		@WeatherStatusDetails NVARCHAR(50) = '{detailed_status}',
		--@ProcessExecutionID BIGINT = ProcessExecutionID,
		--@ProgramID INT = ProgramID,
		@DailyWeatherForecastsRowHash VARBINARY(32),
		@ErrorMessage NVARCHAR(MAX),
		@ErrorSeverity INT,
		@ErrorState INT,
		@xstate INT,
		@ProgramID INT = 2,
		@ProcessExecutionID BIGINT = {ProcessExecutionID}

	BEGIN TRY
		BEGIN TRANSACTION
			SET @DailyWeatherForecastsRowHash = CAST(HASHBYTES('SHA2_256', CAST(ISNULL(@DateOfWeatherForecast,'') AS NVARCHAR(50))+'|'+CAST(ISNULL(@MorningTemperatureF,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@DayTemperatureF,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@EveningTemperatureF,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@NightTemperatureF,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@MinTemperatureF,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@MaxTemperatureF,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@SunriseTime,'') AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@SunsetTime,'') AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@WeatherStatusID,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@WindSpeedMPH,0) AS NVARCHAR(50))+'|'+CAST(ISNULL(@CloudCoverage,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@HumidityPercentage,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@RainForecastMM,0) AS NVARCHAR(50))+'|'+
																			   CAST(ISNULL(@SnowForecastMM,0) AS NVARCHAR(50))) AS VARBINARY(32))

			IF NOT EXISTS (SELECT 1 FROM owm.WeatherStatus WHERE WeatherStatusID = @WeatherStatusID)
				INSERT INTO owm.WeatherStatus(WeatherStatusID, WeatherStatusName, WeatherStatusDetails)
				VALUES (@WeatherStatusID, @WeatherStatusName, @WeatherStatusDetails)
				
			IF NOT EXISTS (SELECT 1 FROM owm.DailyWeatherForecasts WHERE DateOfWeatherForecast = @DateOfWeatherForecast AND RowHash = @DailyWeatherForecastsRowHash)
				UPDATE owm.DailyWeatherForecasts 
				SET IsCurrent = 0, 
					UpdatedOn = SYSDATETIMEOFFSET()
				WHERE DateOfWeatherForecast = @DateOfWeatherForecast
				
			IF NOT EXISTS (SELECT 1 FROM owm.DailyWeatherForecasts WHERE DateOfWeatherForecast = @DateOfWeatherForecast AND RowHash = @DailyWeatherForecastsRowHash)
				INSERT INTO owm.DailyWeatherForecasts (DateOfWeatherForecast, IsCurrent, MorningTemperatureF, DayTemperatureF, EveningTemperatureF, NightTemperatureF,
												MinTemperatureF, MaxTemperatureF, SunriseTime, SunsetTime, 
												WeatherStatusID, WindSpeedMPH, CloudCoverage, HumidityPercentage, RainForecastMM,
												SnowForecastMM, RowHash)
				VALUES (@DateOfWeatherForecast, @IsCurrent, @MorningTemperatureF, @DayTemperatureF, @EveningTemperatureF, @NightTemperatureF,
						@MinTemperatureF, @MaxTemperatureF, @SunriseTime, @SunsetTime,
						@WeatherStatusID, @WindSpeedMPH, @CloudCoverage, @HumidityPercentage, @RainForecastMM,
						@SnowForecastMM, @DailyWeatherForecastsRowHash)
						
		COMMIT TRANSACTION
		END TRY
		BEGIN CATCH
			SELECT @ErrorMessage = ERROR_MESSAGE() + ' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) + ' Line: ' + CAST(ERROR_LINE() AS NVARCHAR) + ' Procedure: ' + ERROR_PROCEDURE()
				 , @ErrorSeverity = ERROR_SEVERITY()
				 , @ErrorState = ERROR_STATE()
				 , @xstate = XACT_STATE()

			IF @xstate = -1
				ROLLBACK TRANSACTION	
			INSERT INTO etl.ProcessExecutionLogs (ProgramID, ProcessExecutionID, ErrorSeverity, ErrorState, X_State, ErrorMessage)
			VALUES (@ProgramID, @ProcessExecutionID, @ErrorSeverity, @ErrorState, @xstate, @ErrorMessage)

		END CATCH		
"""

ProcessExecutionGeneration = """
DECLARE @ProcessExecutionID INT
EXEC @ProcessExecutionID = etl.ProcessExecutionGeneration @ProgramName = {ProgramName}
SELECT @ProcessExecutionID AS ProcessExecutionID
"""

ProcessExecutionComplete = """
EXEC etl.ProcessExecutionComplete @ProcessExecutionID = {ProcessExecutionID}, @RecordCount = {RecordCount}
"""
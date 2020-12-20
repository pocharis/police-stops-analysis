/*** Question 2 of Part 3. Accross the years, find the number of stops in various periods of the day. Example: For 2015, find the number of stops in early-morning, morning, afternoon or evening. 

Assume early morning to be between 12am to 6am, 
       Morning between  6am to 12noon, 
       Afternoon between 12noon to 6pm,
       Evening between 6pm to 12am.
 ***/



/* In order to correctly load the csv file, the CSVLoader 
User Defined Function is registered and Defined below. */
REGISTER /usr/lib/pig/piggybank.jar;

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- The MACRO defined in yearTimeMac.macro file is imported here and called later. 
IMPORT 'yearTimeMacro.pig';


-- The dataset is then loaded uysing the CSVLoader defined above.
policeStops_Dataset = LOAD 'terry-stops.csv' USING CSVLoader() AS (
				Subject_Age_Group:chararray,
				Stop_Resolution:chararray,
				Officer_ID:int,
				Officer_YOB:int,
				Officer_Gender:chararray,
				Officer_Race:chararray,
				Subject_Perceived_Race:chararray,
				Subject_Perceived_Gender:chararray,
				Reported_Date:chararray,
				Reported_Time:chararray,
				Arrest_Flag:chararray);

-- From the dataset, the date and time are loaded respectively. 
timeStops =  FOREACH policeStops_Dataset GENERATE Reported_Date,Reported_Time;


/* In order to convert the separate date and time to a single time format, 
   they are concatenated below.*/
concatDateTime = FOREACH timeStops GENERATE CONCAT(CONCAT(Reported_Date,'T'),
						Reported_Time) AS CharStopTime;


/*The ToDate() function is then used to cast the array to a date-time type to 
allow extraction of year, month, day or time when needed. */
convertToDateTime = FOREACH concatDateTime GENERATE ToDate(CharStopTime) AS StopTime;


/* Below, the time periods are categorised into EarlyMorning, Morning, 
Afternoon and Evening according to when the stop was done. */
SPLIT convertToDateTime INTO 
			EarlyMorning IF GetHour(StopTime) >= 0 AND GetHour(StopTime) < 6, 
			Morning IF GetHour(StopTime) >= 6 AND GetHour(StopTime) < 12,
			Afternoon IF GetHour(StopTime) >= 12 AND GetHour(StopTime) < 18,
			Evening IF GetHour(StopTime) >= 18 AND GetHour(StopTime) <= 23;



/* For the various time period, the macro is then called to return 
the count of stops for those time period accross the years.*/

yearEarlyMorning = yearTimeMacro(EarlyMorning, 'StopTime');
--DUMP yearEarlyMorning;

yearMorning = yearTimeMacro(Morning, 'StopTime');
--DUMP yearMorning;

yearAfternoon = yearTimeMacro(Afternoon, 'StopTime');
--DUMP yearAfternoon;

yearEvening = yearTimeMacro(Evening, 'StopTime');
--DUMP yearEvening;

--DESCRIBE yearEvening;


--Joining the time periods to the respective years.
joiner = JOIN yearEarlyMorning BY Year, 
	      yearMorning BY Year, 
	      yearAfternoon BY Year, 
	      yearEvening BY Year;

--DUMP joiner;
--DESCRIBE joiner;

--Renaming and rearranging the columns for more readability of the result.
finalYearTimePeriod = FOREACH joiner{
	GENERATE yearEarlyMorning::Year AS Year, 
		 yearEarlyMorning::count AS earlyMorningCount,
		 yearMorning::count AS morningCount,
		 yearAfternoon::count AS afternoonCount,
		 yearEvening::count AS eveningCount;
};

--Display of final result
DUMP finalYearTimePeriod;






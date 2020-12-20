
/*** For each year, this macro is meant to compute the number of stops based on the stop time periods. 
   For an Evening stop time, the macro will compute number of times there were stops 
   accross the years. 

The output for variable morning, will be the count for the various years.
(2015,1336)
(2016,1512)
(2017,1292)
(2018,751)

Shows that in 2015, there were 1,336 stops in the morning and so on for subsequent years;

***/

DEFINE yearTimeMacro(timePeriod, StopTime) RETURNS yearDayT{

	$yearDayT = FOREACH (GROUP $timePeriod BY GetYear($StopTime)) GENERATE 
			group AS Year, 
			COUNT($timePeriod) AS count;

};


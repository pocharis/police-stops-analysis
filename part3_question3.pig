/*** Question 3 of Part 3, this is an analysis to find out what percentage of a Suspects race, 
	stoped by a particular race of police officer.

For instance, for all stops by an Asian Officer, what percentage of the Suspect is White? 

***/


/*** In order to correctly load the csv file, the CSVLoader User Defined Function is registered and Defined below ***/
REGISTER /usr/lib/pig/piggybank.jar

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


/*** The dataset is then loaded uysing the CSVLoader defined above. ***/

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


-- Extracting the required column from the dataset. 
policeRace_suspectRace = FOREACH policeStops_Dataset GENERATE Officer_Race, Subject_Perceived_Race;


-- Cleaning the loaded data to get rid of those without data or unspecified race type of both suspect and officer. 
dataFilter = FILTER policeRace_suspectRace BY   Officer_Race != '-' AND 
						Subject_Perceived_Race != '-' AND 
						Subject_Perceived_Race != 'Unknown' AND
						Officer_Race != 'Unknown' AND 
						Officer_Race != 'Not Specified';


-- Grouping the filtered data, to do a count of each Suspects race that corresponds to various races of the Officers 
groupedRaceCompare = GROUP dataFilter BY (Officer_Race,Subject_Perceived_Race);


--Counting the number of stops by each Officer race, accross the various Suspects races
extractedRaceCompare = FOREACH groupedRaceCompare{
	count = (int)COUNT(dataFilter.Subject_Perceived_Race);
	GENERATE FLATTEN(group) AS (officerRace, SubjectRace), 
				    count as Count;
}

--Grouping By officer race to get a sum of the various race counts
groupPercentCalc = GROUP extractedRaceCompare BY officerRace;

--Calculating the total stop done by each race of the Officers
percentCalc = FOREACH groupPercentCalc{
	
	total = (int)SUM(extractedRaceCompare.Count);
	
	GENERATE FLATTEN(extractedRaceCompare) AS (officerRace, SubjectRace, Count),
		 total AS Total; 
}

/* Computing the various races of the officers and the percentage of various Suspects races that was stopped.
   The ROUND is used to round up the percentages to one decimal place.
 */
finalPercent = FOREACH percentCalc GENERATE officerRace AS officerRace, 
					    SubjectRace AS SubjectRace,
					    ROUND(100*(float)(Count) / (float)Total*10.0)/10.0 As percentOfRace:float;

--ordering to get better insight
orderedFinalPercent = ORDER finalPercent BY officerRace ASC, percentOfRace DESC;



--Result below
DUMP orderedFinalPercent;


--This is used to delete the output folder if it exists, and then store the output of whole analysis.
sh rm -r -f part3_question3_answer;
STORE orderedFinalPercent INTO 'part3_question3_answer';




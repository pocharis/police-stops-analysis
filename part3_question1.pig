/*** Question 1 of Part 3. Compute the different races and  count of  age range for each of them.

This is to determine for each race, the number of stops that took place accross the age ranges.
It can also be reordered to find the race with the highest stops.

***/


/* In order to correctly load the csv file, the CSVLoader User Defined Function is registered and Defined below*/

REGISTER /usr/lib/pig/piggybank.jar

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


--The dataset is then loaded uysing the CSVLoader defined above.

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

 

-- From the dataset, the Subject_Perceived_Race and Subject_Age_Group are loaded respectively. 
race_age = FOREACH policeStops_Dataset GENERATE Subject_Perceived_Race, Subject_Age_Group;

-- Filtering to remove the empty tuples and subjcts race that is unknown.
race_age = FILTER race_age BY Subject_Perceived_Race != '-' AND
			      Subject_Age_Group != '-' AND 
			      Subject_Perceived_Race != 'Unknown';


--Grouping, in order to count the age ranges.
grouped = GROUP race_age BY (Subject_Perceived_Race, Subject_Age_Group);

--Counting the Age groups for each race.
countRaceAge =  FOREACH grouped{
	countAge = COUNT(race_age.Subject_Age_Group);
	GENERATE group.Subject_Perceived_Race AS Race,
		 group.Subject_Age_Group as AgeRange, 
		 countAge AS countAge;
};

--Ordering to obtain the descending order of age ranges for each race.
orderedCountRaceAge = ORDER countRaceAge BY Race ASC, countAge DESC;

--Output of the ordered  result in ascending order of race and descending order of age range.
DUMP orderedCountRaceAge;


--OR

-- Ordering to find the race with the highest stops according to particular age range
highestRaceWithStops = ORDER countRaceAge BY countAge DESC;

--Output of the  result, in descending order of stops by age range.
DUMP highestRaceWithStops;


/*This is used to delete the output folder if it exists, and then 
store the output of whole analysis.*/
sh rm -r -f part3_question1_answer;
STORE highestRaceWithStops INTO 'part3_question1_answer';

 



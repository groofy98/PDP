/* @Author: Sjors Grooff
Date: 09-09-2021

Load the data from the CVS files with the correct datatypes.
I used the CSVExcelstorage loader because the csv was formatted as such and omitted the quote removal step.
*/
players = LOAD '/user/maria_dev/players.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS
	(game_id:int,
    country:chararray,
    won:int,
    num_supply_centers:int,
    eliminated:int,
    start_turn:int,
    end_turn:int
    );

-- We downselect to only won games
playerWon = FILTER players BY (won == 1);

-- Next we need to group by country
playerWonByCountry = GROUP playerWon BY (country);

-- We need to add a count to the touple and we do this by firt flattening the existing tuple and creating a new tuple with count added
playerWonByCountryWithCount = foreach playerWonByCountry generate flatten(group) as (country), COUNT($1);

-- Lastly we order the data by location
result = ORDER playerWonByCountryWithCount BY country;

DUMP result;
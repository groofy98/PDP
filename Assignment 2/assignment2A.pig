/* Load the data from the CVS files with the correct datatypes.
I used the CSVExcelstorage loader because the csv was formatted as such and omitted the quote removal step.
*/
orders = LOAD '/user/maria_dev/orders.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS
	(game_id:int,
    unit_id:int,
    unit_order:chararray,
    location:chararray,
    target:chararray,
    target_dest:chararray,
    success:chararray,
    reason:chararray,
    turn_num:int);

-- We only need moves that target holland so we do a filter operation
targetsHolland = FILTER orders BY (target == 'Holland');

-- Next we need to group all moves by location
targetsHollandByLocation = GROUP targetsHolland BY (location, target);

-- We need to add a count to the touple and we do this by firt flattening the existing tuple and creating a new tuple with count added
targetsHollandByLocationWithCount = foreach targetsHollandByLocation generate flatten(group) as (location, target), COUNT($1);

-- Lastly we order the data by location
result = ORDER targetsHollandByLocationWithCount BY location;

-- Finally show the result
DUMP result;
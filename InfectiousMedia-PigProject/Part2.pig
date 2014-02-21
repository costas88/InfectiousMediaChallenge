PostcodeData = LOAD '/home/ubuntu/data/ofcom-postcode-data-for-consumers-2013.csv' using PigStorage (',') as (postcode: chararray, postcodeDataStatus: chararray, slowLines: chararray, avgSpeed: chararray, medianSpeed: chararray, maxSpeed: chararray, NGA: chararray, connectionNo: int);
LimitedData = Limit PostcodeData 1000;

MaxAndAvgSpeed = FOREACH LimitedData GENERATE (float) REPLACE(maxSpeed, '>=', '') as maxSpeed,(float) REPLACE(avgSpeed, '>=', '') as avgSpeed;
sortWithMaxSpeed = GROUP MaxAndAvgSpeed By maxSpeed;
maxSpeedAndCounter = FOREACH sortWithMaxSpeed GENERATE group, COUNT($1) as counter, $1.avgSpeed as avgSpeed;
OrderedMaxSpeedAndCounter = Order maxSpeedAndCounter By group DESC;
top5MaxSpeedsAndCounter = LIMIT OrderedMaxSpeedAndCounter 5;
top5MaxSpeedsFinal = FOREACH top5MaxSpeedsAndCounter GENERATE group, counter, FLATTEN($2.avgSpeed) as avgSpeed;

postCodeAndAvgSpeed = FOREACH LimitedData GENERATE postcode,  (float) REPLACE(avgSpeed, '>=', '') as avgSpeed;
sortWithAvgSpeed =  GROUP postCodeAndAvgSpeed BY avgSpeed;
avgSpeedAndCounter = FOREACH sortWithAvgSpeed GENERATE group, $1.postcode as postcode, COUNT($1) as counter;
OrderedAvgSpeedAndCounter = Order avgSpeedAndCounter By counter DESC;

JoinedRelations = JOIN top5MaxSpeedsFinal  by avgSpeed, OrderedAvgSpeedAndCounter by group;
Results = FOREACH JoinedRelations GENERATE $0,$1,$2,$4,COUNT($4) as counter2;
distinctResults = Distinct Results;
groupingByMaxSpeed = GROUP distinctResults by $0;
sortedResults = foreach groupingByMaxSpeed{
    sorted = order $1 by counter2 DESC;
	limitation = limit sorted 5;
    generate $0,limitation;
}



Store sortedResults INTO 'Part2Results' USING PigStorage (',');
Describe sortedResults;


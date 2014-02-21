Data = LOAD '/home/ubuntu/data/ofcom-postcode-data-for-consumers-2013.csv' using PigStorage (',') as (postcode: chararray, postcodeDataStatus: chararray, slowLines: chararray, avgSpeed: chararray, medianSpeed: chararray, maxSpeed: chararray, NGA: chararray, connectionNo: int);

MaxSpeedAndPostcode = FOREACH Data GENERATE (float) REPLACE(maxSpeed, '>=', '') as maxSpeed, postcode;
sortWithMaxSpeed = GROUP MaxSpeedAndPostcode By maxSpeed;
maxSpeedAndCounter = FOREACH sortWithMaxSpeed GENERATE group as maxSpeed, COUNT($1) as counter;
OrderedMaxSpeedAndCounter = Order maxSpeedAndCounter By maxSpeed DESC;
top5MaxSpeedsAndCounter = LIMIT OrderedMaxSpeedAndCounter 5;

Store top5MaxSpeedsAndCounter INTO 'part1Results' USING PigStorage (',');

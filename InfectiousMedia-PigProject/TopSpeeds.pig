postcodeData = LOAD '/home/ubuntu/data/ofcom-postcode-data-for-consumers-2013.csv' using PigStorage (',') as (postcode: chararray, postcodeDataStatus: chararray, slowLines: chararray, avgSpeed: float, medianSpeed: float, maxSpeed: chararray, NGA: chararray, connectionNo: int);
removeSymbolsFromMaxSpeed = FOREACH postcodeData GENERATE postcode, postcodeDataStatus, slowLines, avgSpeed, medianSpeed, REPLACE(maxSpeed, '>=', '') as maxSpeed, NGA, connectionNo;
postCodesAndMaxSpeeds = FOREACH removeSymbolsFromMaxSpeed GENERATE postcode,(float)maxSpeed;
sortWithMaxSpeed = GROUP postCodesAndMaxSpeeds By maxSpeed;
maxSpeedAndCounter = FOREACH sortWithMaxSpeed GENERATE group, COUNT(postCodesAndMaxSpeeds);
maxSpeedDescending = Order maxSpeedAndCounter By group DESC;
top5speedsAndCounter = LIMIT maxSpeedDescending 5;
Store top5speedsAndCounter into 'results1' using PigStorage (',');



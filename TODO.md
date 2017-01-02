# TODO #

## Started (Jan 2nd 2017) ##

1. Correct summaries for cross-shard calculations
1. Add "average" and "count distict" calculations

## To be done soon ##

1. Document server mode / parallel queries
1. Generalization of Symbol Tables + Add One, Four & Eight Byte Symbol Tables
1. Enhance queries to support conditional functions... Eg. ('price' '>' 100)('Name' 'like' 'A%')
1. Beef up error checking on file creation
1. Database status reporting function (# shards, records in each, statistics, etc)
1. Add a "Char" type which does not use a symbol table
1. User Guide

## More speculative ideas ##
1. RESTful / ODATA? API
1. Timestamped non-overwriting updates
1. Delete records (AFTER non-overwriting updates)
1. Database cleanup (throw away history)
1. TimeStamp columns
1. Aggregations in queries
1. Add support for noFiles switch: run entirely in memory with no backing storage
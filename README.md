# search-gov-indexing
Indexing ingest and processing for Search.gov


## Indexing Module List


### Alphanumeric Search Model 
Uses currently indexed data in ElasticSearch to generate a model for improving alphanumeric search within Search.gov

| Module | Description|
| -------- | ------|
| Data Grabber |Loads data from a snapshot into ElasticSearch for analysis.|
| RegEx Crawler |Using RegEx patterns will crawl ElasticSearch looking for alphanumeric Strings|
| Levenshtein | Creates a Levenshtein distance calculation between words in i14y documents and search values |
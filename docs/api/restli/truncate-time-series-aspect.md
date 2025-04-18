# Truncate Timeseries Index Endpoint

You can do a HTTP POST request to `/gms/operations?action=truncateTimeseriesAspect` endpoint to manage the size of a time series index by removing entries older than a certain timestamp, thereby truncating the table to only the entries needed, to save space. The `getIndexSizes` endpoint can be used to identify the largest indices. The output includes the index parameters needed for this function.

```
curl --location --request POST 'https://demo.datahubproject.io/api/gms/operations?action=truncateTimeseriesAspect' \
--header 'Authorization: Bearer TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "entityType": "YOUR_ENTITY_TYPE",
    "aspect": "YOUR_ASPECT_NAME",
    "endTimeMillis": 1000000000000
}'

curl --location --request POST 'https://demo.datahubproject.io/api/gms/operations?action=truncateTimeseriesAspect' \
--header 'Authorization: Bearer TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "entityType": "YOUR_ENTITY_TYPE",
    "aspect": "YOUR_ASPECT_NAME",
    "endTimeMillis": 1000000000000,
    "dryRun": false,
    "batchSize": 100,
    "timeoutSeconds": 3600
}'
```

The supported parameters are
- `entityType` - Required type of the entity to truncate the index of, for example, `dataset`. 
- `aspect` - Required name of the aspect to truncate the index of, for example, `datasetusagestatistics`. A call to `getIndexSizes` shows the `entityType` and `aspect` parameters for each index along with its size. 
- `endTimeMillis` - Required timestamp to truncate the index to. Entities with timestamps older than this time will be deleted. 
- `dryRun` - Optional boolean to enable/disable dry run functionality. Default: true. In a dry run, the following information will be printed:
```
{"value":"Delete 0 out of 201 rows (0.00%). Reindexing the aspect without the deleted records. This was a dry run. Run with dryRun = false to execute."}
```
- `batchSize` - Optional integer to control the batch size for the deletion. Default: 10000
- `timeoutSeconds` - Optional integer to set a timeout for the delete operation. Default: No timeout set

The output to the call will be information about how many rows would be deleted and how to proceed for a dry run: 
```
{"value":"Delete 0 out of 201 rows (0.00%). Reindexing the aspect without the deleted records. This was a dry run. Run with dryRun = false to execute."}
```
For a non-dry-run, the output will be the Task ID of the asynchronous delete operation. This task ID can be used to monitor the status of the operation.
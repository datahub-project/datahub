# Get Index Sizes Endpoint

You can do a HTTP POST request to `/gms/operations?action=getIndexSizes` endpoint with no parameters to see the size of indices in ElasticSearch. For now, only timeseries indices are supported, as they can grow indefinitely, and the `truncateTimeseriesAspect` endpoint is provided to clean up old entries. This endpoint can be used in conjunction with the cleanup endpoint to see which indices are the largest before truncation.

```
curl --location --request POST 'https://demo.datahubproject.io/api/gms/operations?action=getIndexSizes' \
--header 'Authorization: Bearer TOKEN'
```

The endpoint takes no parameters, and the output will be a string representing a JSON object containing the following information about each index:
```
      {
        "aspectName": "datasetusagestatistics",
        "sizeMb": 0.208,
        "indexName": "dataset_datasetusagestatisticsaspect_v1",
        "entityName": "dataset"
      }
```

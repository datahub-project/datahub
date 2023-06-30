# Get ElasticSearch Task Status Endpoint

You can do a HTTP POST request to `/gms/operations?action=getEsTaskStatus` endpoint to see the status of the input task running in ElasticSearch. For example, the task ID given by the [`truncateTimeseriesAspect` endpoint](./truncate-time-series-aspect.md). The task ID can be passed in as a string with node name and task ID separated by a colon (as is output by the previous API), or the node name and task ID parameters separately.

```
curl --location --request POST 'https://demo.datahubproject.io/api/gms/operations?action=getEsTaskStatus' \
--header 'Authorization: Bearer TOKEN'
--header 'Content-Type: application/json' \
--data-raw '{
    "task": "aB1cdEf2GHIJKLMnoPQr3S:123456"
}'

curl --location --request POST  http://localhost:8080/operations\?action\=getEsTaskStatus \         
--header 'Authorization: Bearer TOKEN'
--header 'Content-Type: application/json' \
--data-raw '{
    "nodeId": "aB1cdEf2GHIJKLMnoPQr3S",
    taskId: 12345
}' 
```

The output will be a string representing a JSON object with the task status.
```
{
  "value": "{\"error\":\"Could not get task status for XIAMx5WySACgg9XxBgaKmw:12587\"}"
}
```
```
"{
  "completed": true,
  "taskId": "qhxGdzytQS-pQek8CwBCZg:54654",
  "runTimeNanos": 1179458,
  "status": "{
    "total": 0,
    "updated": 0,
    "created": 0,
    "deleted": 0,
    "batches": 0,
    "version_conflicts": 0,
    "noops": 0,
    "retries": {
      "bulk": 0,
      "search": 0
    },
    "throttled_millis": 0,
    "requests_per_second": -1.0,
    "throttled_until_millis": 0
  }
}
```

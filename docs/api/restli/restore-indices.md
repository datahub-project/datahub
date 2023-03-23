# Restore Indices Endpoint

You can do a HTTP POST request to `/gms/aspects?action=restoreIndices` endpoint with the `urn` as part of JSON Payload to restore indices for the particular URN, or with the `urnLike` regex to restore for `batchSize` URNs matching the pattern starting from `start`.

```
curl --location --request POST 'https://demo.datahubproject.io/api/gms/aspects?action=restoreIndices' \
--header 'Authorization: Bearer TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "urn": "YOUR_URN"
}'

curl --location --request POST 'https://demo.datahubproject.io/api/gms/aspects?action=restoreIndices' \
--header 'Authorization: Bearer TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "urnLike": "urn:dataPlatform:%"
}'
```
w
The supported parameters are
- `urn` - Optionl URN string
- `aspect` - Optional Aspect string
- `urnLike` - Optional string regex to match URNs
- `start` - Optional integer to decide which rows number of sql store to restore. Default: 0
- `batchSize` - Optional integer to decide how many rows to restore. Default: 10

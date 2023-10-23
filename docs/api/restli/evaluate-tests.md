# Evaluate Tests Endpoint
<FeatureAvailability saasOnly />

You can do a HTTP POST request to `/gms/test?action=evaluate` endpoint with the `urn` as part of JSON Payload to run metadata tests for the particular URN.

```
curl --location --request POST 'https://DOMAIN.acryl.io/gms/test?action=evaluate' \
--header 'Authorization: Bearer TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "urn": "YOUR_URN"
}'
```
w
The supported parameters are
- `urn` - Required URN string
- `push` - Optional Boolean - whether or not to push the results to persist them. Default `false`.
- `testUrns` - Optional List of string - If you wish to get specific test URNs evaluated

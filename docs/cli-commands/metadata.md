# datahub get, put, delete, exists and timeline

## delete

The `delete` command allows you to delete metadata from DataHub.

The [metadata deletion guide](../how/delete-metadata.md) covers the various options for the delete command.

## exists

The exists command can be used to check if an entity exists in DataHub.

```shell
> datahub exists --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
true
> datahub exists --urn "urn:li:dataset:(urn:li:dataPlatform:hive,NonExistentHiveDataset,PROD)"
false
```

## get

The `get` command allows you to easily retrieve metadata from DataHub, by using the REST API. This works for both versioned aspects and timeseries aspects. For timeseries aspects, it fetches the latest value.
For example the following command gets the ownership aspect from the dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`

```shell-session
$ datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership
{
  "ownership": {
    "lastModified": {
      "actor": "urn:li:corpuser:jdoe",
      "time": 1680210917580
    },
    "owners": [
      {
        "owner": "urn:li:corpuser:jdoe",
        "source": {
          "type": "SERVICE"
        },
        "type": "NONE"
      }
    ]
  }
}
```

## put

The `put` group of commands allows you to write metadata into DataHub. This is a flexible way for you to issue edits to metadata from the command line.

### put aspect

The **put aspect** (also the default `put`) command instructs `datahub` to set a specific aspect for an entity to a specified value.
For example, the command shown below sets the `ownership` aspect of the dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)` to the value in the file `ownership.json`.
The JSON in the `ownership.json` file needs to conform to the [`Ownership`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl) Aspect model as shown below.

```json
{
  "owners": [
    {
      "owner": "urn:li:corpuser:jdoe",
      "type": "DEVELOPER"
    },
    {
      "owner": "urn:li:corpuser:jdub",
      "type": "DATAOWNER"
    }
  ]
}
```

```console
datahub --debug put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership -d ownership.json

[DATE_TIMESTAMP] DEBUG    {datahub.cli.cli_utils:340} - Attempting to emit to DataHub GMS; using curl equivalent to:
curl -X POST -H 'User-Agent: python-requests/2.26.0' -H 'Accept-Encoding: gzip, deflate' -H 'Accept: */*' -H 'Connection: keep-alive' -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'Content-Type: application/json' --data '{"proposal": {"entityType": "dataset", "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)", "aspectName": "ownership", "changeType": "UPSERT", "aspect": {"contentType": "application/json", "value": "{\"owners\": [{\"owner\": \"urn:li:corpuser:jdoe\", \"type\": \"DEVELOPER\"}, {\"owner\": \"urn:li:corpuser:jdub\", \"type\": \"DATAOWNER\"}]}"}}}' 'http://localhost:8080/aspects/?action=ingestProposal'
Update succeeded with status 200
```

### put platform

**🤝 Version Compatibility:** `acryl-datahub>0.8.44.4`

The **put platform** command instructs `datahub` to create or update metadata about a data platform. This is very useful if you are using a custom data platform, to set up its logo and display name for a native UI experience.

```shell
datahub put platform --name longtail_schemas --display_name "Long Tail Schemas" --logo "https://flink.apache.org/img/logo/png/50/color_50.png"
✅ Successfully wrote data platform metadata for urn:li:dataPlatform:longtail_schemas to DataHub (DataHubRestEmitter: configured to talk to https://longtailcompanions.acryl.io/api/gms with token: eyJh**********Cics)
```

## timeline

The `timeline` command allows you to view a version history for entities. Currently only supported for Datasets. For example,
the following command will show you the modifications to tags for a dataset for the past week. The output includes a computed semantic version,
relevant for schema changes only currently, the target of the modification, and a description of the change including a timestamp.
The default output is sanitized to be more readable, but the full API output can be obtained by passing the `--verbose` flag and
to get the raw JSON difference in addition to the API output you can add the `--raw` flag. For more details about the feature please see [the main feature page](../dev-guides/timeline.md)

```console
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)" --category TAG --start 7daysago
2022-02-17 14:03:42 - 0.0.0-computed
 MODIFY TAG dataset:mysql:User.UserAccount : A change in aspect editableSchemaMetadata happened at time 2022-02-17 20:03:42.0
2022-02-17 14:17:30 - 0.0.0-computed
 MODIFY TAG dataset:mysql:User.UserAccount : A change in aspect editableSchemaMetadata happened at time 2022-02-17 20:17:30.118
```

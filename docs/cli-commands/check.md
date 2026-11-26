# datahub check

The datahub package is composed of different plugins that allow you to connect to different metadata sources and ingest metadata from them.
The `check` command allows you to check if all plugins are loaded correctly as well as validate an individual MCE-file.

## restore-indices

This command allows you to restore indices for one or more `urn`.

```shell
datahub --debug check restore-indices --urn "URN"
```

It can also take `--file` argument that points to a file that has list of urns like

```shell
datahub check restore-indices --file ./urn.txt
```

where urn.txt is like this

```urn.txt
urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.schema1.test_all_nulls,PROD)
urn:li:dataset:(urn:li:dataPlatform:platform1,test_db.schema2.test_complex_types,PROD)
urn:li:dataset:(urn:li:dataPlatform:redshift,test_db.schema3.test_few_rows,PROD)
```

## get-kafka-consumer-offsets

This required DataHub Cloud `0.3.12.x` or above version.

```shell
datahub check get-kafka-consumer-offsets
```

which can give can print out the details of lag in kafka topics.

```
{'mcl': {'generic-mae-consumer-job-client': {'MetadataChangeLog_Versioned_v1': {'metrics': {'avgLag': 36,
                                                                                            'maxLag': 36,
                                                                                            'medianLag': 36,
                                                                                            'totalLag': 36},
                                                                                'partitions': {'0': {'lag': 36,
                                                                                                     'offset': 257318}}}}},
 'mcl-timeseries': {'generic-mae-consumer-job-client': {'MetadataChangeLog_Timeseries_v1': {'metrics': {'avgLag': 0,
                                                                                                        'maxLag': 0,
                                                                                                        'medianLag': 0,
                                                                                                        'totalLag': 0},
                                                                                            'partitions': {'0': {'lag': 0,
                                                                                                                 'offset': 113}}}}},
 'mcp': {'generic-mce-consumer-job-client': {'MetadataChangeProposal_v1': {'metrics': {'avgLag': 7222149,
                                                                                       'maxLag': 7222149,
                                                                                       'medianLag': 7222149,
                                                                                       'totalLag': 7222149},
                                                                           'partitions': {'0': {'lag': 7222149,
                                                                                                'offset': 250254}}}}}}
```

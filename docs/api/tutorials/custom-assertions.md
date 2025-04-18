import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Custom Assertions

This guide specifically covers how to create and report results for custom assertions in DataHub.
Custom Assertions are those not natively run or directly modeled by DataHub, and managed by a 3rd party framework or tool.

To create _native_ assertions using the API (e.g. for DataHub to manage), please refer to the [Assertions API](./assertions.md).

This guide may be used as reference for partners seeking to integrate their own monitoring tools with DataHub.

## Goal Of This Guide

In this guide, you will learn how to 

1. Create and update custom assertions via GraphQL and Python APIs
2. Report results for custom assertions via GraphQL and Python APIs
3. Retrieve results for custom assertions via GraphQL and Python APIs
4. Delete custom assertions via GraphQL and Python APIs

## Prerequisites

The actor making API calls must have the `Edit Assertions` and `Edit Monitors` privileges for the Tables being monitored.

## Create And Update Custom Assertions

You may create custom assertions using the following APIs for a Dataset in DataHub. 

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To create a new assertion, use the `upsertCustomAssertion` GraphQL Mutation. This mutation both allows you to 
create and update a given assertion.

```graphql
mutation upsertCustomAssertion {
    upsertCustomAssertion(
        urn: "urn:li:assertion:my-custom-assertion-id", # Optional: if you want to provide a custom id. If not, one will be generated for you.
        input: {
            entityUrn: "<urn of entity being monitored>",
            type: "My Custom Category", # This is how your assertion will appear categorized in DataHub. 
            description: "The description of my external assertion for my dataset",
            platform: {
                urn: "urn:li:dataPlatform:great-expectations", # OR you can provide name: "My Custom Platform" if you do not have an URN for the platform. 
            }
            fieldPath: "field_foo", # Optional: if you want to associated with a specific field,
            externalUrl: "https://my-monitoring-tool.com/result-for-this-assertion" # Optional: if you want to provide a link to the monitoring tool
            # Optional: If you want to provide a custom SQL query for the assertion. This will be rendered as a query in the UI. 
            # logic: "SELECT * FROM X WHERE Y"
      }
  ) {
      urn
    }
}
```

Note that you can either provide a unique `urn` for the assertion, which will be used to generate the corresponding assertion urn in the following format:

`urn:li:assertion:<your-new-assertion-id>`

or a random urn will be created and returned for you. This id should be stable over time and unique for each assertion.

The upsert API will return the unique identifier (URN) for the the assertion if you were successful:

```json
{
  "data": {
    "upsertExternalAssertion": {
      "urn": "urn:li:assertion:your-new-assertion-id"
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

To upsert an assertion in Python, simply use the `upsert_external_assertion` method on the DataHub Client object. 

```python
{{ inline /metadata-ingestion/examples/library/upsert_custom_assertion.py show_path_as_comment }}
```

</TabItem>

</Tabs>

## Report Results For Custom Assertions

When an assertion is evaluated against a Dataset, or a new result is available, you can report the result to DataHub
using the following APIs. 

Once reported, these will appear in the evaluation history of the assertion and will be used to determine whether the assertion is
displayed as passing or failing in the DataHub UI.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To report results for a custom, use the `reportAssertionResult` GraphQL Mutation. This mutation both allows you to
create and update a given assertion.

```graphql
mutation reportAssertionResult {
    reportAssertionResult(
        urn: "urn:li:assertion:<your-new-assertion-id>"
        result: {
            timestampMillis: 1620000000, # Unix timestamp in millis. If not provided, the current time will be used.
            type: SUCCESS,  # or FAILURE or ERROR or INIT
            properties: [
                {
                    key: "my_custom_key",
                    value: "my_custom_value"
                }
            ],
            externalUrl: "https://my-great-expectations.com/results/1234", # Optional: URL to the results in the external tool
            # Optional: If the type is ERROR, you can provide additional context. See full list of error types below. 
            # error: {
            #    type: UNKNOWN_ERROR,
            #    message: "The assertion failed due to an unknown error"
            # }
      }
  )
}
```

The `type` field is used to communicate the latest health status of the assertion.

The `properties` field is used to provide additional key-value pair context that will be displayed alongside the result
in DataHub's UI. 

The full list of supported error types include:

- SOURCE_CONNECTION_ERROR
- SOURCE_QUERY_FAILED
- INSUFFICIENT_DATA
- INVALID_PARAMETERS
- INVALID_SOURCE_TYPE
- UNSUPPORTED_PLATFORM
- CUSTOM_SQL_ERROR
- FIELD_ASSERTION_ERROR
- UNKNOWN_ERROR

```json
{
  "data": {
    "reportAssertionResult": true
  },
  "extensions": {}
}
```

If the result is `true`, the result was successfully reported.

</TabItem>

<TabItem value="python" label="Python">

To report an assertion result in Python, simply use the `report_assertion_result` method on the DataHub Client object.

```python
{{ inline /metadata-ingestion/examples/library/report_assertion_result.py show_path_as_comment }}
```

</TabItem>

</Tabs>

## Retrieve Results For Custom Assertions

After an assertion has been created and run, it will appear in the set of assertions associated with a given dataset urn.
You can retrieve the results of these assertions using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>


### Get Assertions for Dataset

To retrieve all the assertions for a table / dataset, you can use the following GraphQL Query.

```graphql
query dataset {
    dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)") {
        assertions(start: 0, count: 1000) {
            start
            count
            total
            assertions {
                urn
                # Fetch the last run of each associated assertion. 
                runEvents(status: COMPLETE, limit: 1) {
                    total
                    failed
                    succeeded
                    runEvents {
                        timestampMillis
                        status
                        result {
                            type
                            nativeResults {
                                key
                                value
                            }
                        }
                    }
                }
                info {
                    type # Will be CUSTOM
                    customType # Will be your custom type. 
                    description
                    lastUpdated {
                        time
                        actor
                    }
                    customAssertion {
                        entityUrn
                        fieldPath
                        externalUrl
                        logic
                    }
                    source {
                        type
                        created {
                            time
                            actor
                        }
                    }
                }
            }
        }
    }
}
```

### Get Assertion Details

You can use the following GraphQL query to fetch the details for an assertion along with its evaluation history by URN.

```graphql
query getAssertion {
    assertion(urn: "urn:li:assertion:my-custom-assertion-id") {
        urn
        # Fetch the last 10 runs for the assertion. 
        runEvents(status: COMPLETE, limit: 10) {
            total
            failed
            succeeded
            runEvents {
                timestampMillis
                status
                result {
                    type
                    nativeResults {
                        key
                        value
                    }
                }
            }
        }
        info {
            type # Will be CUSTOM
            customType # Will be your custom type. 
            description
            lastUpdated {
                time 
                actor
            }
            customAssertion {
                entityUrn
                fieldPath
                externalUrl
                logic
            }
            source {
                type
                created {
                    time
                    actor
                }
            }
        }
        # Fetch what entities have the assertion attached to it
        relationships(input: {
            types: ["Asserts"]
            direction: OUTGOING
        }) {
            total
            relationships {
                entity {
                    urn
                }
            }
        }
    }
}
```
</TabItem>
</Tabs>


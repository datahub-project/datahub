import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Data Contracts

<FeatureAvailability saasOnly />

This guide specifically covers how to use the Data Contract APIs with **DataHub Cloud**.

## Why Would You Use Data Contract APIs?

The Assertions APIs allow you to create, update, and evaluate Data Contracts programmatically. This is particularly
useful to automate the monitoring of data quality and schema compliance for your data.

### Goal Of This Guide

This guide will show you how to create, update, and check the status of aData Contract.

## Prerequisites

### Privileges Required

The actor making API calls must have the `Edit Data Contract` privileges for the Tables at hand.

### Assertions

Before creating a Data Contract, you should have already created the Assertions that you want to associate with the Data Contract.
Check out the [Assertions](/docs/api/tutorials/assertions.md) guide for details on how to create DataHub Assertions.

## Create & Update Data Contract

You can create a new Data Contract, which is simply bundle of "important" assertions, using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

To create or update a Data Contract, simply use the `upsertDataContract` GraphQL Mutation. 

```graphql
mutation upsertDataContract {
    upsertDataContract(
      input: {
        entityUrn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,purchases,PROD)", # Table to Create Contract for 
        freshness: [
            {
                assertionUrn: "urn:li:assertion:your-freshness-assertion-id",
            }
        ],
        schema: [
            {
                assertionUrn: "urn:li:assertion:your-schema-assertion-id",
            }
        ],
        dataQuality: [
            {
                assertionUrn: "urn:li:assertion:your-column-assertion-id-1",
            },
            {
                assertionUrn: "urn:li:assertion:your-column-assertion-id-2",
            }
        ]
      }) {
        urn
      }
  )
}
```

This API will return a unique identifier (URN) for the Data Contract if you were successful:

```json
{
  "data": {
    "upsertDataContract": {
      "urn": "urn:li:dataContract:your-new-contract-id"
    }
  },
  "extensions": {}
}
```

If you want to update an existing Data Contract, you can use the same API, but also passing the `urn` parameter in the
`upsertDataContract` mutation.

```graphql
mutation upsertDataContract {
    upsertDataContract(
      urn: "urn:li:dataContract:your-existing-contract-id",
      input: {
        freshness: [
            {
                assertionUrn: "urn:li:assertion:your-freshness-assertion-id",
            }
        ],
        schema: [
            {
                assertionUrn: "urn:li:assertion:your-schema-assertion-id",
            }
        ],
        dataQuality: [
            {
                assertionUrn: "urn:li:assertion:your-column-assertion-id-1",
            },
            {
                assertionUrn: "urn:li:assertion:your-column-assertion-id-2",
            }
        ]
      }) {
        urn
      }
  )
}
```

</TabItem>
</Tabs>

## Check Contract Status

You can use the following APIs to check whether a Data Contract is passing or failing, which is determined
by the last status of the assertions associated with the contract.

<Tabs>

<TabItem value="graphql" label="GraphQL" default>

### Check Contract Status for Table

```graphql
query getTableContractStatus {
    dataset(urn: "urn:li:dataset(urn:li:dataPlatform:snowflake,purchases,PROD") {
        contract {
           result {
              type # Passing or Failing.
              assertionResults { # Results of each contract assertion. 
                  assertion {
                     urn
                  }
                  result {
                      type
                      nativeResults {
                          key
                          value
                      }
                  }
              }
           }
        }
    }
}
```

You can also _force refresh_ all of the Contract Assertions by evaluating them on-demand by providing the `refresh` argument
in your query. 

```graphql
query getTableContractStatus {
    dataset(urn: "urn:li:dataset(urn:li:dataPlatform:snowflake,purchases,PROD") {
        contract(refresh: true) {
           ... same
        }
    }
}
```

This will run any native Acryl assertions comprising the Data Contract. Be careful! This can take a while depending on how many native assertions are part of the contract.

If you're successful, you'll get the latest status for the Table Contract: 

```json
{
  "data": {
    "dataset": {
       "contract": {
           "result": {
              "type": "PASSING",
              "assertionResults": [
                  {
                      "assertion": {
                         "urn": "urn:li:assertion:your-freshness-assertion-id"
                      },
                      "result": {
                          "type": "SUCCESS",
                          "nativeResults": [
                              {
                                  "key": "Value",
                                  "value": "1382"
                              }
                          ]
                      }
                  },
                  {
                     "assertion": {
                        "urn": "urn:li:assertion:your-volume-assertion-id"
                     },
                      "result": {
                          "type": "SUCCESS",
                          "nativeResults": [
                              {
                                  "key": "Value",
                                  "value": "12323"
                              }
                          ]
                      }
                  }
              ]
           }
        }
    }
  },
  "extensions": {}
}
```
</TabItem>

</Tabs>


# Constraints API

## Introduction

Constraints let you set requirements for what types of terms must be applied to your datasets. For an overview of Constraints, see this video:

<div style={{ position: "relative", paddingBottom: "56.25%", height: 0 }}>
  <iframe
    src="https://www.loom.com/embed/8834935fd1c64c489c6c131ba10bbe04"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

This feature allows people to specify constraints on your metadata. This currently supports

* Constraint for allowing for one of the GlossaryTerm for GlossaryNode to be present on every dataset



## Prerequisites

The following **Platform** privileges are required to create a constraint. These can be granted via [access policies](../../administering-datahub/policies-guide.md).&#x20;

* Platform Privilege **`CREATE_CONSTRAINTS`** (Not added in bootstrap to root user)

## Adding Constraints

### Adding Glossary Node Constraint

* Ensure that you add required privileges
* Ensure that you have Glossary Nodes ingested in your Datahub instance. If you don't have any terms, you can ingest [this file](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/examples/bootstrap\_data/business\_glossary.yml) for glossary nodes using a recipe [like this](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/examples/recipes/business\_glossary\_to\_datahub.yml).
*   Go to [`http://localhost:9002/api/graphiql`](http://localhost:9002/api/graphiql) and execute this mutation

    ```bash
    mutation createTermConstraint($input: CreateTermConstraintInput!) {
      createTermConstraint(input: $input)
    }
    ```

    with variables

    ```bash
    {
        "input": {
          "name": "My Constraint Title",
          "description": "My Test Constraint Description",
          "nodeUrn": "urn:li:glossaryNode:Classification"
        }
    }
    ```
*   You can query the constraint on the dataset as

    ```bash
    query {
      dataset(urn:"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"){
        constraints{
          type
          displayName
          description
          params {
            hasGlossaryTermInNodeParams {
              nodeName
            }
          }
        }
      }
    }
    ```

    If you have not added glossary terms from Glossary Node `Classification` yet you will get

    ```bash
    {
      "data": {
        "dataset": {
          "constraints": [
            {
              "type": "HAS_GLOSSARY_TERM_IN_NODE",
              "displayName": "shouldHaveGlossary",
              "description": "Test Constraint",
              "params": {
                "hasGlossaryTermInNodeParams": {
                  "nodeName": "Classification"
                }
              }
            }
          ]
        }
      }
    }
    ```

    and when you go to a dataset you should see this constraint as “Missing:Classification” under Glossary Terms



    If you add the required Term you will get this response

    ```bash
    {
      "data": {
        "dataset": {
          "constraints": []
        }
      }
    } 
    ```

    If `constraints` has anything than that means constraint is unfulfilled.

    When you go to the dataset now you should see that warning missing

#### Limitation

* The constraint is on a global level. To check which entity has constraint unfulfilled we have to calculate the it by looking at each entity

# Source Name 

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->
Neo4j metadata will be ingested into DataHub using Call apoc.meta.data();  The data that is returned will be parsed 
and will be displayed as Nodes and Relationships in DataHub.  Each object will be tagged with describing what kind of DataHub
object it is.  The defaults are 'Node' and 'Relationship'.  These tag values can be overwritten in the recipe.



## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from Neo4j, you will need:

* Neo4j instance with APOC installed


### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[neo4j]'`


### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. 

<details>
  <summary>View All Recipe Configuartion Options</summary>
  
  | Field              | Required |     Default     | Description                           |
  |--------------------|:--------:|:---------------:|---------------------------------------|
  | source             |          |                 |                                       |
  | `type`             |    ✅     |     `neo4j`     | A required field with a default value |
  | config             |          |                 |                                       |
  | `uri`              |    ✅     | `default_value` | The URI for the Neo4j  server         |
  | `username`         |    ✅     |      None       | Neo4j Username                        |
  | `password`         |    ✅     |      None       | Neo4j Password
  | `gms_server`       |    ✅     |      None       |Address for the gms server|
  | `node_tag`         |    ❌     |      `Node`       |The tag that will be used to show that the Neo4j object is a Node|
  | `relationship_tag` |    ❌     |  `Relationship`   |The tag that will be used to show that the Neo4j object is a Relationship|
  | `environment`      |    ✅     |      None       ||
  | sink               |          |                 ||
  | `type`             |    ✅     |      None       ||
  | conifg             |          |                 ||
  | `server`           |    ✅     |      None       ||

</details>


```yml
source:
    type: 'neo4j'
    config:
        uri: 'neo4j+ssc://host:7687'
        username: 'neo4j'
        password: 'password'
        gms_server: &gms_server 'http://localhost:8080'
        node_tag: 'Node'
        relationship_tag: 'Relationship'
        environment: 'PROD'

sink:
  type: "datahub-rest"
  config:
    server: *gms_server
```



### Sample data that is returned from Neo4j.  This is the data that is parsed and used to create Nodes, Relationships.

        
      Example relationship:
        {
        relationship_name: {
            count: 1, 
            properties: {}, 
            type: "relationship"
            }
        }
        
      Example node:
        {
        key: Neo4j_Node, 
        value: {
            count: 10, 
            labels: [], 
            properties: {
                node_id: {
                    unique: true, 
                    indexed: true, 
                    type: "STRING", 
                    existence: false
                    }, 
                node_name: {
                    unique: false, 
                    indexed: false, 
                    type: "STRING", 
                    existence: false
                    }
                }, 
            type: "node", 
            relationships: {
                RELATIONSHIP_1: {
                    count: 10, 
                    direction: "in", 
                    labels: ["Node_1", "Node_2", "Node_3"], 
                    properties: {
                        relationsip_name: {
                            indexed: false, 
                            type: "STRING", 
                            existence: false, 
                            array: false
                            }, 
                        relationship_id: {
                            indexed: false, 
                            type: "INTEGER", 
                            existence: false, 
                            array: false
                            }
                        }
                    }, 
                RELATIONSHIP_2: {
                    count: 10, 
                    direction: "out", 
                    labels: ["Node_4"], 
                    properties: {
                        relationship_name: {
                            indexed: false, 
                            type: "STRING", 
                            existence: false, 
                            array: false
                            }, 
                        relationship_id: {
                            indexed: false, 
                            type: "INTEGER", 
                            existence: false, 
                            array: false
                            }
                        }
                    }
                }
            }
        }

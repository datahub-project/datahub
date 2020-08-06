- Start Date: 2020-08-06
- RFC PR: 
- Discussion Issue: https://github.com/linkedin/datahub/issues/1731
- Implementation PR(s): (leave this empty)

# RFC: column-level lineage

## Summary
While datahub currently is supporting table-level lineage as a dataset's aspect. There is a strong need to get column-level lineage. This RFC will discuss what's column-level lineage to build a concensus about a particular problem to solve. Then we break down how to build a column-level lineage graph reprentation in terms of new relationship needed. In the end, we come up with data model proposal, and related changes. 

## Basic example
Here are sample `json` requests to create dataset `p_foo_13`, `p_2_foo_13` and `c_bar_13`. 
>For `p_foo_13` and `p_2_foo_13`, it is nothing new. I put them in the later. For `c_bar_13`, we introduced new aspect of `FineGrainUpStreamLineage`.
1. dataset request Json for c_bar_13
```
{
    "snapshot": {
        "aspects": [
            {
                "com.linkedin.common.Ownership": {
                    "owners": [
                        {
                            "owner": "urn:li:corpuser:kzhang13",
                            "type": "DATAOWNER"
                        }
                    ],
                    "lastModified": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    }
                }
            },
            {
                "com.linkedin.dataset.UpstreamLineage": {
                    "upstreams": [
                        {
                            "auditStamp": {
                                "time": 0,
                                "actor": "urn:li:corpuser:kzhang13"
                            },
                            "dataset": "urn:li:dataset:(urn:li:dataPlatform:hasketl,p_foo_13,PROD)",
                            "type": "TRANSFORMED"
                        },
                        {
                            "auditStamp": {
                                "time": 0,
                                "actor": "urn:li:corpuser:kzhang13"
                            },
                            "dataset": "urn:li:dataset:(urn:li:dataPlatform:hasketl,2_p_foo_13,PROD)",
                            "type": "TRANSFORMED"
                        }
                    ]
                }                  
            },
            {
                "com.linkedin.dataset.FineGrainUpstreamLineage": {
                    "upstreams": [
                        {
                            "auditStamp": {
                                "time": 0,
                                "actor": "urn:li:corpuser:kzhang13"
                            },
                            "dataset": "urn:li:dataset:(urn:li:dataPlatform:hasketl,p_foo_13,PROD)",
                            "fields": [
                                {
                                    "sourceField": "foo131",
                                    "targetField" : "bar131"
                                },
                                {
                                    "sourceField": "foo132",
                                    "targetField" : "bar132"
                                }
                            ]
                        },
                        {
                            "auditStamp": {
                                "time": 0,
                                "actor": "urn:li:corpuser:kzhang13"
                            },
                            "dataset": "urn:li:dataset:(urn:li:dataPlatform:hasketl,p_2_foo_13,PROD)",
                            "fields": [
                                {
                                    "sourceField": "2_foo131",
                                    "targetField" : "bar131"
                                },
                                {
                                    "sourceField": "2_foo132",
                                    "targetField" : "bar132"
                                }
                            ]
                        }
                    ]
                }                  
            },
            {
                "com.linkedin.common.InstitutionalMemory": {
                    "elements": [
                        {
                            "url": "https://www.linkedin.com",
                            "description": "Sample doc",
                            "createStamp": {
                                "time": 0,
                                "actor": "urn:li:corpuser:kzhang13"
                            }
                        }
                    ]
                }
            },
            {
                "com.linkedin.schema.SchemaMetadata": {
                    "schemaName": "hbaseEvent",
                    "platform": "urn:li:dataPlatform:hbase",
                    "version": 0,
                    "created": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    },
                    "lastModified": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    },
                    "hash": "",
                    "platformSchema": {
                        "com.linkedin.schema.KafkaSchema": {
                            "documentSchema": "{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
                        }
                    },
                    "fields": [
                        {
                            "fieldPath": "bar131",
                            "description": "Bar",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "bar132",
                            "description": "Bar",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "bar133",
                            "description": "Bar",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "col134",
                            "description": "Bar",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "urn": "urn:li:dataset:(urn:li:dataPlatform:druid,c_bar_13,PROD)"
    }
}

```
2. dataset json request to create p_bar_13
```
{
    "snapshot": {
        "aspects": [
            {
                "com.linkedin.common.Ownership": {
                    "owners": [
                        {
                            "owner": "urn:li:corpuser:kzhang13",
                            "type": "DATAOWNER"
                        }
                    ],
                    "lastModified": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    }
                }
            },
            {
                "com.linkedin.schema.SchemaMetadata": {
                    "schemaName": "hbaseEvent",
                    "platform": "urn:li:dataPlatform:hbase",
                    "version": 0,
                    "created": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    },
                    "lastModified": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    },
                    "hash": "",
                    "platformSchema": {
                        "com.linkedin.schema.KafkaSchema": {
                            "documentSchema": "{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
                        }
                    },
                    "fields": [
                        {
                            "fieldPath": "foo131",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "foo132",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "foo133",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "foo134",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "urn": "urn:li:dataset:(urn:li:dataPlatform:hasketl,p_foo_13,PROD)"
    }
}



```

3. dataset to create `p_2_foo_13`
```
{
    "snapshot": {
        "aspects": [
            {
                "com.linkedin.common.Ownership": {
                    "owners": [
                        {
                            "owner": "urn:li:corpuser:kzhang13",
                            "type": "DATAOWNER"
                        }
                    ],
                    "lastModified": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    }
                }
            },
            {
                "com.linkedin.schema.SchemaMetadata": {
                    "schemaName": "hbaseEvent",
                    "platform": "urn:li:dataPlatform:hbase",
                    "version": 0,
                    "created": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    },
                    "lastModified": {
                        "time": 0,
                        "actor": "urn:li:corpuser:kzhang13"
                    },
                    "hash": "",
                    "platformSchema": {
                        "com.linkedin.schema.KafkaSchema": {
                            "documentSchema": "{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
                        }
                    },
                    "fields": [
                        {
                            "fieldPath": "2_foo131",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "2_foo132",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "2_foo133",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        },
                        {
                            "fieldPath": "2_foo134",
                            "description": "foo",
                            "nativeDataType": "string",
                            "type": {
                                "type": {
                                    "com.linkedin.schema.StringType": {}
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "urn": "urn:li:dataset:(urn:li:dataPlatform:hasketl,p_2_foo_13,PROD)"
    }
}

```



## Motivation
While datahub currently is supporting table-level lineage as a dataset's aspect. There is a strong need to get column-level lineage. 
Related issues are
1. when does Fine grain lineage feature come out? #1649
2. dataset field level Lineage support? #1519
3. add lineage workflow schedule support #1615

## Detailed design
A sample illustration of this column-level lineage as:
![column-level-lineage](https://user-images.githubusercontent.com/356347/87351952-e384a300-c51f-11ea-96f5-d695554cc5f1.jpg)

If we look at the right part of this screenshot. We notice that
1. table `INSERT-SELECT-1` came from table `orders` and `customers`
2. the`oid`, `cid`, `ottl`, `sid` columns of `INSERT-SELECT-1` were from ones of `orders` table
3.  the `cl` and `cem` columns of `INSERT-SELECT-1` were from ones of `customers` table. 
4. there are more tables on the right, `small_orders`, `medium_orders`, `large_orders` and `special_orders` are derived from `INSERT-SELECT-1`

Below this `INSERT-SELECT-1`, there is another lineage representation cases following the similar fashion. 

Now we look at the left part of this screenshot. We notice how the SQL statement is used to generate the target table, and how the columns in the target table are derived from the source tables. 

In this design review, I think we need to address two important issues:
1. How should we modify or create Dataset's `Upstream.pdl` to support column level lineage. To make it easier to understand, the current `Upstream.pdl` look like (deleted code comment for abbreviation) 
```
import com.linkedin.common.DatasetUrn

record Upstream {
  auditStamp: AuditStamp
  dataset: DatasetUrn
  type: DatasetLineageType
}
``` 

2. How could we provide sample script (python like) so end-user would use it to parse their `sql` statement easily, and ingest MCE message so Datahub could pick them up.

### Column-level Lineage Design
@clojurians-org has used [Uber's QueryParser](https://github.com/uber/queryparser) to do the following work, and help me understanding how the column-level lineage should be generated from a SQL statement. I think it's worth to bring it up right now to help us model the column-level lineage aspect. 


In a simplified `select-insert` SQL statement
```
INSERT OVERWRITE TABLE TMP.TFVDM1 (cpc, larluo)
SELECT CPC, LARLUO FROM ODS.FVS t
WHERE t.HDATASRC1 = 'A'
```
Interpreting this SQL statement, we understand  
1. there are two databases: `ODS` and `TMP`
2. table `TFVDM1` of database `TMP`  is from `FVS` of `ODS` (`t` for abbr.)
3.  `t` has three columns appeared, `HDATASRC1`, `CPC` and `LARLUO`
4. `cpc` and `larluo` have been turned into `TFVDM1`'s columns.

These analysis will help us understand the following `Query Parser` result.
After running QueryParser's analysis (initial script is developed), it will generate a JSON representation of column level lineage as two parts:
1. `TFVDM1` is from `FVS` based on `FVS`'s column `HDATASRC1` constraint;  
> Note: to represent column level lineage, it might not be necessary to show what query constraint has been used. It might not be shown on graph representation because this `hdatasrc1` doesn't appear in the target `tfvdm1` table. However, it is nice to have this information.
 
```
[
    {
        "Left": {
            "fqtnTableName": "tfvdm1",
            "fqtnDatabaseName": "tmp",
            "fqtnSchemaName": "tfvdm1"
        }
    },
    {
        "columns": [
            {
                "fqcnSchemaName": "fvs",
                "fqcnDatabaseName": "ods",
                "fqcnTableName": "fvs",
                "fqcnColumnName": "hdatasrc1"
            }
        ],
        "tables": [
            {
                "fqtnTableName": "fvs",
                "fqtnDatabaseName": "ods",
                "fqtnSchemaName": "fvs"
            }
        ]
    }
]
```
2. Columns in target `tfvdm1` table in relationship to columns in source `fvs` table

>  from the following, we understand that the `cpc` column of target `tfvdm1` table is from `cpc` and `hdatasrc1` columns of source `fvs` table. 

```
[
    {
        "Right": {
            "fqcnSchemaName": "tfvdm1",
            "fqcnDatabaseName": "tmp",
            "fqcnTableName": "tfvdm1",
            "fqcnColumnName": "cpc"
        }
    },
    {
        "columns": [
            {
                "fqcnSchemaName": "fvs",
                "fqcnDatabaseName": "ods",
                "fqcnTableName": "fvs",
                "fqcnColumnName": "cpc"
            },
            {
                "fqcnSchemaName": "fvs",
                "fqcnDatabaseName": "ods",
                "fqcnTableName": "fvs",
                "fqcnColumnName": "hdatasrc1"
            }
        ],
        "tables": []
    }
]

```
and, in similar fashion
```
[
    {
        "Right": {
            "fqcnSchemaName": "tfvdm1",
            "fqcnDatabaseName": "tmp",
            "fqcnTableName": "tfvdm1",
            "fqcnColumnName": "larluo"
        }
    },
    {
        "columns": [
            {
                "fqcnSchemaName": "fvs",
                "fqcnDatabaseName": "ods",
                "fqcnTableName": "fvs",
                "fqcnColumnName": "hdatasrc1"
            },
            {
                "fqcnSchemaName": "fvs",
                "fqcnDatabaseName": "ods",
                "fqcnTableName": "fvs",
                "fqcnColumnName": "larluo"
            }
        ],
        "tables": []
    }
]

```
`Dataset` has an `aspect` of `SchemaMetadata`. In the `SchemaMetadat`, the `fields` property is where we can use to store `column` information. 

#### 1. Establish a relationship between a `dataset` and its `fields`
 firstly, we need to establish a relationship between `dataset` and its `fields`. Let's call it `hasField` for right now.

In the `neo4j`, the expected graph representation would look like:
> disclaimer: I am not a neo4j expert, or actually know too much about it. 

![column-lineage-diagram](https://user-images.githubusercontent.com/356347/87818179-ca396a80-c82f-11ea-95d6-60eb1601c994.png)

To have this new relationship, a few files would be changed. We can use this [Onboarding to GMA Graph - Adding a new relationship type](https://github.com/linkedin/datahub/blob/master/docs/demo/graph-onboarding.md) as the reference for the implementation detail.

#### 2. Establish a relationship between a field of a source dataset and a field of target dataset. 
Let's call it `derivedBy` for right now
In the `neo4j`, the expected graph representation would look like:

![column-lineage-diagram-FULL](https://user-images.githubusercontent.com/356347/87819688-42a12b00-c832-11ea-9e1b-cfc6ea9ecc9b.png)

> there is typo in this graph. We actually mean `derivedBy`
In this diagram, the bottom is `source` dataset with its fields, the top is `target` dataset with its fields. The new relationship `derivedBy` illustrated how different column in target is generated.

Here are a few more notes:
1. [BaseRelationship.pdl](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/relationship/BaseRelationship.pdl) only supports `Urn` for `source` and `destination` right now. It won't work while we plan to use `SchemaField`  as `source` or `destination`.  Apparently, it won't make sense to lift `SchemaField` as the first class entity, so `derivedFrom` probably should be inherits from `BaseRelationship`

2. Current code base has [contains](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/relationship/Contains.pdl) relationship, but seems it has not gotten used. I assume it can be used for the `hasField` relationship defined earlier. If we do so, the `SchemaField` just has to treated and modified as first class entity. 


### Relationship Implementation
Mentioned earlier, I have proposed two new relationships: 
1. `HasField`  - the relationship between a dataset and its schemaFields. 
```
@pairings = [{
     "destination" : "com.linkedin.schema.Urn",
     "source" : "com.linkedin.common.urn.DatasetUrn"
 }]
record HasField includes BaseRelationship {

}
```

2 `DerivedBy` - the relationship between a dataset's schemaField and its parent's schemaField. 
```
@pairings = [{
     "destination" : "com.linkedin.common.urn.Urn",
     "source" : "com.linkedin.common.urn.Urn"
 }]
record DerivedBy includes BaseRelationship {

}
```

### Aspect Implementation
The next step is to create new aspect for `dataset`, named `FinegrainUpstreamLineage.pdl`. For simplicity, it has one property, 
```
record FineGrainUpstreamLineage {
 upstreams: array[FineGrainUpstream]
}
```
I also created a `FineGrainUpstream` model, it looks like
```
record FineGrainUpstream {
  auditStamp: AuditStamp
  dataset: DatasetUrn
  fields: array[FineGrainFieldUpstream]
}
```
The `FineGrainFieldUpstream` model is also new, it looks like this:
```
record FineGrainFieldUpstream {
 sourceField: string
 targetField: string
}
```
To compare the existing `table-level` lineage

FineGrainUpstreamLineage  --> UpStreamLineage
FineGrainUpstream --> UpStream
FineGrainFieldUpstream (new) --> n/a


### Relationship Builder Implementation
The next steps to implement graph builder with `metadata-builders`. 
1. we want to build the graph between a dataset and its fields. We name it `HasFieldBuilderFromSchemaMetadata`. 
>SchemaMetadata is an existing model
```
public class HasFieldBuilderFromSchemaMetadata  extends BaseRelationshipBuilder<SchemaMetadata> {

  public HasFieldBuilderFromSchemaMetadata() {
    super(SchemaMetadata.class);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull SchemaMetadata schemaMetadata) {
    if (schemaMetadata.getFields() == null) {
      return Collections.emptyList();
    }
    List<HasField> list = new ArrayList();
    for (SchemaField field : schemaMetadata.getFields()) {
      try {
        list.add(new HasField().setSource(urn).setDestination(new Urn(urn.toString() + ":" + field.getFieldPath())));
      } catch (URISyntaxException e) {

      }
    }
    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(
        list,
        REMOVE_ALL_EDGES_FROM_SOURCE)
    );
  }
}

```

> As you can see here, we create a `urn` based on the **_dataset urn_** and  _schemaField_

2. we want to build the graph between a dataset's field and the field being derived in another dataset. 

```
public class DerivedByBuilderFromFineGrainUpstreamLineage extends BaseRelationshipBuilder<FineGrainUpstreamLineage>  {
  public DerivedByBuilderFromFineGrainUpstreamLineage() {
    super(FineGrainUpstreamLineage.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull FineGrainUpstreamLineage upstreamLineage) {
    if (upstreamLineage.getUpstreams().isEmpty()) {
      return Collections.emptyList();
    }

    List<DerivedBy> list = new ArrayList();

    for(FineGrainUpstream stream: upstreamLineage.getUpstreams()) {
      for(FineGrainFieldUpstream fieldUpStream: stream.getFields()) {
        try {
          list.add(new DerivedBy()
              .setSource(Urn.createFromString(stream.getDataset().toString() + ":" + fieldUpStream.getSourceField()))
              .setDestination(Urn.createFromString(urn.toString() + ":" + fieldUpStream.getTargetField())));
        } catch (URISyntaxException e) {

        }
      }
    }

    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(list, REMOVE_ALL_EDGES_FROM_SOURCE));
  }

}

```


> pay attention on how the `source urn is set` in `.setSource(Urn.createFromString(stream.getDataset().toString() + ":" + fieldUpStream.getSourceField()))`
and `destination urn is set` in `setDestination(Urn.createFromString(urn.toString() + ":" + fieldUpStream.getTargetField())`

The base `BaseRelationshipBuilder` defines there is an urn always pointing to the dataset having this [FineGrainUpstreamLineage] relationship aspect.  And this `urn` is the destination of upstream lineage.

The final step, we register these two relationship builders with `DatasetGraphBuilder`
```
Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new DownstreamOfBuilderFromUpstreamLineage());
          add(new HasFieldBuilderFromSchemaMetadata());
          add(new OwnedByBuilderFromOwnership());
          add(new DerivedByBuilderFromFineGrainUpstreamLineage());
        }
      });
```




## How we teach this
This implementation supports features 
1. relationship between a dataset and its fields
2. relationship between a dataset and the other dataset (already available)
3. relationship between a dataset field derived by **one or more fields**. 

In this Neo4j graph presentation, 
1. dataset `c_bar_13` has fields of `bar131`, `bar132`, `bar133` and `col134`;
2. `p_foo_13` dataset has fields of foo131, foo132, foo133, and foo134
3. 'p_2_foo_13` dataset has fields of 2_foo131, 2_foo132, 2_foo133, and 2_foo134
4. dataset `c_bar_13` is downstream of `p_foo_13` and `p_2_foo_13`
5. bar131 of c_bar_13 is derived by foo131 and 2_foo131
6. bar 132 of c_bar_13 is derived by foo132 and 2_foo132



![Screen Shot 2020-08-04 at 11 42 54 AM](https://user-images.githubusercontent.com/356347/89320841-a8215400-d647-11ea-882b-8c9dea4bdbf6.png)



## Drawbacks
Because `SchemaField` is not a first-class entity in Datahub, the current [Neo4jGraphWriterDao](https://github.com/linkedin/datahub/blob/master/metadata-dao-impl/neo4j-dao/src/main/java/com/linkedin/metadata/dao/internal/Neo4jGraphWriterDAO.java) only supports the first class entity by enforcing [validation rule] implemented by [RelationshipValidator](https://github.com/linkedin/datahub/blob/master/metadata-validators/src/main/java/com/linkedin/metadata/validator/RelationshipValidator.java)

In the other word, the validator looks at the source and target of a pair of relationship:
1. whether it is a Urn
2. whether it is a registered Urn (first class entity)

I did the following two hacks
1. remove this relationship validator. 
2. create a SchemaField urn **on the fly** by combining the dataset it belongs to and its field path. (more details later)

## Alternatives
I have not looked at other alertnatives. I probably should have looked at how Apache Atlas solves the column-level lineage problem.


## Rollout / Adoption Strategy
Since this implemetation introuces a new `aspect:FineGrainUpstreamLineage` for `dataset`, it won't affect the existing aspects of a dataset. 
To adopte this new feature, a related sample MCE message is included [bootstrap_mce.dat](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/mce-cli/bootstrap_mce.dat)
Also, sample rest API requests in `postman` format is added under `contrib` folder.

## Unresolved questions

1. Scripts to auto-extract column mapping between source and destination
2. Assemble into a MCE message
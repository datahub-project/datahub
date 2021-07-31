---
title: The Metadata Model
sidebar_label: The Metadata Model
slug: /metadata-modeling/metadata-model
---

# How does DataHub model metadata?

DataHub uses the Pegasus schema (PDL) language extended with a custom set of annotations to model metadata.

Conceptually, metadata is modeled using the following abstractions

- **Entities**: An entity is the primary node in the metadata graph. For example, an instance of a Dataset or a CorpUser is an Entity. An entity is made up of a unique identifier (a primary key) and groups of metadata which we call aspects.


- **Aspects**: An aspect is a collection of attributes that describes a particular facet of an entity. They are the smallest atomic unit of write in DataHub. That is, Multiple aspects associated with the same Entity can be updated independently. For example, DatasetProperties contains a collection of attributes that describes a Dataset. Aspects can be shared across entities, for example the "Ownership" an aspect is re-used across all the Entities that have owners. 


- **Keys & Urns**: A key is a special type of aspect that contains the fields that uniquely identify an individual Entity. Key aspects can be serialized into *Urns*, which represent a stringified form of the key fields used for primary-key lookup. Moreover, *Urns* can be converted back into key aspect structs, making key aspects a type of "virtual" aspect. Key aspects provide a mechanism for clients to easily read fields comprising the primary key, which are usually generally useful like Dataset names, platform names etc. Urns provide a friendly handle by which Entities can be queried without requiring a fully materialized struct. 


- **Relationships**: A relationship represents a named edge between 2 entities. They are declared via foreign key attributes within Aspects along with a custom annotation (@Relationship). Relationships permit edges to be traversed bi-directionally. For example, a Chart may refer to a CorpUser as its owner via a relationship named "OwnedBy". This edge would be walkable starting from the Chart *or* the CorpUser instance.


Here is an example graph consisting of 3 types of entity (CorpUser, Chart, Dashboard), 2 types of relationship (OwnedBy, Contains), and 3 types of metadata aspect (Ownership, ChartInfo, and DashboardInfo).

![metadata-modeling](../imgs/metadata-model-chart.png)

## Querying the Metadata Graph 

DataHub’s modeling language allows you to optimize metadata persistence to align with query patterns.

There are three supported ways to query the metadata graph: by primary key lookup, a search query, and via relationship traversal. 

### Querying an Entity

#### Fetching Latest Entity Aspects (Snapshot)

Querying an Entity by primary key means using the "entities" endpoint, passing in the 
urn of the entity to retrieve. 

For example, to fetch a Chart entity, we can use the following CURL: 

```
curl --location --request GET 'http://localhost:8080/entities/urn%3Ali%3Achart%3Acustomers
```

The type of request will return a set of versioned aspects, each at the latest version. 

As you'll notice, we perform the lookup using the url-encoded *Urn* associated with an entity. 
The response would be an "Entity" record containing the Entity Snapshot (which in turn contains the latest aspects associated with the Entity).

#### Fetching Versioned Aspects

DataHub also supports fetching individual pieces of metadata about an Entity, which we call aspects. To do so, 
you'll provide both an Entity's primary key (urn) along with the aspect name and version that you'd like to retrieve. 

For example, to fetch the latest version of a Dataset's SchemaMetadata aspect, you would issue the following query:

```
curl 'http://localhost:8080/aspects/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Afoo%2Cbar%2CPROD)?aspect=schemaMetadata&version=0'

{
   "version":0,
   "aspect":{
      "com.linkedin.schema.SchemaMetadata":{
         "created":{
            "actor":"urn:li:corpuser:fbar",
            "time":0
         },
         "platformSchema":{
            "com.linkedin.schema.KafkaSchema":{
               "documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
            }
         },
         "lastModified":{
            "actor":"urn:li:corpuser:fbar",
            "time":0
         },
         "schemaName":"FooEvent",
         "fields":[
            {
               "fieldPath":"foo",
               "description":"Bar",
               "type":{
                  "type":{
                     "com.linkedin.schema.StringType":{
                        
                     }
                  }
               },
               "nativeDataType":"string"
            }
         ],
         "version":0,
         "hash":"",
         "platform":"urn:li:dataPlatform:foo"
      }
   }
}
```

#### Fetching Timeseries Aspects

DataHub supports an API for fetching a group of Timeseries aspects about an Entity. For example, you may want to use this API
to fetch recent profiling runs & statistics about a Dataset. To do so, you can issue a "get" request against the `/aspects` endpoint.

For example, to fetch dataset profiles (ie. stats) for a Dataset, you would issue the following query:

```
curl -X POST 'http://localhost:8080/aspects?action=getTimeseriesAspectValues' \
--data '{
    "urn": "urn:li:dataset:(urn:li:dataPlatform:redshift,global_dev.larxynx_carcinoma_data_2020,PROD)",
    "entity": "dataset",
    "aspect": "datasetProfile",
    "startTimeMillis": 1625122800000,
    "endTimeMillis": 1627455600000
}'

{
   "value":{
      "limit":10000,
      "aspectName":"datasetProfile",
      "endTimeMillis":1627455600000,
      "startTimeMillis":1625122800000,
      "entityName":"dataset",
      "values":[
         {
            "aspect":{
               "value":"{\"timestampMillis\":1626912000000,\"fieldProfiles\":[{\"uniqueProportion\":1.0,\"sampleValues\":[\"123MMKK12\",\"13KDFMKML\",\"123NNJJJL\"],\"fieldPath\":\"id\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":3742},{\"uniqueProportion\":1.0,\"min\":\"1524406400000\",\"max\":\"1624406400000\",\"sampleValues\":[\"1640023230002\",\"1640343012207\",\"16303412330117\"],\"mean\":\"1555406400000\",\"fieldPath\":\"date\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":3742},{\"uniqueProportion\":0.037,\"min\":\"21\",\"median\":\"68\",\"max\":\"92\",\"sampleValues\":[\"45\",\"65\",\"81\"],\"mean\":\"65\",\"distinctValueFrequencies\":[{\"value\":\"12\",\"frequency\":103},{\"value\":\"54\",\"frequency\":12}],\"fieldPath\":\"patient_age\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":79},{\"uniqueProportion\":0.00820873786407767,\"sampleValues\":[\"male\",\"female\"],\"fieldPath\":\"patient_gender\",\"nullCount\":120,\"nullProportion\":0.03,\"uniqueCount\":2}],\"rowCount\":3742,\"columnCount\":4}",
               "contentType":"application/json"
            }
         },
      ]
   }
}
```

You'll notice that the aspect itself is serialized as escaped JSON. This is part of a shift toward a more generic set of READ / WRITE APIs
that permit serialization of aspects in different ways. By default, the content type will be JSON, and the aspect can be deserialized into a normal JSON object 
in the language of your choice. Note that this will soon become the de-facto way to both write and read individual aspects. 



### Search Query

A search query allows you to search for entities matching an arbitrary string. 

For example, to search for entities matching the term "customers", we can use the following CURL:

```
curl --location --request POST 'http://localhost:8080/entities?action=search' \                           
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
    "input": "\"customers\"",
    "entity": "chart",
    "start": 0,
    "count": 10
}'
```

The notable parameters are `input` and `entity`. `input` specifies the query we are issuing and `entity` specifies the Entity Type we want to search over. This is the common name of the Entity as defined in the @Entity definition. The response contains a list of Urns, that can be used to fetch the full entity.

### Relationship Query

A relationship query allows you to find Entity connected to a particular source Entity via an edge of a particular type.

For example, to find the owners of a particular Chart, we can use the following CURL:

```
curl --location --request GET --header 'X-RestLi-Protocol-Version: 2.0.0' 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn:li:chart:customers&types=OwnedBy'
```

The notable parameters are `direction`, `urn` and `types`. The response contains *Urns* associated with all entities connected 
to the primary entity (urn:li:chart:customer) by an relationship named "OwnedBy". That is, it permits fetching the owners of a given
chart. 

### Special Aspects

There are 2 "special" aspects worth mentioning: 

1. Key aspects
2. Browse aspect
3. Timeseries aspects

#### Key aspects

As introduced above, Key aspects are structs / records that contain the fields that uniquely identify an Entity. There are 
some constraints about the fields that can be present in Key aspects:

- All fields must be of STRING or ENUM type
- All fields must be REQUIRED

Keys can be created from and turned into *Urns*, which represent the stringified version of the Key record. 
The algorithm used to do the conversion is straightforward: the fields of the Key aspect are substituted into a
string template based on their index (order of definition) using the following template:

```aidl
// Case 1: # key fields == 1
urn:li:<entity-name>:key-field-1

// Case 2: # key fields > 1
urn:li:<entity-name>:(key-field-1, key-field-2, ... key-field-n) 
```

By convention, key aspects are defined under `metadata-models/src/main/pegasus/com/linkedin/metadata/key`.

##### Example

A CorpUser can be uniquely identified by a "username", which should typically correspond to an LDAP name. 

Thus, it's Key Aspect is defined as the following: 

```aidl
namespace com.linkedin.metadata.key

/**
 * Key for a CorpUser
 */
@Aspect = {
  "name": "corpUserKey"
}
record CorpUserKey {
  /**
  * The name of the AD/LDAP user.
  */
  username: string
}
```

and it's Entity Snapshot model is defined as 

```aidl
/**
 * A metadata snapshot for a specific CorpUser entity.
 */
@Entity = {
  "name": "corpuser",
  "keyAspect": "corpUserKey"
}
record CorpUserSnapshot {

  /**
   * URN for the entity the metadata snapshot is associated with.
   */
  urn: CorpuserUrn

  /**
   * The list of metadata aspects associated with the CorpUser. Depending on the use case, this can either be all, or a selection, of supported aspects.
   */
  aspects: array[CorpUserAspect]
}
```

Using a combination of the information provided by these models, we are able to generate the Urn corresponding to a CorpUser as 

```
urn:li:corpuser:<username>
```

Imagine we have a CorpUser Entity with the username "johnsmith". In this world, the JSON version of the Key Aspect associated with the Entity would be 

```aidl
{
  "username": "johnsmith"
}
```

and its corresponding Urn would be

```aidl
urn:li:corpuser:johnsmith 
```

#### BrowsePaths aspect

The BrowsePaths aspect allows you to define a custom "browse path" for an Entity. A browse path is a way to hierarchically organize
entities. They manifest within the "Explore" features on the UI, allowing users to navigate through trees of related entities of a given type. 

To support browsing a particular entity, simply include the "BrowsePaths" aspect in its aspect union: 

```aidl
// DatasetAspect.pdl

/**
 * A union of all supported metadata aspects for a Dataset
 */
typeref DatasetAspect = union[
  DatasetKey,
  ...
  BrowsePaths
]
```

By declaring this aspect, you can produce custom browse paths as well as query for browse paths manually using a CURL like the following:

```aidl
curl --location --request POST 'http://localhost:8080/entities?action=browse' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
    "path": "/my/custom/browse/path",
    "entity": "dataset",
    "start": 0,
    "limit": 10
}'
```

Notice that you must provide 

a. A "/"-delimited root path for which to fetch results.
b. An entity "type" using its common name ("dataset" in the example above). 

#### Timeseries aspects

Timeseries aspects are aspects that have a timestampMillis field, and are meant for aspects that continuously change on a
timely basis e.g. data profiles, usage statistics, etc.

Each timeseries aspect must be declared "type": "timeseries" and must
include [TimeseriesAspectBase](https://github.com/linkedin/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/timeseries/TimeseriesAspectBase.pdl)
, which contains a timestampMillis field.

Timeseries aspect cannot have any fields that have the @Searchable or @Relationship annotation, as it goes through a
completely different flow.

Please refer
to [DatasetProfile](https://github.com/linkedin/datahub/tree/master/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProfile)
to see an example of a timeseries aspect.

Because timeseries aspects are updated on a frequent basis, ingests of these aspects go straight to elastic search (
instead of being stored in local DB). 

You can retrieve timeseries aspects using the "aspects?action=getTimeseriesAspectValues" end point. 
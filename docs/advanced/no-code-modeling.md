# No Code Metadata Modeling 

## Summary of changes

As part of the No Code Metadata Modeling initiative, we've made radical changes to the DataHub stack. 

Specifically, we've 

- Decoupled the persistence layer from Java + Rest.li specific concepts 
- Consolidated the per-entity Rest.li resources into a single general-purpose Entity Resource
- Consolidated the per-entity Graph Index Writers + Readers into a single general-purpose Neo4J DAO 
- Consolidated the per-entity Search Index Writers + Readers into a single general-purpose ES DAO. 
- Developed mechanisms for declaring search indexing configurations + foreign key relationships as annotations
on PDL models themselves.
- Introduced a special "Browse Paths" aspect that allows the browse configuration to be 
pushed into DataHub, as opposed to computed in a blackbox lambda sitting within DataHub
- Introduced special "Key" aspects for conveniently representing the information that identifies a DataHub entities via
a normal struct.
- Removed the need for hand-written Elastic `settings.json` and `mappings.json`. (Now generated at runtime)
- Removed the need for the Elastic Set Up container (indexes are not registered at runtime)
- Simplified the number of models that need to be maintained for each DataHub entity. We removed the need for
     1. Relationship Models
     2. Entity Models 
     3. Urn models + the associated Java container classes 
     4. 'Value' models, those which are returned by the Rest.li resource

In doing so, dramatically reducing the level of effort required to add or extend an existing entity.

For more on the design considerations, see the **Design** section below. 

## Migration

TODO: 


## Engineering Spec

This section will provide a more in-depth overview of the design considerations that were at play when working on the No
Code initiative. 

# Use Cases

Who needs what & why?

| As a             | I want to                | because 
| ---------------- | ------------------------ | ------------------------------
| DataHub Operator | Add new entities         | The default domain model does not match my business needs
| DataHub Operator | Extend existing entities | The default domain model does not match my business needs

What we heard from folks in the community is that adding new entities + aspects is just **too difficult**.

They'd be happy if this process was streamlined and simple. **Extra** happy if there was no chance of merge conflicts in the future. (no fork necessary)

# Goals

### Primary Goal

**Reduce the friction** of adding new entities, aspects, and relationships.

### Secondary Goal

Achieve the primary goal in a way that does not require a fork.

# Requirements

### Must-Haves

1. Mechanisms for **adding** a browsable, searchable, linkable GMS entity by defining one or more PDL models
  - GMS Endpoint for fetching entity
  - GMS Endpoint for fetching entity relationships
  - GMS Endpoint for searching entity
  - GMS Endpoint for browsing entity
2. Mechanisms for **extending** a ****browsable, searchable, linkable GMS ****entity by defining one or more PDL models
  - GMS Endpoint for fetching entity
  - GMS Endpoint for fetching entity relationships
  - GMS Endpoint for searching entity
  - GMS Endpoint for browsing entity
3. Mechanisms + conventions for introducing a new **relationship** between 2 GMS entities without writing code
4. Clear documentation describing how to perform actions in #1, #2, and #3 above published on [datahubproject.io](http://datahubproject.io)

## Nice-to-haves

1. Mechanisms for automatically generating a working GraphQL API using the entity PDL models
2. Ability to add / extend GMS entities without a fork.
  - e.g. **Register** new entity / extensions *at runtime*. (Unlikely due to code generation)
  - or, **configure** new entities at *deploy time*

## What Success Looks Like

1. Adding a new browsable, searchable entity to GMS (not DataHub UI / frontend) takes 1 dev < 15 minutes.
2. Extending an existing browsable, searchable entity in GMS takes 1 dev < 15 minutes
3. Adding a new relationship among 2 GMS entities takes 1 dev < 15 minutes
4. [Bonus] Implementing the `datahub-frontend` GraphQL API for a new / extended entity takes < 10 minutes


## Design

## State of the World

### Modeling

Currently, there are various models in GMS:

1. [Urn](https://github.com/linkedin/datahub/blob/master/li-utils/src/main/pegasus/com/linkedin/common/DatasetUrn.pdl) - Structs composing primary keys
2. [Root] [Snapshots](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot/Snapshot.pdl) - Container of aspects
3. [Aspects](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/DashboardAspect.pdl) - Optional container of fields
4. [Values](https://github.com/linkedin/datahub/blob/master/gms/api/src/main/pegasus/com/linkedin/dataset/Dataset.pdl), [Keys](https://github.com/linkedin/datahub/blob/master/gms/api/src/main/pegasus/com/linkedin/dataset/DatasetKey.pdl) - Model returned by GMS [Rest.li](http://rest.li) API (public facing)
5. [Entities](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/entity/DatasetEntity.pdl) - Records with fields derived from the URN. Used only in graph / relationships
6. [Relationships](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/relationship/Relationship.pdl) - Edges between 2 entities with optional edge properties
7. [Search Documents](https://github.com/linkedin/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/metadata/search/ChartDocument.pdl) - Flat documents for indexing within Elastic index
  - And corresponding index [mappings.json](https://github.com/linkedin/datahub/blob/master/gms/impl/src/main/resources/index/chart/mappings.json), [settings.json](https://github.com/linkedin/datahub/blob/master/gms/impl/src/main/resources/index/chart/settings.json)

Various components of GMS depend on / make assumptions about these model types:

1. IndexBuilders depend on **Documents**
2. GraphBuilders depend on **Snapshots**
3. RelationshipBuilders depend on **Aspects**
4. Mae Processor depend on **Snapshots, Documents, Relationships**
5. Mce Processor depend on **Snapshots, Urns**
6. [Rest.li](http://rest.li) Resources on **Documents, Snapshots, Aspects, Values, Urns**
7. Graph Reader Dao (BaseQueryDao) depends on **Relationships, Entity**
8. Graph Writer Dao (BaseGraphWriterDAO) depends on **Relationships, Entity**
9. Local Dao Depends on **aspects, urns**
10. Search Dao depends on **Documents**

Additionally, there are some implicit concepts that require additional caveats / logic:

1. Browse Paths - Requires defining logic in an entity-specific index builder to generate.
2. Urns - Requires defining a) an Urn PDL model and b) a hand-written Urn class 

As you can see, there are many tied up concepts. Fundamentally changing the model would require a serious amount of refactoring, as it would require new versions of numerous components.

The challenge is, how can we meet the requirements without fundamentally altering the model?

## Solution

In a nutshell, the idea is to consolidate the number of models + code we need to write on a per-entity basis.
We intend to achieve this by making search index + relationship configuration declarative, specified as part of the model
definition itself. 

We will use this configuration to drive more generic versions of the index builders + rest resources, 
with the intention of reducing the overall surface area of GMS. 

During this initiative, we will also seek to make the concepts of Browse Paths and Urns declarative. Browse Paths
will be provided using a special BrowsePaths aspect. Urns will no longer be strongly typed. 

### Defining an Entity

We will outline what the experience of adding a new Entity should look like. We'll name it "MyNewEntity". 

Step 1: Define the Entity Key Aspect

A key represents the fields that uniquely identify the entity. Previously, these fields were
part of the Urn Java Class that was defined for each entity. 

In the new world, we will permit specification of a normal PDL struct to represent fields of the primary key. 
This struct will be used to generate a serialized string key, represented by an Urn. Each field in the key struct
will be converted into a single part of the Urn's tuple, in order.

```
/**
 * Key for MyNewEntity
 */
@Aspect = {
  "name": "myNewEntityKey",
  "isKey": true
}
record MyNewEntityKey {
  /**
  * The name of my entity
  */
  name: string
  
  /**
  * The environment where my new entity lives. 
  */
  origin: FabricType
}
```

Notice that Key aspects are special; they need to be annotated with an @Aspect annotation that contains a field
"isKey": true. This instructs DataHub that this struct should be considered a primary key for an entity. 

The Urn representation of the Entity Key shown above would be:

```urn:li:<entityType>:(<name>,<origin>)```

Step 2: Define custom aspects

Each aspect represents an independent package of data about a single instance of an entity. To define a new custom aspect for your
new entity, simply define a PDL record annotated with @Aspect.

```
@Aspect = {
  "name": "myNewEntityInfo"
}
record MyNewEntityInfo {

  /**
   * displayName of this user ,  e.g.  Hang Zhang(DataHQ)
   */
  displayName: optional string
  
  ...
}
```

Step 3: Define the Entity Aspect Union

Today, aspects should be included in a per-entity Aspect union. Any aspects containing metadata about
an entity should be included. Note that any record appearing in the Union should be annotated with @Aspect.

```
namespace com.linkedin.metadata.aspect

import com.linkedin.metadata.key.MyNewEntityKey
import com.linkedin.myentity.MyNewEntityInfo

/**
 * A union of all supported metadata aspects for a MyNewEntity
 */
typeref MyNewEntityAspect = union[MyNewEntityKey, MyNewEntityInfo]
```


Step 4: Define an Entity Snapshot 

```
/**
 * A metadata snapshot for a specific CorpUser entity.
 */
@Entity = {
  "name": "myNewEntity",
  "searchable": true,
  "browsable": true
}
record MyNewEntity {

  /**
   * URN for the entity the metadata snapshot is associated with.
   */
  urn: Urn

  /**
   * The list of metadata aspects associated with the CorpUser. Depending on the use case, this can either be all, or a selection, of supported aspects.
   */
  aspects: array[MyNewEntityAspect]
}
```


Step 5: Add the snapshot to the Snapshot.pdl union

```
 /**
  * A union of all supported metadata snapshot types.
  */
  typeref Snapshot = union[
  .... 
  MyNewSnapshot
  ]
```

### Configuring Searchable Fields

TODO

### Configuring Relationships 

TODO 

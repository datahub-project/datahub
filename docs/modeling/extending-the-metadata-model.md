---
title: Extending the metadata model
sidebar_label: Extending the metadata model
slug: /metadata-modeling/extending-the-metadata-model
---

# How to extend the Metadata model

You can extend the metadata model by either creating a new Entity or extending an existing one.
Unsure if you need to create a new entity or add an aspect to an existing entity?
Read [metadata-model](./metadata-model.md) to understand these two concepts and figure out which one you need to create.

We will outline what the experience of adding a new Entity should look like through a real example of adding the Dashboard Entity. If you want to extend an existing Entity or Aspect, you can skip directly to Step 4.

High level, an entity is made up of

1) a union of Aspects, or bundles of metadata,
2) a Key Aspect, which uniquely identifies an instance of an entity,
3) A snapshot, which pais a union of aspects with a serialized key, or urn.

Now we'll walk you through the steps required to create these metadata models, ingest and view your extensions to the model.
We will use the Dashboard entity as an example. This entity already exists in Datahub, and you can view these files 
and how they are used.

### Step 1: Define the Entity Key Aspect

A key represents the fields that uniquely identify the entity. For those familiar with Datahub’s previous architecture, these fields were previously part of the Urn Java Class that was defined for each entity.

This struct will be used to generate a serialized string key, represented by an Urn. Each field in the key struct will be converted into a single part of the Urn's tuple, in the order they are defined.

For example,

```
namespace com.linkedin.metadata.key

/**
 * Key for a Dashboard
 */
@Aspect = {
  "name": "dashboardKey",
}
record DashboardKey {
  /**
  * The name of the dashboard tool such as looker, redash etc.
  */
  @Searchable = {
    "fieldName": "tool",
    "fieldType": "TEXT_PARTIAL",
    "addToFilters": true,
    "boostScore": 4.0
  }
  dashboardTool: string

  /**
  * Unique id for the dashboard. This id should be globally unique for a dashboarding tool even when there are multiple deployments of it. As an example, dashboard URL could be used here for Looker such as 'looker.linkedin.com/dashboards/1234'
  */
  dashboardId: string
}

```

The Urn representation of the Entity Key shown above would be:

```urn:li:dashboard:(<tool>,<id>)```

Keys need to be annotated with an @Aspect annotation, This instructs DataHub that this struct can be a part of.

The key can also be annotated with the two index annotations, @Searchable and @Relationship. This tells Datahub to use the fields in the key to create relationships and index fields for search. See Step 4 for more details on the annotation model.

### Step 2: Define the Entity Aspect Union

You must create an Aspect union to define what aspects an entity can contain. Any record appearing in the Union should be annotated with @Aspect. To begin with, your entity’s AspectUnion will just contain the new Key until you create more custom aspects (see Step 4). As you can see, the Dashboard’s aspect union contains its key along with the other custom Aspects it includes.

```
namespace com.linkedin.metadata.aspect

import com.linkedin.metadata.key.DashboardKey
import com.linkedin.dashboard.DashboardInfo
import com.linkedin.common.Ownership
import com.linkedin.common.Status
import com.linkedin.common.GlobalTags
import com.linkedin.common.BrowsePaths

/**
 * A union of all supported metadata aspects for a Dashboard
 */
typeref DashboardAspect = union[
    DashboardKey,
    DashboardInfo,
    Ownership,
    Status,
    GlobalTags,
    BrowsePaths
]
```

The first aspect will always be the Entity’s key. The other aspects can be Dashboard specific, like DashboardInfo, or shared, such as Ownership. This union can be extended over time as you expand the metadata model. You can include any existing type with the @Aspect annotation in your entity’s aspect union or create new ones- Step 4 goes into detail about how to create a new Aspect.

### Step 3: Define an Entity Snapshot

The snapshot describes the format of how an entity is serialized for read and write operations to GMS,
the generic metadata store. All snapshots have two fields, `urn` of type `Urn` and `snapshot`
of type `union[Aspect1, Aspect2, ...]`.

The snapshot needs an `@Entity` annotation with the entity’s name.
The name is used for specifying entity type when searching, using autocomplete, etc.

```
namespace com.linkedin.metadata.snapshot

import com.linkedin.common.DashboardUrn
import com.linkedin.metadata.aspect.DashboardAspect

/**
 * A metadata snapshot for a specific Dashboard entity.
 */
@Entity = {
  "name": "dashboard",
  "searchable": true,
  "browsable": true
}
record DashboardSnapshot {

  /**
   * URN for the entity the metadata snapshot is associated with.
   */
  urn: DashboardUrn

  /**
   * The list of metadata aspects associated with the dashboard.
   */
  aspects: array[DashboardAspect]
}
```

Step 4: Define custom aspects

Some aspects, like Ownership and GlobalTags, are general purpose,
and you can include them in your entity’s Aspect union freely.
They will extend your entity with their attributes all without creating an aspect.
If you only want to use existing aspects, you can skip to Step 5.
However, to include attributes that are not included in an existing Aspect, a new Aspect must be created.

Let’s look at the DashboardInfo aspect as an example of what goes into a new aspect.

```
namespace com.linkedin.dashboard

import com.linkedin.common.AccessLevel
import com.linkedin.common.ChangeAuditStamps
import com.linkedin.common.ChartUrn
import com.linkedin.common.Time
import com.linkedin.common.Url
import com.linkedin.common.CustomProperties
import com.linkedin.common.ExternalReference

/**
 * Information about a dashboard
 */
@Aspect = {
  "name": "dashboardInfo"
}
record DashboardInfo includes CustomProperties, ExternalReference {

  /**
   * Title of the dashboard
   */
  @Searchable = {
    "fieldType": "TEXT_WITH_PARTIAL_MATCHING",
    "queryByDefault": true,
    "enableAutocomplete": true,
    "boostScore": 10.0
  }
  title: string

  /**
   * Detailed description about the dashboard
   */
  @Searchable = {
    "fieldType": "TEXT",
    "queryByDefault": true,
    "hasValuesFieldName": "hasDescription"
  }
  description: string

  /**
   * Charts in a dashboard
   */
  @Relationship = {
    "/*": {
      "name": "Contains",
      "entityTypes": [ "chart" ]
    }
  }
  charts: array[ChartUrn] = [ ]
 
  /**
   * Captures information about who created/last modified/deleted this dashboard and when
   */
  lastModified: ChangeAuditStamps

  /**
   * URL for the dashboard. This could be used as an external link on DataHub to allow users access/view the dashboard
   */
  dashboardUrl: optional Url

  /**
   * Access level for the dashboard
   */
  @Searchable = {
    "fieldType": "KEYWORD",
    "addToFilters": true
  }
  access: optional AccessLevel

  /**
   * The time when this dashboard last refreshed
   */
  lastRefreshed: optional Time
}
```

The Aspect has four key components: its properties, the @Aspect annotation, the @Searchable annotation and the @Relationship annotation. Let’s break down each part.

- The Aspect’s properties. The record’s properties can be declared as a field on the record, or by including another record in the Aspect’s definition (`record DashboardInfo includes CustomProperties, ExternalReference {`). Properties can be defined as
pdl primitives, enums, or collections (see [pdl schema documentation](https://linkedin.github.io/rest.li/pdl_schema))
references to other entities, of type Urn or optionally `<Entity>Urn`
another pdl record type, even if that type is an Aspect itself
- The @Aspect annotation. This is used to declare that the record is an Aspect and can be included in an entity’s Snapshot. It also declares an identifier for the aspect so it can be stored and retrieved. Unlike the other two annotations, @Aspect is applied to the entire record rather than a specific field.
- The @Searchable annotation. This annotation can be applied to any primitive field to indicate that it should be indexed in Elasticsearch and can be searched on. For a complete guide on using the search annotation, see the annotation docs further down in this document.
- The @Relationship annotations. These annotations create edges between the Snapshot’s Urn and the destination of the annotated field when the snapshots are ingested. @Relationship annotations must be applied to fields of type Urn. In the case of DashboardInfo, the `charts` field is an Array of Urns. The @Relationship annotation cannot be applied directly to an array of Urns. That’s why you see the use of an Annotation override (`”/*”:) to apply the @Relationship annotation to the Urn directly.
  Read more about overrides in the annotation docs further down on this page.

After you create your Aspect, you need to add it into the Aspect Union of each entity you’d like to attach the aspect to. Refer back to Step 2 for how Aspects are added to Aspect Unions.

### Step 5: Re-build datahub to have access to your new or updated entity

Run `/.gradlew build` from the repository root to rebuild Datahub with access to your new entity.

Then, re-deploy gms, mae-consumer and mce-consumer (see [docker development](../../docker/README.md) for details on how to deploy during development). This will allow Datahub to read and write Snapshots of your new entity, along with server search and graph queries for that entity type.

To emit snapshots to ingest from the Datahub CLI tool, first install datahub cli locally [following the instructions here](../../metadata-ingestion/developing.md). `./gradlew build` generated the avro schemas your local ingestion cli tool uses earlier. After following the developing guide, you should be able to emit your new event using the local datahub cli.

Now you are ready to start ingesting metadata for your new entity!


### (Optional) Step 6: Extend the datahub frontend to view your entity in GraphQL & React

At the moment, custom React and Grapqhl code needs to be written to view your entity in GraphQL or React. For instructions on how to start extending the GraphQL graph, see [graphql docs](../../datahub-graphql-core/README.md). Once you’ve done that, you can follow the guide [here](../../datahub-web-react/README.md) to add your entity into the React UI.

## Metadata Annotations
There are four annotations that tell DataHub how to treat certain fields and structs.

#### @Entity
This annotation is applied to each Snapshot record, such as DashboardSnapshot.pdl. Each snapshot that is included in Snapshot.pdl must have this annotation.

It takes the following parameters:

```aidl
@Entity = {
// name used when referring to the entity in APIs.
String name;
}
```


#### @Aspect
This annotation is applied to each Aspect record, such as DashboardInfo.pdl. Each aspect that is included in an entity’s AspectUnion must have this annotation.

It takes the following parameters:

```aidl
@Aspect = {
  // name used when referring to the aspect in APIs.
  String name;
}
```

#### @Searchable

This annotation is applied to fields inside an Aspect. It tells Datahub to index that field so it can be searched.

It takes the following parameters:

```aidl
@Searchable = {
  // Name of the field in the search index. Defaults to the field name in the schema
  Optional String fieldName;
  // Type of the field. Defines how the field is indexed and matched
  // One of      KEYWORD, TEXT, TEXT_PARTIAL, BROWSE_PATH, URN, URN_PARTIAL, BOOLEAN,  COUNT
  FieldType fieldType;
  // Whether we should match the field for the default search query
  Optional boolean queryByDefault;
  // Whether we should use the field for default autocomplete
  Optional boolean enableAutocomplete;
  // Whether or not to add field to filters.
  Optional boolean addToFilters;
  // Boost multiplier to the match score. Matches on fields with higher boost score ranks higher
  Optional double boostScore;
  // If set, add a index field of the given name that checks whether the field exists
  Optional String hasValuesFieldName;
  // If set, add a index field of the given name that checks the number of elements
  Optional String numValuesFieldName;
  // (Optional) Weights to apply to score for a given value.
  Map<Object, Double> weightsPerFieldValue;
}
```

Let’s take a look at a real world example using the `title` field of `DashboardInfo.pdl`:

```aidl
 /**
   * Title of the dashboard
   */
  @Searchable = {
    "fieldType": "TEXT_PARTIAL",
    "enableAutocomplete": true,
    "boostScore": 10.0
  }
  title: string
```

This annotation is saying that we want to index the title field in Elasticsearch. We want to support partial matches on the title, so queries for `Cust` should return a Dashboard with the title `Customers`. `enableAutocomplete` is set to true, meaning that we can autocomplete on this field when typing into the search bar. Finally, a boostScore of 10 is provided, meaning that we should prioritize matches to title over matches to other fields, such as description, when ranking.

Now, when Datahub ingests Dashboards, it will index the Dashboard’s title in Elasticsearch. When a user searches for Dashboards, that query will be used to search on the title index and matching Dashboards will be returned.

The settings for how each field is indexed is defined by the field type. Each field type is associated with a set of analyzers Elasticsearch will use to tokenize the field. Such sets are defined in the LINK(MappingsBuider), which generates the mappings for the index for each entity given the fields with the search annotations. To customize the set of analyzers used to index a certain field, you must add a new field type and define the set of mappings to be applied in the LINK(MappingsBuilder).


#### @Relationship

This annotation is applied to fields inside an Aspect. This annotation creates edges between an Entity’s Urn and the destination of the annotated field when the Entity is ingested. @Relationship annotations must be applied to fields of type Urn.

It takes the following parameters:

```aidl
@Relationship = {
  // name of the relationship. This is used for issuing graph queries on a subset of relationship types.
  String name;
  // list of valid destination entity types- default is all
  Optional List<String> entityTypes;
}
```

Let’s take a look at a real world example to see how this annotation is used. The `Owner.pdl` struct is referenced by the `Ownership.pdl` aspect. `Owned.pdl` contains a relationship to a CorpUser or CorpGroup:

```
namespace com.linkedin.common

/**
 * Ownership information
 */
record Owner {

  /**
   * Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name
   */
  @Relationship = {
    "name": "OwnedBy",
    "entityTypes": [ "corpUser", "corpGroup" ]
  }
  owner: Urn

  ...
}
```

This annotation says that when we ingest an Entity with an OwnershipAspect, Datahub will create an OwnedBy relationship between that entity and the CorpUser or CorpGroup who owns it.

#### Annotation Overrides

You will not always be able to apply annotations to a primitive field directly. This may be because the field is wrapped in an Array, or because the field is part of a shared struct that many entities reference. In these cases, you need to use annotation overrides. An override is done by specifying a fieldPath to the target field inside the annotation, like so:

```
 /**
   * Charts in a dashboard
   */
  @Relationship = {
    "/*": {
      "name": "Contains",
      "entityTypes": [ "chart" ]
    }
  }
  charts: array[ChartUrn] = [ ]
```

This override applies the relationship annotation to each element in the Array, rather than the array itself. This allows a unique Relationship to be created for between the Dashboard and each of its charts.

Another example can be seen in the case of tags. In this case, TagAssociation.pdl has a @Searchable annotation:

```
 @Searchable = {
    "fieldName": "tags",
    "fieldType": "URN_WITH_PARTIAL_MATCHING",
    "queryByDefault": true,
    "hasValuesFieldName": "hasTags"
  }
  tag: TagUrn
```

At the same time, SchemaField overrides that annotation to allow for searching for tags applied to schema fields specifically. To do this, it overrides the Searchable annotation applied to the `tag` field of `TagAssociation` and replaces it with its own- this has a different boostScore and a different fieldName.

```
 /**
   * Tags associated with the field
   */
  @Searchable = {
    "/tags/*/tag": {
      "fieldName": "fieldTags",
      "fieldType": "URN_WITH_PARTIAL_MATCHING",
      "queryByDefault": true,
      "boostScore": 0.5
    }
  }
  globalTags: optional GlobalTags
```

As a result, you can issue a query specifically for tags on Schema Fields via `fieldTags:<tag_name>` or tags directly applied to an entity via `tags:<tag_name>`. Since both have `queryByDefault` set to true, you can also search for entities with either of these properties just by searching for the tag name.

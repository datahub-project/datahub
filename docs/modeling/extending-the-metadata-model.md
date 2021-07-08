---
slug: /metadata-modeling/extending-the-metadata-model
---

# Extending the Metadata Model

You can extend the metadata model by either creating a new Entity or extending an existing one. Unsure if you need to
create a new entity or add an aspect to an existing entity? Read [metadata-model](./metadata-model.md) to understand
these two concepts prior to making changes.

We will outline what the experience of adding a new Entity should look like through a real example of adding the
Dashboard Entity. If you want to extend an existing Entity, you can skip directly to Step 4.

At a high level, an entity is made up of

1. a union of Aspects, or bundles of related metadata,
2. a Key Aspect, which uniquely identifies an instance of an entity,
3. A snapshot, which pairs a group of aspects with a serialized key, or urn.

## Defining an Entity

Now we'll walk through the steps required to create, ingest, and view your extensions to the metadata model. 
We will use the existing "Dashboard" entity for purposes of illustration.

### Step 1: Define the Entity Key Aspect

A key represents the fields that uniquely identify the entity. For those familiar with DataHub’s legacy architecture,
these fields were previously part of the Urn Java Class that was defined for each entity.

This struct will be used to generate a serialized string key, represented by an Urn. Each field in the key struct will
be converted into a single part of the Urn's tuple, in the order they are defined.

Let’s define a Key aspect for our new Dashboard entity.

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
    ...
  }
  dashboardTool: string

  /**
  * Unique id for the dashboard. This id should be globally unique for a dashboarding tool even when there are multiple deployments of it. As an example, dashboard URL could be used here for Looker such as 'looker.linkedin.com/dashboards/1234'
  */
  dashboardId: string
}

```

The Urn representation of the Key shown above would be:

```
urn:li:dashboard:(<tool>,<id>)
```

Because they are aspects, keys need to be annotated with an @Aspect annotation, This instructs DataHub that this struct can be a part of.

The key can also be annotated with the two index annotations: @Searchable and @Relationship. This instructs DataHub infra to use
the fields in the key to create relationships and index fields for search. See Step 4 for more details on the annotation
model.

**Constraints**: Note that each field in a Key Aspect MUST be of String or Enum type.


### Step 2: Define custom aspects

Some aspects, like Ownership and GlobalTags, are reusable across entities. They can be included in an entity’s set of aspects
freely. To include attributes that are not included in an existing Aspect, a new Aspect must be created.

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

The Aspect has four key components: its properties, the @Aspect annotation, the @Searchable annotation and the
@Relationship annotation. Let’s break down each of these.

- The Aspect’s properties. The record’s properties can be declared as a field on the record, or by including another
  record in the Aspect’s definition (`record DashboardInfo includes CustomProperties, ExternalReference {`). Properties
  can be defined as PDL primitives, enums, records, or collections (
  see [pdl schema documentation](https://linkedin.github.io/rest.li/pdl_schema))
  references to other entities, of type Urn or optionally `<Entity>Urn`
- The @Aspect annotation. This is used to declare that the record is an Aspect and can be included in an entity’s
  Snapshot. It also declares a common name for the aspect. Unlike the other two
  annotations, @Aspect is applied to the entire record rather than a specific field.
- The @Searchable annotation. This annotation can be applied to any primitive field to indicate that it should be
  indexed in Elasticsearch and can be searched on. For a complete guide on using the search annotation, see the
  annotation docs further down in this document.
- The @Relationship annotations. These annotations create edges between the Snapshot’s Urn and the destination of the
  annotated field when the snapshots are ingested. @Relationship annotations must be applied to fields of type Urn. In
  the case of DashboardInfo, the `charts` field is an Array of Urns. The @Relationship annotation cannot be applied
  directly to an array of Urns. That’s why you see the use of an Annotation override (`”/*”:) to apply the @Relationship
  annotation to the Urn directly. Read more about overrides in the annotation docs further down on this page.

After you create your Aspect, you need to add it into the Aspect Union of each entity you’d like to attach the aspect
to. Refer back to Step 2 for how Aspects are added to Aspect Unions.

**Constraints**: Note that all aspects MUST be of type Record. 

### Step 3: Define the Entity Aspect Union

You must create an Aspect union to define what aspects an Entity is associated with. An aspect represents a related record of metadata about an entity.
Any record appearing in the Union should be annotated with @Aspect. 

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

The first aspect will be by convention the Entity’s key aspect. Other aspects can be Dashboard specific, like DashboardInfo, or
common, such as Ownership. This union can be extended over time as you expand the metadata model. You can include any
existing type with the @Aspect annotation in your entity’s aspect union or create new ones- Step 4 goes into detail
about how to create a new Aspect.

To extend an existing entity, simply add your new Aspect to the Entity's list of aspect via the Aspect Union model. 

### Step 4: Define an Entity Snapshot

The snapshot describes the format of how an entity is serialized for read and write operations to GMS, the generic
metadata store. All snapshots have two fields, `urn` of type `Urn` and `snapshot`
of type `union[Aspect1, Aspect2, ...]`.

The snapshot needs an `@Entity` annotation with the entity’s name. The name is used for specifying entity type when
searching, using autocomplete, etc.

```
namespace com.linkedin.metadata.snapshot

import com.linkedin.common.DashboardUrn
import com.linkedin.metadata.aspect.DashboardAspect

/**
 * A metadata snapshot for a specific Dashboard entity.
 */
@Entity = {
  "name": "dashboard"
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

If you're extending an existing Entity, you can skip this step. 

### Step 5: Re-build DataHub to have access to your new or updated entity

If you have updated any existing types or see an `Incompatible changes` warning when building,
you will need to run
`./gradlew :gms:impl:build -Prest.model.compatibility=ignore`
before running `build`.

Then, run `./gradlew build` from the repository root to rebuild Datahub with access to your new entity.

Then, re-deploy gms, mae-consumer and mce-consumer (see [docker development](../../docker/README.md) for details on how
to deploy during development). This will allow Datahub to read and write Snapshots of your new entity, along with server
search and graph queries for that entity type.

To emit snapshots to ingest from the Datahub CLI tool, first install datahub cli
locally [following the instructions here](../../metadata-ingestion/developing.md). `./gradlew build` generated the avro
schemas your local ingestion cli tool uses earlier. After following the developing guide, you should be able to emit
your new event using the local datahub cli.

Now you are ready to start ingesting metadata for your new entity!

### (Optional) Step 6: Extend the DataHub frontend to view your entity in GraphQL & React

At the moment, custom React and Grapqhl code needs to be written to view your entity in GraphQL or React. For
instructions on how to start extending the GraphQL graph, see [graphql docs](../../datahub-graphql-core/README.md). Once
you’ve done that, you can follow the guide [here](../../datahub-web-react/README.md) to add your entity into the React
UI.

## Metadata Annotations

There are four core annotations that DataHub recognizes. 

#### @Entity

This annotation is applied to each Entity Snapshot record, such as DashboardSnapshot.pdl. Each one that is included in the root Snapshot.pdl model must have this annotation.

It takes the following parameters:

- **name**: string - A common name used to identify the entity. Must be unique among all entities DataHub is aware of.

##### Example

```aidl
@Entity = {
// name used when referring to the entity in APIs.
String name;
}
```

#### @Aspect

This annotation is applied to each Aspect record, such as DashboardInfo.pdl. Each aspect that is included in an entity’s
AspectUnion must have this annotation.

It takes the following parameters:

- **name**: string - A common name used to identify the Aspect. Must be unique among all aspects DataHub is aware of.

##### Example

```aidl
@Aspect = {
  // name used when referring to the aspect in APIs.
  String name;
}
```

#### @Searchable

This annotation is applied to fields inside an Aspect. It instructs DataHub to index the field so it can be retrieved via the search APIs.


It takes the following parameters:

- **fieldType**: string - The settings for how each field is indexed is defined by the field type. Each field type is associated with a set of
analyzers Elasticsearch will use to tokenize the field. Such sets are defined in the MappingsBuider, which generates the
mappings for the index for each entity given the fields with the search annotations. To customize the set of analyzers
used to index a certain field, you must add a new field type and define the set of mappings to be applied in the
MappingsBuilder.

  Thus far, we have implemented 8 fieldTypes:
  
  1. *KEYWORD* - Short text fields that only support exact matches, often used only for filtering
  
  2. *TEXT* - Text fields delimited by spaces/slashes/periods. Default field type for string variables.
  
  3. *TEXT_PARTIAL* - Text fields delimited by spaces/slashes/periods with partial matching support. Note, partial matching is expensive, so
  this field type should not be applied to fields with long values (like description)
  
  4. *BROWSE_PATH* - Field type for browse paths. Applies specific mappings for slash delimited paths.
  
  5. *URN* - Urn fields where each sub-component inside the urn is indexed. For instance, for a data platform urn like
  "urn:li:dataplatform:kafka", it will index the platform name "kafka" and ignore the common components
  
  6. *URN_PARTIAL* - Urn fields where each sub-component inside the urn is indexed with partial matching support.
  
  7. *BOOLEAN* - Boolean fields used for filtering.
  
  8. *COUNT* - Count fields used for filtering.
  
- **fieldName**: string (optional) - The name of the field in search index document. Defaults to the field name where the annotation resides. 
- **queryByDefault**: boolean (optional) - Whether we should match the field for the default search query. True by default for text and urn fields.
- **enableAutocomplete**: boolean (optional) -  Whether we should use the field for autocomplete. Defaults to false
- **addToFilters**: boolean (optional) - Whether or not to add field to filters. Defaults to false
- **boostScore**: double (optional) - Boost multiplier to the match score. Matches on fields with higher boost score ranks higher.
- **hasValuesFieldName**: string (optional) - If set, add an index field of the given name that checks whether the field exists
- **numValuesFieldName**: string (optional) - If set, add an index field of the given name that checks the number of elements
- **weightsPerFieldValue**: map[object, double] (optional) - Weights to apply to score for a given value.


##### Example

Let’s take a look at a real world example using the `title` field of `DashboardInfo.pdl`:

```aidl
record DashboardInfo {
 /**
   * Title of the dashboard
   */
  @Searchable = {
    "fieldType": "TEXT_PARTIAL",
    "enableAutocomplete": true,
    "boostScore": 10.0
  }
  title: string
  ....
}
```

This annotation is saying that we want to index the title field in Elasticsearch. We want to support partial matches on
the title, so queries for `Cust` should return a Dashboard with the title `Customers`. `enableAutocomplete` is set to
true, meaning that we can autocomplete on this field when typing into the search bar. Finally, a boostScore of 10 is
provided, meaning that we should prioritize matches to title over matches to other fields, such as description, when
ranking.

Now, when Datahub ingests Dashboards, it will index the Dashboard’s title in Elasticsearch. When a user searches for
Dashboards, that query will be used to search on the title index and matching Dashboards will be returned.

#### @Relationship

This annotation is applied to fields inside an Aspect. This annotation creates edges between an Entity’s Urn and the
destination of the annotated field when the Entity is ingested. @Relationship annotations must be applied to fields of
type Urn.

It takes the following parameters:

- **name**: string - A name used to identify the Relationship type.
- **entityTypes**: array[string] (Optional) - A list of entity types that are valid values for the foreign-key relationship field. 

##### Example

Let’s take a look at a real world example to see how this annotation is used. The `Owner.pdl` struct is referenced by
the `Ownership.pdl` aspect. `Owned.pdl` contains a relationship to a CorpUser or CorpGroup:

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

This annotation says that when we ingest an Entity with an OwnershipAspect, DataHub will create an OwnedBy relationship
between that entity and the CorpUser or CorpGroup who owns it. This will be queryable using the Relationships resource in both the forward and inverse directions.

#### Annotating Collections & Annotation Overrides

You will not always be able to apply annotations to a primitive field directly. This may be because the field is wrapped
in an Array, or because the field is part of a shared struct that many entities reference. In these cases, you need to
use annotation overrides. An override is done by specifying a fieldPath to the target field inside the annotation, like
so:

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

This override applies the relationship annotation to each element in the Array, rather than the array itself. This
allows a unique Relationship to be created for between the Dashboard and each of its charts.

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

At the same time, SchemaField overrides that annotation to allow for searching for tags applied to schema fields
specifically. To do this, it overrides the Searchable annotation applied to the `tag` field of `TagAssociation` and
replaces it with its own- this has a different boostScore and a different fieldName.

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

As a result, you can issue a query specifically for tags on Schema Fields via `fieldTags:<tag_name>` or tags directly
applied to an entity via `tags:<tag_name>`. Since both have `queryByDefault` set to true, you can also search for
entities with either of these properties just by searching for the tag name.

---
slug: /metadata-modeling/extending-the-metadata-model
---

# Extending the Metadata Model

You can extend the metadata model by either creating a new Entity or extending an existing one. Unsure if you need to
create a new entity or add an aspect to an existing entity? Read [metadata-model](./metadata-model.md) to understand
these two concepts prior to making changes.

## To fork or not to fork?

An important question that will arise once you've decided to extend the metadata model is whether you need to fork the main repo or not. Use the diagram below to understand how to make this decision.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-model-to-fork-or-not-to.png"/>
</p>

The green lines represent pathways that will lead to lesser friction for you to maintain your code long term. The red lines represent higher risk of conflicts in the future. We are working hard to move the majority of model extension use-cases to no-code / low-code pathways to ensure that you can extend the core metadata model without having to maintain a custom fork of DataHub.

We will refer to the two options as the **open-source fork** and **custom repository** approaches in the rest of the document below.

## This Guide

This guide will outline what the experience of adding a new Entity should look like through a real example of adding the
Dashboard Entity. If you want to extend an existing Entity, you can skip directly to [Step 3](#step-3-define-custom-aspects-or-attach-existing-aspects-to-your-entity).

At a high level, an entity is made up of:

1. A Key Aspect: Uniquely identifies an instance of an entity,
2. A list of specified Aspects, groups of related attributes that are attached to an entity.

## Defining an Entity

Now we'll walk through the steps required to create, ingest, and view your extensions to the metadata model. We will use
the existing "Dashboard" entity for purposes of illustration.

### <a name="step_1"></a>Step 1: Define the Entity Key Aspect

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

Because they are aspects, keys need to be annotated with an @Aspect annotation, This instructs DataHub that this struct
can be a part of.

The key can also be annotated with the two index annotations: @Relationship and @Searchable. This instructs DataHub
infra to use the fields in the key to create relationships and index fields for search. See [Step 3](#step-3-define-custom-aspects-or-attach-existing-aspects-to-your-entity) for more details on
the annotation model.

**Constraints**: Note that each field in a Key Aspect MUST be of String or Enum type.

### <a name="step_2"></a>Step 2: Create the new entity with its key aspect

Define the entity within an `entity-registry.yml` file. Depending on your approach, the location of this file may vary. More on that in steps [4](#step-4-choose-a-place-to-store-your-model-extension) and [5](#step-5-attaching-your-non-key-aspects-to-the-entity).

Example:

```yaml
- name: dashboard
  doc: A container of related data assets.
  keyAspect: dashboardKey
```

- name: The entity name/type, this will be present as a part of the Urn.
- doc: A brief description of the entity.
- keyAspect: The name of the Key Aspect defined in step 1. This name must match the value in the PDL annotation.

#

### <a name="step_3"></a>Step 3: Define custom aspects or attach existing aspects to your entity

Some aspects, like Ownership and GlobalTags, are reusable across entities. They can be included in an entity’s set of
aspects freely. To include attributes that are not included in an existing Aspect, a new Aspect must be created.

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
@Relationship annotation. Let’s break down each of these:

- **Aspect properties**: The record’s properties can be declared as a field on the record, or by including another
  record in the Aspect’s definition (`record DashboardInfo includes CustomProperties, ExternalReference {`). Properties
  can be defined as PDL primitives, enums, records, or collections (
  see [pdl schema documentation](https://linkedin.github.io/rest.li/pdl_schema))
  references to other entities, of type Urn or optionally `<Entity>Urn`
- **@Aspect annotation**: Declares record is an Aspect and includes it when serializing an entity. Unlike the following
  two annotations, @Aspect is applied to the entire record, rather than a specific field. Note, you can mark an aspect
  as a timeseries aspect. Check out this [doc](metadata-model.md#timeseries-aspects) for details.
- **@Searchable annotation**: This annotation can be applied to any primitive field or a map field to indicate that it
  should be indexed in Elasticsearch and can be searched on. For a complete guide on using the search annotation, see
  the annotation docs further down in this document.
- **@Relationship annotation**: These annotations create edges between the Entity’s Urn and the destination of the
  annotated field when the entities are ingested. @Relationship annotations must be applied to fields of type Urn. In
  the case of DashboardInfo, the `charts` field is an Array of Urns. The @Relationship annotation cannot be applied
  directly to an array of Urns. That’s why you see the use of an Annotation override (`"/*":`) to apply the @Relationship
  annotation to the Urn directly. Read more about overrides in the annotation docs further down on this page.
- **@UrnValidation**: This annotation can enforce constraints on Urn fields, including entity type restrictions and existence.

After you create your Aspect, you need to attach to all the entities that it applies to.

**Constraints**: Note that all aspects MUST be of type Record.

### <a name="step_4"></a>Step 4: Choose a place to store your model extension

At the beginning of this document, we walked you through a flow-chart that should help you decide whether you need to maintain a fork of the open source DataHub repo for your model extensions, or whether you can just use a model extension repository that can stay independent of the DataHub repo. Depending on what path you took, the place you store your aspect model files (the .pdl files) and the entity-registry files (the yaml file called `entity-registry.yaml` or `entity-registry.yml`) will vary.

- Open source Fork: Aspect files go under [`metadata-models`](../../metadata-models) module in the main repo, entity registry goes into [`metadata-models/src/main/resources/entity-registry.yml`](../../metadata-models/src/main/resources/entity-registry.yml). Read on for more details in [Step 5](#step-5-attaching-your-non-key-aspects-to-the-entity).
- Custom repository: Read the [metadata-models-custom](../../metadata-models-custom/README.md) documentation to learn how to store and version your aspect models and registry.

### <a name="step_5"></a>Step 5: Attaching your non-key Aspect(s) to the Entity

Attaching non-key aspects to an entity can be done simply by adding them to the entity registry yaml file. The location of this file differs based on whether you are following the oss-fork path or the custom-repository path.

Here is an minimal example of adding our new `DashboardInfo` aspect to the `Dashboard` entity.

```yaml
entities:
   - name: dashboard
   - keyAspect: dashBoardKey
   aspects:
     # the name of the aspect must be the same as that on the @Aspect annotation on the class
     - dashboardInfo
```

Previously, you were required to add all aspects for the entity into an Aspect union. You will see examples of this pattern throughout the code-base (e.g. `DatasetAspect`, `DashboardAspect` etc.). This is no longer required.

### <a name="step_6"></a>Step 6 (Oss-Fork approach): Re-build DataHub to have access to your new or updated entity

If you opted for the open-source fork approach, where you are editing models in the `metadata-models` repository of DataHub, you will need to re-build the DataHub metadata service using the steps below. If you are following the custom model repository approach, you just need to build your custom model repository and deploy it to a running metadata service instance to read and write metadata using your new model extensions.

Read on to understand how to re-build DataHub for the oss-fork option.

**_NOTE_**: If you have updated any existing types or see an `Incompatible changes` warning when building, you will need to run
`./gradlew :metadata-service:restli-servlet-impl:build -Prest.model.compatibility=ignore`
before running `build`.

Then, run `./gradlew build` from the repository root to rebuild DataHub with access to your new entity.

Then, re-deploy metadata-service (gms), and mae-consumer and mce-consumer (optionally if you are running them unbundled). See [docker development](../../docker/README.md) for details on how
to deploy during development. This will allow DataHub to read and write your new entity or extensions to existing entities, along with serving search and graph queries for that entity type.

### <a name="step_7"></a>(Optional) Step 7: Use custom models with the Python SDK

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs queryString="python-custom-models">
<TabItem value="local" label="Local CLI" default>

If you're purely using the custom models locally, you can use a local development-mode install of the DataHub CLI.

Install the DataHub CLI locally by following the [developer instructions](../../metadata-ingestion/developing.md).
The `./gradlew build` command already generated the avro schemas for your local ingestion cli tool to use.
After following the developing guide, you should be able to emit your new event using the local DataHub CLI.

</TabItem>
<TabItem value="packaged" label="Custom Models Package">

If you want to use your custom models beyond your local machine without forking DataHub, then you can generate a custom model package that can be installed from other places.

This package should be installed alongside the base `acryl-datahub` package, and its metadata models will take precedence over the default ones.

```bash
$ cd metadata-ingestion
$ ../gradlew customPackageGenerate -Ppackage_name=my-company-datahub-models -Ppackage_version="0.0.1"
<bunch of log lines>
Successfully built my-company-datahub-models-0.0.1.tar.gz and acryl_datahub_cloud-0.0.1-py3-none-any.whl

Generated package at custom-package/my-company-datahub-models
This package should be installed alongside the main acryl-datahub package.

Install the custom package locally with `pip install custom-package/my-company-datahub-models`
To enable others to use it, share the file at custom-package/my-company-datahub-models/dist/<wheel file>.whl and have them install it with `pip install <wheel file>.whl`
Alternatively, publish it to PyPI with `twine upload custom-package/my-company-datahub-models/dist/*`
```

This will generate some Python build artifacts, which you can distribute within your team or publish to PyPI.
The command output contains additional details and exact CLI commands you can use.

Once this package is installed, you can use the DataHub CLI as normal, and it will use your custom models.
You'll also be able to import those models, with IDE support, by changing your imports.

```diff
- from datahub.metadata.schema_classes import DatasetPropertiesClass
+ from my_company_datahub_models.metadata.schema_classes import DatasetPropertiesClass
```

</TabItem>
</Tabs>

### <a name="step_8"></a>(Optional) Step 8: Extend the DataHub frontend to view your entity in GraphQL & React

If you are extending an entity with additional aspects, and you can use the auto-render specifications to automatically render these aspects to your satisfaction, you do not need to write any custom code.

However, if you want to write specific code to render your model extensions, or if you introduced a whole new entity and want to give it its own page, you will need to write custom React and Grapqhl code to view and mutate your entity in GraphQL or React. For
instructions on how to start extending the GraphQL graph, see [graphql docs](../../datahub-graphql-core/README.md). Once you’ve done that, you can follow the guide [here](../../datahub-web-react/README.md) to add your entity into the React UI.

## Metadata Annotations

There are four core annotations that DataHub recognizes:

#### @Entity

**Legacy**
This annotation is applied to each Entity Snapshot record, such as DashboardSnapshot.pdl. Each one that is included in
the root Snapshot.pdl model must have this annotation.

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
set of aspects in the `entity-registry.yml` must have this annotation.

It takes the following parameters:

- **name**: string - A common name used to identify the Aspect. Must be unique among all aspects DataHub is aware of.
- **type**: string (optional) - set to "timeseries" to mark this aspect as timeseries. Check out
  this [doc](metadata-model.md#timeseries-aspects) for details.
- **autoRender**: boolean (optional) - defaults to false. When set to true, the aspect will automatically be displayed
  on entity pages in a tab using a default renderer. **_This is currently only supported for Charts, Dashboards, DataFlows, DataJobs, Datasets, Domains, and GlossaryTerms_**.
- **renderSpec**: RenderSpec (optional) - config for autoRender aspects that controls how they are displayed. **_This is currently only supported for Charts, Dashboards, DataFlows, DataJobs, Datasets, Domains, and GlossaryTerms_**. Contains three fields:
  - **displayType**: One of `tabular`, `properties`. Tabular should be used for a list of data elements, properties for a single data bag.
  - **displayName**: How the aspect should be referred to in the UI. Determines the name of the tab on the entity page.
  - **key**: For `tabular` aspects only. Specifies the key in which the array to render may be found.

##### Example

```aidl
@Aspect = {
  // name used when referring to the aspect in APIs.
  String name;
}
```

#### @Searchable

This annotation is applied to fields inside an Aspect. It instructs DataHub to index the field so it can be retrieved
via the search APIs.

:::note
If you are adding @Searchable to a field that already has data, you'll want to restore indices [via api](https://docs.datahub.com/docs/api/restli/restore-indices/) or [via upgrade step](https://github.com/datahub-project/datahub/blob/master/metadata-service/factories/src/main/java/com/linkedin/metadata/boot/steps/RestoreGlossaryIndices.java) to have it be populated with existing data.
:::

It takes the following parameters:

- **fieldType**: string - The settings for how each field is indexed is defined by the field type. In general this defines how the field is indexed in the Elasticsearch document. The field type also decides the subfields a field is indexed with (for example `.delimited`, `.ngram` and `.keyword`), which the default full-text search, autocomplete and exact match queries read.

  **Available field types:**

  1. _KEYWORD_ - Short text fields that only support exact matches, often used only for filtering. The whole value is indexed as one term, so it has to stay under Lucene's 32,766-byte term limit.

  2. _TEXT_ - Text fields delimited by spaces/slashes/periods. Default field type for string variables. Exact matches (the keyword forms) skip values over 32,766 characters; the analyzed subfields still index them.

  3. _BOOLEAN_ - Boolean fields used for filtering.

  4. _COUNT_ - Count fields used for filtering.

  5. _DATETIME_ - Datetime fields used to represent timestamps.

  6. _OBJECT_ - Each property in an object will become an extra column in Elasticsearch and can be referenced as
     `field.property` in queries. **Default limits**: Maximum 1000 object keys, maximum 4096 characters per value. You should be careful to not use it on objects with many properties as it can cause a mapping explosion in Elasticsearch.

  7. _DOUBLE_ - Double precision numeric fields used for filtering and calculations.

  8. _MAP_ARRAY_ - Array fields that are stored as maps in Elasticsearch. **Default limits**: Maximum 1000 array elements, maximum 4096 characters per value.

  **⚠️ Deprecated field types (avoid using in new code):**

  10. ~~_TEXT_PARTIAL_~~ - **DEPRECATED**: Text fields with partial matching support. This field type is expensive and should not be applied to fields with long values. Use TEXT instead.

  11. ~~_WORD_GRAM_~~ - **DEPRECATED**: Text fields with word gram support. This field type is expensive and should not be applied to fields with long values. Use TEXT instead.

  12. ~~_BROWSE_PATH_~~ - **DEPRECATED**: Field type for browse paths. Browse paths are handled by name, use `browsePathV2` field name. There can only be one for a given entity.

  13. ~~_URN_~~ - **DEPRECATED**: Urn fields where each sub-component is indexed. Use KEYWORD instead.

  14. ~~_URN_PARTIAL_~~ - **DEPRECATED**: Urn fields with partial matching support. Use KEYWORD instead.

**⚠️ Important Length Limitations:**

- **Regular Fields**: Keyword forms skip values over **32,766 characters** (`ignore_above: 32766`). Lucene's term limit is 32,766 UTF-8 bytes, so a shorter value with many multi-byte characters still fails the document write. On Search V3, the copies under `_aspects` skip strings over 8,191 characters and URNs over 255
- **Object Fields**: Maximum **1000 object keys** and **4096 characters per value** to prevent mapping explosion
- **Array Fields**: Maximum **1000 array elements** and **4096 characters per value**
- **Field Names**: Maximum **255 characters** for Elasticsearch field name compatibility

**Configuration Overrides:**

- **Environment Variables**: Some limits can be configured via environment variables:
  - `SEARCH_DOCUMENT_MAX_VALUE_LENGTH`: Override default 4096 character limit for object/array values
  - `SEARCH_DOCUMENT_MAX_ARRAY_LENGTH`: Override default 1000 element limit for arrays
  - `SEARCH_DOCUMENT_MAX_OBJECT_KEYS`: Override default 1000 key limit for objects
- **Special Fields**: Some system fields have different limits:
  - **The `urn` field**: Automatically set to **512 characters** (`ignore_above: 512`)

**Note**: The `ignore_above` settings are automatically applied by the system. While some limits can be configured via environment variables, the regular field limit is hard-coded and cannot be overridden through annotations or configuration.

**Important**: The ability to have longer keyword fields is limited to system-level configurations and special field types. Regular user-defined fields will always be subject to the default limits for performance and compatibility reasons.

- **fieldName**: string (optional) - The name of the field in search index document. Defaults to the field name where
  the annotation resides.

- **queryByDefault**: boolean (optional) - Whether we should match the field for the default search query. True by
  default for text and urn fields. On Search V3, a field queried by default that names no shared field copies into
  `_search.other` (see [Shared search fields on Search V3](#shared-search-fields-on-search-v3)).

- **enableAutocomplete**: boolean (optional) - Whether we should use the field for autocomplete. Defaults to false.
  On Search V3, these fields copy into `_search.autocomplete`, the one field autocomplete reads.

- **addToFilters**: boolean (optional) - Whether or not to add field to filters. Defaults to false

- **addHasValuesToFilters**: boolean (optional) - Whether or not to add the "has values" to filters. Defaults to true

- **filterNameOverride**: string (optional) - Display name for the filter in the UI

- **hasValuesFilterNameOverride**: string (optional) - Display name for the "has values" filter in the UI

- **boostScore**: double (optional) - **⚠️ DEPRECATED**: Boost multiplier to the match score. Matches on fields with higher boost score
  ranks higher.

- **hasValuesFieldName**: string (optional) - If set, add an index field of the given name that checks whether the field
  exists

- **numValuesFieldName**: string (optional) - If set, add an index field of the given name that checks the number of
  elements

- **weightsPerFieldValue**: map[object, double] (optional) - **⚠️ DEPRECATED**: Weights to apply to score for a given value. **Use `searchLabel` with `@SearchScore` annotations instead for value-based scoring.**

- **fieldNameAliases**: array[string] (optional) - Aliases for this field that can be used for sorting and other operations. These aliases are created with the aspect name prefix (e.g., `metadata.aliasName`) and provide alternative names for accessing the same field data. Useful for creating multiple access paths to the same field.

- **includeSystemModifiedAt**: boolean (optional) - **⚠️ DEPRECATED**: Whether to include a system-modified timestamp field for this searchable field. **This will be handled programmatically for all aspects in future versions.**

- **systemModifiedAtFieldName**: string (optional) - **⚠️ DEPRECATED**: Custom name for the system-modified timestamp field. **This will be handled programmatically for all aspects in future versions.**

- **includeQueryEmptyAggregation**: boolean (optional) - Whether to create a missing field aggregation when querying the corresponding field. Only affects query time, not mapping. Useful for analytics and reporting.

- **searchTier**: integer (optional) - **⚠️ DEPRECATED, no-op**: Still accepted and validated (an integer >= 1 on `KEYWORD`, `TEXT`, `TEXT_PARTIAL`, `WORD_GRAM` or `URN` fields) so existing models keep loading, but it no longer changes the index mapping or the search queries, and no `_search.tier_{tier}` field is created. Use `queryByDefault` and `enableAutocomplete` to control full-text search and autocomplete.

- **searchLabel**: string (optional) - Unified label for search operations. Copies the field value into `_search.{label}` (without prefixes). Replaces the previous `sortLabel` and `boostLabel` annotations. The field stays indexed under its own name too. For string fields on Search V3, the label also names the shared full-text field the value lands in (see [Shared search fields on Search V3](#shared-search-fields-on-search-v3)).

- **searchIndexed**: boolean (optional) - **⚠️ DEPRECATED, no-op**: Still accepted and validated (it can only be true together with `searchTier`, on `KEYWORD` or `TEXT` fields), but every searchable field is indexed under its own name regardless.

- **entityFieldName**: string (optional) - If set, this field is copied into `_search.{entityFieldName}`, so several aspects can fill one entity-level field. `_entityName` aliases `_search.entityName` when a field of the entity sets `entityName`.

- **eagerGlobalOrdinals**: boolean (optional) - Whether to set `eager_global_ordinals` to true for this field. This improves aggregation performance for frequently aggregated keyword fields by pre-building ordinals at index time. **Note**: eagerGlobalOrdinals can only be true for KEYWORD, URN, or URN_PARTIAL field types. Defaults to false.

**⚠️ Note on deprecated parameters:** On Search V3, `boostScore` only weighs a match of a field's whole value, such as an exact name: full-text search matches words in the shared `_search` fields, and each shared field has one weight, shared by all the fields that feed it. `weightsPerFieldValue` applies on Search V2 and V3. Both will be replaced by newer features in future versions. `searchLabel` is not a replacement for either: it does not weight matches.

##### Example

Let’s take a look at a real world example using the `title` field of `DashboardInfo.pdl`:

```aidl
record DashboardInfo {
 /**
   * Title of the dashboard
   */
  @Searchable = {
    "fieldType": "KEYWORD",
    "entityFieldName": "name"
  }
  title: string
  ....
}
```

This annotation is saying that we want to index the title field in Elasticsearch. `entityFieldName: "name"` consolidates this field into the entity-level `_search.name` field, allowing other aspects to contribute to the same consolidated field.

**Advanced Example with New Features:**

```aidl
record DashboardInfo {
  /**
   * Priority level for the dashboard
   */
  @Searchable = {
    "fieldType": "COUNT",
    "searchLabel": "priority",
    "addToFilters": true
  }
  priority: int

  /**
   * Status of the dashboard
   */
  @Searchable = {
    "fieldType": "KEYWORD",
    "addToFilters": true,
    "filterNameOverride": "Dashboard Status",
    "eagerGlobalOrdinals": true
  }
  status: string

  /**
   * Owner URN for the dashboard
   */
  @Searchable = {
    "fieldType": "URN",
    "addToFilters": true,
    "eagerGlobalOrdinals": true,
    "searchLabel": "owner"
  }
  owner: string
}
```

This example demonstrates several new features:

- **Priority field**: `fieldType: "COUNT"` with `searchLabel: "priority"` creates a numeric field that copies to `_search.priority` for proper numeric sorting operations, and `addToFilters: true` makes it available as a filter
- **Status field**: `addToFilters: true` makes it available as a filter, `filterNameOverride` provides a custom display name "Dashboard Status", and `eagerGlobalOrdinals: true` optimizes aggregation performance for this frequently filtered field
- **Owner field**: `fieldType: "URN"` with `eagerGlobalOrdinals: true` optimizes aggregation performance for owner-based filtering, and `searchLabel: "owner"` copies the field to `_search.owner` for ranking operations

Now, when DataHub ingests Dashboards, it will index the priority and status fields in Elasticsearch. The priority field will be available for sorting operations, and both fields will be available as filters in the UI.

Note, when @Searchable annotation is applied to a map, it will convert it into a list with "key.toString()
=value.toString()" as elements. This allows us to index map fields, while not increasing the number of columns indexed.
This way, the keys can be queried by `aMapField:key1=value1`.

You can change this behavior by specifying the fieldType as OBJECT in the @Searchable annotation. It will put each key
into a column in Elasticsearch instead of an array of serialized kay-value pairs. This way the query would look more
like `aMapField.key1:value1`. As this method will increase the number of columns with each unique key - large maps can
cause a mapping explosion in Elasticsearch. You should _not_ use the object fieldType if you expect your maps to get
large.

#### @SearchScore ⚠️ DEPRECATED

**⚠️ DEPRECATED**: This annotation is deprecated and should not be used in new code. Use `searchLabel` instead for ranking functionality.

#### Search Label System

The search label system provides a powerful way to organize search fields and create specialized search experiences:

**Search Tiers (`searchTier`) ⚠️ DEPRECATED:**

Earlier versions copied fields with `searchTier` into `_search.tier_{tier}` fields that served Search V3 full-text search. Search V3 now reads the shared `_search` fields described below, so `searchTier` and `searchIndexed` are accepted but ignored, and no `_search.tier_{tier}` field is created.

**Search Labels (`searchLabel`):**

- Fields with `searchLabel` are copied to `_search.{label}` fields (without prefixes)
- Replaces the previous `sortLabel` and `boostLabel` annotations for a unified approach
- Useful for creating specialized search, sorting, and ranking operations across multiple aspects

**Entity Field Consolidation (`entityFieldName`):**

- Allows multiple aspects to consolidate into a single entity-level field
- Useful for creating unified search experiences across different aspect types
- Fields are copied to `_search.{entityFieldName}`

<a name="shared-search-fields-on-search-v3"></a>
**Shared search fields on Search V3:**

Search V3 full-text search and autocomplete read a few shared `_search` fields instead of every searchable field.
Only these fields are analyzed; the fields under their own names (and under `_aspects`) are keywords, numbers, dates
and booleans for filters, facets and sorts. Each searchable string field copies into:

- the field its `searchLabel` or `entityFieldName` names, for example `_search.entityName` or `_search.qualifiedName`;
- otherwise, for the fields that name no label, the shared field DataHub declares for its search field name:
  `_search.description` for `description`, `editedDescription`, `definition` and `assertionDescription`, and
  `_search.columns` for the schema field paths, descriptions, labels, tags and terms (`fieldPaths`,
  `fieldDescriptions`, `editedFieldDescriptions`, `fieldLabels`, `fieldTags`, `editedFieldTags`,
  `fieldGlossaryTerms`, `editedFieldGlossaryTerms`);
- otherwise, when the field is queried by default, `_search.other`, so V3 still searches every field V2 searches. An
  entity none of whose fields names `entityName` sends the field its `_entityName` alias names to
  `_search.entityName` instead. The urn and the fields queried by default of an entity a reference field names
  (`@SearchableRef`) only ever copy into `_search.other`;
- and, with `enableAutocomplete`, also `_search.autocomplete`.

Structured property values of type `string`, `rich_text` and `urn` copy into `_search.structuredProperties`, which
every V3 entity index maps, unless the property's definition sets `excludeFromFullTextSearch` in its
`searchConfiguration`.

A shared field fed by several fields holds all of their values, so an ingested and an edited description are both
searchable. An edited display name (`editedName`) lands in `_search.other`, so the name entities sort by stays the
ingested one, as on V2. A shared field that a field queried by default feeds is analyzed (`text` and `stemmed`, with
an identifier such as `customer_id` indexed whole and by its parts, short parts such as `id` dropped like any short word), and `_search.autocomplete` has a
search-as-you-type `ngram` subfield; the other
shared fields stay keywords, dates or numbers. A label a model names keeps its indexed keyword for sorts and filters
even when it is analyzed, except `description`, `columns`, `structuredProperties` and `other`, which hold only their analyzed subfields. Matches in `_search.entityName` and
`_search.qualifiedName` weigh 10, in `_search.structuredProperties` 0.8, in `_search.other` 0.5, and in every other
shared field 1. A search field
configuration (`fieldConfigurations` in the search configuration) names a shared field either directly or by any field
that feeds it, except `_search.other`, which only its own name selects. Matched fields ("Matched on") come from the urn,
the fields that feed each searched shared field, except `_search.columns`, and of `_search.other` only the fields that
name or tag an entity (`name`, `editedName`, `displayName`, `fullName`, `title`, `tags` and `glossaryTerms`): a match
in a column or in another field still counts, but is not reported, since its values can be too large to fetch with
every hit.

**Benefits of the New System:**

1. **Organized Search Fields**: All search-related fields are grouped under `_search.*`
2. **Flexible Querying**: Search queries can target specific sort or ranking fields
3. **Performance**: Optimized storage and query patterns for complex search scenarios

#### Migration Guide for Deprecated Features

If you're currently using deprecated field types or parameters, here's how to migrate to the new system:

**Field Type Migrations:**

| Deprecated     | Recommended Replacement | Notes                                                        |
| -------------- | ----------------------- | ------------------------------------------------------------ |
| `TEXT_PARTIAL` | `TEXT`                  | Use TEXT with appropriate analyzers for partial matching     |
| `WORD_GRAM`    | `TEXT`                  | Use TEXT with word delimited analyzers for word-based search |
| `BROWSE_PATH`  | `BROWSE_PATH_V2`        | Use BROWSE_PATH_V2 for improved path hierarchy support       |
| `URN`          | `TEXT`                  | Use TEXT with URN analyzers for component-based search       |
| `URN_PARTIAL`  | `TEXT`                  | Use TEXT with URN analyzers and partial matching             |

**Parameter Migrations:**

| Deprecated Pattern                        | New Pattern   | Benefits                                                                  |
| ----------------------------------------- | ------------- | ------------------------------------------------------------------------- |
| `includeSystemModifiedAt: true`           | **Automatic** | System modification tracking is now handled automatically for all aspects |
| `systemModifiedAtFieldName: "customName"` | **Automatic** | System modification field names are now standardized automatically        |

**Example Migration:**

```aidl
// Old deprecated approach
@Searchable = {
  "fieldType": "TEXT_PARTIAL",
  "queryByDefault": true,
  "enableAutocomplete": true,
  "boostScore": 10.0
}
title: string

// New recommended approach
@Searchable = {
  "fieldType": "TEXT",
  "enableAutocomplete": true,
  "entityFieldName": "name"
}
title: string
```

**Benefits of Migration:**

- Better search performance through optimized indexing
- More organized search field structure
- Future-proof annotations that won't be deprecated
- Improved Elasticsearch mapping efficiency

#### @Relationship

This annotation is applied to fields inside an Aspect. This annotation creates edges between an Entity's Urn and the
destination of the annotated field when the Entity is ingested. @Relationship annotations must be applied to fields of
type Urn.

It takes the following parameters:

- **name**: string - A name used to identify the Relationship type.
- **entityTypes**: array[string] (Optional) - A list of entity types that are valid values for the foreign-key
  relationship field.

##### Example

Let's take a look at a real world example to see how this annotation is used. The `Owner.pdl` struct is referenced by
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

This annotation says that when we ingest an Entity with an Ownership Aspect, DataHub will create an OwnedBy relationship
between that entity and the CorpUser or CorpGroup who owns it. This will be queryable using the Relationships resource
in both the forward and inverse directions.

#### @UrnValidation

This annotation can be applied to Urn fields inside an aspect. The annotation can optionally perform one or more of the following:

- Enforce that the URN exists
- Enforce stricter URN validation
- Restrict the URN to specific entity types

##### Example

Using this example from StructuredPropertyDefinition, we are enforcing that the valueType URN must exist,
it must follow stricter Urn encoding logic, and it can only be of entity type `dataType`.

```
    @UrnValidation = {
      "exist": true,
      "strict": true,
      "entityTypes": [ "dataType" ],
    }
    valueType: Urn
```

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

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
- **aspectJson**: boolean (optional) - defaults to false. Search V3 only. When true, the entity document stores this aspect's JSON, the same JSON stored in SQL, at `_aspects.<aspectName>._aspectJson` and does not index it. Enabling it on an aspect that already has data requires a Search V3 reindex, and the stored JSON increases the size of `_source`.
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
  the root field `other` (see [Shared search fields on Search V3](#shared-search-fields-on-search-v3)). Search V3
  leaves the field out of an unqualified query until `"v3": { "queryByDefault": true }` opts it in.

- **enableAutocomplete**: boolean (optional) - Whether we should use the field for autocomplete. Defaults to false.
  Search V2 reads the field for typeahead. Search V3 autocomplete prefixes `name.text`, or the caller's resolved field, and does not read this flag.

- **addToFilters**: boolean (optional) - Whether or not to add field to filters. Defaults to false

- **addHasValuesToFilters**: boolean (optional) - Whether or not to add the "has values" to filters. Defaults to true

- **filterNameOverride**: string (optional) - Display name for the filter in the UI

- **hasValuesFilterNameOverride**: string (optional) - Display name for the "has values" filter in the UI

- **boostScore**: double (optional) - **⚠️ DEPRECATED**: Boost multiplier to the match score. Matches on fields with higher boost score
  ranks higher.

- **hasValuesFieldName**: string (optional) - If set, add an index field of the given name that checks whether the field
  exists. Search V2 and V2.5 index that boolean for every field that declares it, storing both `true` and `false`.
  Search V3 indexes it only when the field has a `searchLabel`. Every source of that label writes the same boolean.
  The boolean is stored under the aspect only when the source is populated, and `copy_to` collects those
  trues onto the shared name. It is not stored as `false`: a false filter matches documents where
  the field is missing. A Search V3 field with no `searchLabel` has no boolean. Presence is an
  `EXISTS` or `IS_NULL` query on the field itself. A dedicated URN root stores that value on
  `<field>_urn`. A filter or sort that still names the old boolean is rewritten onto that field when
  it is a presence check: a true or `EXISTS` filter becomes `exists`, a false or `IS_NULL` filter
  matches documents where the field is missing, and a sort reads the field. A contains, range, or
  other condition is not a boolean check and matches nothing.

- **numValuesFieldName**: string (optional) - If set, add an index field of the given name that checks the number of
  elements

- **weightsPerFieldValue**: map[object, double] (optional) - **⚠️ DEPRECATED**: Weights to apply to score for a given value. **Use `searchLabel` with `@SearchScore` annotations instead for value-based scoring.**

- **fieldNameAliases**: array[string] (optional) - Aliases for this field that can be used for sorting and other operations. These aliases are created with the aspect name prefix (e.g., `metadata.aliasName`) and provide alternative names for accessing the same field data. Useful for creating multiple access paths to the same field.

- **includeSystemModifiedAt**: boolean (optional) - **⚠️ DEPRECATED**: Whether to include a system-modified timestamp field for this searchable field. Search V2 and V2.5 still index that sidecar. Search V3 records the same write time on every aspect as `systemMetadata.aspectModified` and does not search the sidecar. Set `"v3": { "includeSystemModifiedAt": false }` so V3 leaves it out while V2 keeps the top-level keys.

- **systemModifiedAtFieldName**: string (optional) - **⚠️ DEPRECATED**: Custom name for the system-modified timestamp field on Search V2 and V2.5. Search V3 does not map it.

- **includeQueryEmptyAggregation**: boolean (optional) - Whether to create a missing field aggregation when querying the corresponding field. Only affects query time, not mapping. Useful for analytics and reporting.

- **v3**: object (optional) - Read only by Search V3. V2 and V2.5 ignore it. `searchLabel`, set only on this object, is the shared root field the value is copied into (see [Shared search fields on Search V3](#shared-search-fields-on-search-v3)). `degree` is how far that value sits from the entity: `0` keeps the label, and `1` indexes `<searchLabel>_1`. A URN at degree 1 keeps the parsed name on `<label>_1` and the complete URN on `<label>_1_urn`. `boostScore` inside `v3` is the Search V3 weight of that indexed field; the top-level `boostScore` stays the V2 weight. `mappings` is the V3 root and its label subfields; when it is omitted, V3 derives them from `fieldType`. A field of type `URN` or `URN_PARTIAL`, or a root mapping of type `URN`, stores the full URN on its keyword. Filters, facets, sorts, and ranges read that keyword. The parsed name is projected to `_aspects.<aspect>.<field>__urn_name`. Every URN root is two sibling keywords: `<field>` is the parsed name, and `<field>_urn` is the complete URN. Degree qualifies the label first, so a column tag is `tags_1` and `tags_1_urn`, not `tags_urn_1`. `<field>_urn.lowercase` folds that complete URN with `keyword_normalizer`. `text` and `stemmed` are added to `<field>` only when that field is included in full text. `owners` keeps the parsed keyword and has neither analyzer. `lowercase: true` adds `<field>.lowercase`, folded with `keyword_normalizer`, on an indexed keyword root. A `WORD_GRAM` or `TEXT` field qualifies when its V3 root mapping is that keyword. Contains, prefix, suffix, and an unquoted `/q` value read that subfield when it is mapped. Otherwise a term stays case-insensitive and a wildcard is also queried in lowercase. The default is false, so the subfield is not mapped. Shipped `name`, every `name_<degree>`, and `qualifiedName` set it. A URN that names a shared label copies both values onto that label. A schema field's parent has no label, so the parsed dataset name is `parent`. A schema field's parent has no label, so its parsed dataset name is copied into `other`. The document URN stays an exact keyword, with `urn.lowercase` for a case-insensitive match. The parsed identity is the root field `urn_name`, stored in its original case, with `urn_name.lowercase` for a case-insensitive match. `urn_name` is not a full-text field. `queryByDefault` on this object overrides the top-level flag for V3 only. `includeSystemModifiedAt: false` turns off the V2 system-modified sidecar. `enabled: false` drops the field from V3 search (its mapping, its copy into a shared root field, and full text) and leaves V2 unchanged. Names use a keyword root, which supplies `.keyword` automatically, plus declared `text` and `stemmed` subfields on the `name` label.

- **searchIndexed**: boolean (optional) - **⚠️ DEPRECATED, no-op**: Still accepted on `KEYWORD` or `TEXT` fields, but every searchable field is indexed under its own name regardless.

- **entityFieldName**: string (optional) - If set, this field is copied into the root field `{entityFieldName}` on Search V3, so several aspects can fill one entity-level field. `_entityName` aliases the root field `name` when a field of the entity feeds that shared field. On Search V2 and V2.5 it is copied into `_search.{entityFieldName}`.

- **eagerGlobalOrdinals**: boolean (optional) - Whether to set `eager_global_ordinals` to true for this field. This improves aggregation performance for frequently aggregated keyword fields by pre-building ordinals at index time. **Note**: eagerGlobalOrdinals can only be true for KEYWORD, URN, or URN_PARTIAL field types. Defaults to false.

**⚠️ Note on deprecated parameters:** On Search V3, the default weight of an indexed field is `v3.boostScore` when that degree sets one, and otherwise the top-level `boostScore`. Every source of one indexed field uses that same score. `weightsPerFieldValue` applies on Search V2 and V3. Both will be replaced by newer features in future versions. `searchLabel` names the field; it does not by itself weight matches.

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

This annotation is saying that we want to index the title field in Elasticsearch. `entityFieldName: "name"` consolidates this field into the root field `name`, allowing other aspects to contribute to the same consolidated field.

**Advanced Example with New Features:**

```aidl
record DashboardInfo {
  /**
   * Priority level for the dashboard
   */
  @Searchable = {
    "fieldType": "COUNT",
    "v3": { "searchLabel": "priority" },
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
    "v3": { "searchLabel": "owner" }
  }
  owner: string
}
```

This example demonstrates several new features:

- **Priority field**: `fieldType: "COUNT"` with `v3.searchLabel: "priority"` creates a numeric field that copies to the root field `priority` for numeric sorting, and `addToFilters: true` makes it available as a filter
- **Status field**: `addToFilters: true` makes it available as a filter, `filterNameOverride` provides a custom display name "Dashboard Status", and `eagerGlobalOrdinals: true` optimizes aggregation performance for this frequently filtered field
- **Owner field**: `fieldType: "URN"` with `eagerGlobalOrdinals: true` optimizes aggregation performance for owner-based filtering, and `v3.searchLabel: "owner"` copies the field to the root field `owner`

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

**⚠️ DEPRECATED**: This annotation is deprecated and should not be used in new code. Use `v3.searchLabel` instead for ranking functionality.

#### Search Label System

The search label system provides a powerful way to organize search fields and create specialized search experiences:

**Search Labels (`v3.searchLabel`):**

- `searchLabel` is a property of the `v3` object. Search V2 and V2.5 do not read it.
- On Search V3, `v3.searchLabel` names the root field the value is copied into.
- Replaces the previous `sortLabel` and `boostLabel` annotations for a unified approach
- Useful for creating specialized search, sorting, and ranking operations across multiple aspects

**Degree of separation (`v3.degree`):**

A label can appear on the entity and again on a child or related record. Each distance is its own indexed field. `v3.degree` is a non-negative integer and defaults to `0`. Degree `0` keeps the label (`name`, `tags`, `glossaryTerms`, `owners`). Degree `n` is `<searchLabel>_<n>` (`name_1`, `description_1`, `tags_1`, `glossaryTerms_1`, `owners_2`). Nesting does not assign a degree; only an explicit `v3.degree` does. A schema field path or label is `name` at degree 1.

The degree belongs to the place the shared aspect is used. The same record can be the entity's own metadata in one embedding and a child field in another. A path override that sets only `v3.degree`, `v3.boostScore`, `v3.queryByDefault`, or a combination is merged onto the annotation already in effect. An override that sets `fieldType` still replaces that annotation, and the replacement carries the degree and the V3 score.

`v3.boostScore` is the Search V3 weight of that indexed field. Sources of one indexed field must agree on it, and a farther degree must score lower than a nearer one. `v3.queryByDefault` must agree for that same indexed field, and it may differ across degrees of one label. When it is unset, Search V3 leaves the field out of an unqualified query. A custom model sets `v3.queryByDefault` true to opt in. V2 keeps reading the top-level `boostScore` and `queryByDefault`. A dataset's own description, tags, and glossary terms stay on `description`, `tags`, and `glossaryTerms` and stay in an unqualified search. The same values on a schema field index as `description_1`, `tags_1`, and `glossaryTerms_1`, and a schema field path or label indexes as `name_1`. A schema field leaves `v3.queryByDefault` unset, so an unqualified Search V3 query does not read it. An edited column sets `v3.queryByDefault` false on its path override, because the field annotation itself opts in for a business attribute. Search V2 still reads them through the top-level flag. A business attribute's own edited description, tags, and glossary terms stay queried. An explicit filter on the column field still matches.

The existence boolean follows the same degree. Degree 0 keeps `hasDescription`, `hasTags`, and `hasGlossaryTerms`. Degree 1 indexes `hasDescription_1`, `hasTags_1`, and `hasGlossaryTerms_1`. The aspect document still writes the declared name, and V2 filters keep reading it.

**Entity Field Consolidation (`entityFieldName`):**

- Allows multiple aspects to consolidate into a single entity-level field
- Useful for creating unified search experiences across different aspect types
- Fields are copied to the root field named by `entityFieldName`

<a name="shared-search-fields-on-search-v3"></a>
**Shared search fields on Search V3:**

Search V3 keeps three field surfaces. A searchable non-object field with no `v3.searchLabel`, or whose label is its own name, is projected to the document root. A field whose label is a different name is not: chart `title` labeled `name` has a root `name` and no root `title`, and `_aspects.chartInfo.title` copies into `name`. Either way the `_aspects.<aspect>.<field>` copy of a root-projected field is stored with `"index": false` and `"doc_values": false` so the value is not indexed twice. Per-aspect system metadata is the exception: `_aspects.<aspect>._systemMetadata.lastObserved`, `aspectCreated.time`, `aspectModified.time`, `aspectCreated.actor`, `aspectModified.actor`, and both `impersonator` fields stay indexed and, except `impersonator`, still copy into the root `_system` object. `v3.source.index` and `v3.source.docValues` turn either capability back on for that copy only. Object fields are not projected to the root, so their `_aspects` copy follows the V3 root: indexed strings are keywords with a byte-safe `ignore_above`, and dates, numbers and booleans use their native type. `copy_to` on the aspect field, including a URN copied to `<field>_urn`, stays, and a parsed-name sibling such as `<field>__urn_name` stays indexed. The document-root field uses the V3 root, and `copy_to` feeds the shared label. The shared label is one root plus the union of its contributors' analyzer subfields. Full-text search reads `text` and `stemmed` only when the label declares them. A search-only label sets `"index": false` and `"docValues": false` on the label root and on the document-root field. That does not change the aspect-source default, and `v3.source` does not change the label root.

`mappings` replaces the fallback for that field. A root entry has `type` and no `subfield`. A subfield entry has both. Omitted `index` and `docValues` mean true. Exactly one root is required, and a subfield requires a `searchLabel`.

```json
"v3": {
  "searchLabel": "name",
  "mappings": [
    { "type": "KEYWORD" },
    { "subfield": "text", "type": "TEXT" },
    { "subfield": "stemmed", "type": "STEMMED" }
  ]
}
```

That leaves `_aspects.<aspect>.name` stored and not indexed, the same default as every other root-projected field. To keep that copy indexed and aggregatable, set both source flags. Omit a key to leave that capability off. `"index": true` alone is searchable and not sortable or aggregatable.

```json
"v3": {
  "searchLabel": "name",
  "source": { "index": true, "docValues": true }
}
```

An indexed keyword root automatically provides the case-preserving `.keyword` representation used by exact queries, so annotations do not declare that subfield again. A description is a text blob: its common label uses a keyword root with `index` and `docValues` both false, plus `text` and `stemmed`. V3 rejects filters with values, facets, sorts, ranges and exact-match boosts on that label or its contributors. An `EXISTS` query on an unindexed text blob cannot work either; use the indexed `hasDescription` boolean to filter for presence.

When `mappings` is omitted, `KEYWORD` is an indexed keyword root. `TEXT`, `TEXT_PARTIAL`, `WORD_GRAM`, `URN` and `URN_PARTIAL` use that root plus `text` and `stemmed` on the label. `DATETIME`, `BOOLEAN`, `COUNT` and `DOUBLE` are one native root. Browse path v2 keeps its path analyzer on the source field. Browse path v1 is not a Search V3 type: those fields stay on V2 and are dropped from the V3 index. Bundled shared labels use explicit mappings so their contract does not depend on fallback. The top-level `fieldType` is still the V2 mapping.

Full-text search reads shared root fields instead of every searchable field. Search V3 autocomplete prefixes the shared `name` field.
The browse path v2 root keeps its path analyzer. Cross-aspect system metadata is the root `_system` object: `created.time`, `created.actor`, `modified.time`, `modified.actor`, `lastObserved`, `runId`, and `lastRunId`. It is not the same as the root `runId`, `systemCreated`, or `systemIndexModified` fields. Each searchable string field copies into:

- the root field its `v3.searchLabel` or `entityFieldName` names. The label is the semantic bucket, so an ingested
  value and an edited value of the same concept share it: names (the root field `name`),
  `description` for the entity's own description, edited description, glossary definition and assertion description, and `description_1` for a schema field description,
  `name_1` for schema field paths and labels, `tags` for tags on the entity and `tags_1` for tags
  on a schema field, `glossaryTerms` and `glossaryTerms_1` on the same split, and `owners` for owner urns. `sourceCreatedAt` and `sourceLastModifiedAt` are times
  reported by the source platform. An incident's start time is `sourceCreatedAt`. A time or actor
  recorded when DataHub itself wrote the aspect stays indexed on
  `_aspects.<aspect>._systemMetadata` (`aspectCreated` and `aspectModified`, including `actor` and
  `impersonator`) and copies to `_system.created.time`,
  `_system.created.actor`, `_system.modified.time`, and
  `_system.modified.actor`. `impersonator` has no root field. The V2 system-modified sidecar
  (`includeSystemModifiedAt`) is Search V2 and V2.5 only;
- otherwise, when the field is queried by default, the root field `other`, so V3 still searches every field V2 searches. An
  entity none of whose fields names `name` sends the field its `_entityName` alias names to
  `name` instead. A reference field (`@SearchableRef`) copies the referenced entity's default text
  fields into `other`. Its raw URN stays exact. A URN field stores the full URN on its keyword and
  projects the parsed name to `_aspects.<aspect>.<field>__urn_name`. Every URN root keeps the parsed
  name on `<field>` and the complete URN on `<field>_urn`. Degree qualifies the label first: a column tag is `tags_1` with the full URN `tags_1_urn`. `<field>_urn.lowercase` is the same URN
  through `keyword_normalizer`. Full text and its exact-match boost read `<field>` only when the
  field is included in full text. `owners` keeps the parsed keyword and has no `text` or `stemmed`.
  A URN that names a shared label copies both values onto that label.
  The document URN stays an exact keyword for identity filters and direct URN lookup. `urn.lowercase` is that same URN through `keyword_normalizer`, and a case-insensitive match reads it. The parsed identity is the root field `urn_name`, in its original case, and a case-insensitive match reads `urn_name.lowercase`. `urn_name` is not included in full text. A dataset URN's parsed name is the dataset name. A
  schema-field URN's parsed name is the field path with version and type annotations removed,
  without the parent dataset.

A structured property is not a shared field. A `string` or `rich_text` property is full-text indexed only when
its definition has a `searchConfiguration`: that adds `text` and `stemmed` subfields to its root keyword field
`structuredProperties.<field>`. A `urn` property with `searchConfiguration` keeps that field as the exact URN, with `.lowercase`, and
writes the parsed name to `structuredProperties.<field>__urn_name` in its original case. A case-insensitive match reads
`__urn_name.lowercase`. `text` and `stemmed` on that sibling are what full text reads. `copyTo` copies that name into the named shared
fields, and `copyUrnTo` copies the complete URN. A destination named in both lists receives both values. A `urn` property without `searchConfiguration` keeps both exact keywords and their `.lowercase` subfields and has no analyzed
subfields. `queryByDefault` decides whether an unqualified search includes the property.
`boostScore` is the weight of `.text` (0.8 when unset) and `subfieldBoostScores.stemmed` is the weight of `.stemmed`
(half the `.text` weight when unset). `copyTo` names shared fields (`name`, `qualifiedName`, `description`,
`name_1`, `other`) or a root field that feeds one. `copyTo` copies the parsed name for a URN property, and `copyUrnTo` copies the complete URN. A destination named in both lists receives both values. The property field and the stored name stay as they are. Unset copies nowhere.
`excludeFromFullTextSearch` applies to Search V2 and V2.5; Search V3 ignores it.
Number and date properties are not analyzed, and a `copyTo` on one is ignored. A property with no `searchConfiguration`
keeps its keyword, number or date field for filters and facets only.

A shared field fed by several fields holds all of their values, so an ingested description and an edited
description are both searchable, and an edited display name is searched as `name` with the ingested
name. A label that declares `text` and `stemmed` is analyzed on those subfields (an identifier such as `customer_id` is indexed whole and by its parts, and short parts such as `id` are dropped like any short word). `other` is a search-only keyword plus `text` and `stemmed`, because no PDL field owns it. A label whose root is an indexed keyword stays exact for sorts and filters; analyzer subfields do not replace that root. Matches in `name` and
`qualifiedName` weigh 10, in `other` 0.5, and in every other
shared field 1. A structured property's `.text` subfield weighs its `boostScore`, or 0.8 when that is unset. A field configuration names every opted-in property with `structuredProperties`, or one property with its full field path. A search field
configuration (`fieldConfigurations` in the search configuration) names a shared field either directly or by any field
that feeds it, except `other`, which only its own name selects. Matched fields ("Matched on") come from the urn,
the fields that feed each searched shared field, except `name_1`, and of `other` only the fields that
name or tag an entity (`name`, `editedName`, `displayName`, `fullName`, `title`, `tags` and `glossaryTerms`): a match
in a column or in another field still counts, but is not reported, since its values can be too large to fetch with
every hit.

**Benefits of the New System:**

1. **Organized Search Fields**: Shared labels are root fields, and cross-aspect system metadata is the root `_system` object
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

| Deprecated Pattern                        | New Pattern          | Benefits                                                                    |
| ----------------------------------------- | -------------------- | --------------------------------------------------------------------------- |
| `includeSystemModifiedAt: true`           | **V2 and V2.5 only** | Search V3 uses `systemMetadata.aspectModified` and does not map the sidecar |
| `systemModifiedAtFieldName: "customName"` | **V2 and V2.5 only** | The sidecar name stays on the top-level annotation for older indices        |

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
entities with either of these properties just by searching for the tag name. On Search V3 the column copy is the
separate field `tags_1`, so a filter on `tags` matches the entity's own tags. Glossary terms follow the same split
(`glossaryTerms` and `glossaryTerms_1`).

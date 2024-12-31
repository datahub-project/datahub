import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Emitting Patch Updates to DataHub

## Why Would You Use Patch

By default, most of the SDK tutorials and APIs involve applying full upserts at the aspect level, e.g. replacing the aspect entirely. 
This means that when you want to change even a single field within an aspect without modifying others, you need to do a read-modify-write to avoid overwriting existing fields.
To support these scenarios, DataHub supports `PATCH` operations to perform targeted changes for individual fields or values within arrays of fields are possible without impacting other existing metadata.

:::note

Currently, PATCH support is only available for a selected set of aspects, so before pinning your hopes on using PATCH as a way to make modifications to aspect values, confirm whether your aspect supports PATCH semantics. The complete list of Aspects that are supported are maintained [here](https://github.com/datahub-project/datahub/blob/9588440549f3d99965085e97b214a7dabc181ed2/entity-registry/src/main/java/com/linkedin/metadata/models/registry/template/AspectTemplateEngine.java#L24). 

:::

## How To Use Patches

Here's how to find the appropriate classes for the language for your choice.

<Tabs>
<TabItem value="Python" label="Python SDK" default>

The Python Patch builders are entity-oriented and located in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/9588440549f3d99965085e97b214a7dabc181ed2/metadata-ingestion/src/datahub/specific) module and located in the `datahub.specific` module. 
Patch builder helper classes exist for 

- [Datasets](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/dataset.py)
- [Charts](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/chart.py)
- [Dashboards](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/dashboard.py)
- [Data Jobs (Tasks)](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/datajob.py)
- [Data Products](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/dataproduct.py)

And we are gladly accepting contributions for Containers, Data Flows (Pipelines), Tags, Glossary Terms, Domains, and ML Models.

### Add & Remove Owners for Dataset

To add & remove specific owners for a dataset:

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_owner_patch.py show_path_as_comment }}
```

### Add & Remove Tags for Dataset

To add & remove specific tags for a dataset:

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_tag_patch.py show_path_as_comment }}
```

And for a specific schema field within the Dataset:

```python
{{ inline /metadata-ingestion/examples/library/dataset_field_add_tag_patch.py show_path_as_comment }}
```

### Add & Remove Glossary Terms for Dataset

To add & remove specific glossary terms for a dataset:

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_glossary_term_patch.py show_path_as_comment }}
```

And for a specific schema field within the Dataset:

```python
{{ inline /metadata-ingestion/examples/library/dataset_field_add_glossary_term_patch.py show_path_as_comment }}
```

### Add & Remove Structured Properties for Dataset

To add & remove structured properties for a dataset:

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_structured_properties_patch.py show_path_as_comment }}
```

### Add & Remove Upstream Lineage for Dataset

To add & remove a lineage edge connecting a dataset to it's upstream or input at both the dataset and schema field level:

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_upstream_lineage_patch.py show_path_as_comment }}
```

### Add & Remove Read-Only Custom Properties for Dataset

To add & remove specific custom properties for a dataset: 

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_remove_custom_properties_patch.py show_path_as_comment }}
```

</TabItem>
<TabItem value="Java" label="Java SDK">

The Java Patch builders are aspect-oriented and located in the [datahub-client](https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/datahub-client/src/main/java/datahub/client/patch) module under the `datahub.client.patch` namespace.

### Add & Remove Read-Only Custom Properties

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAddRemove.java show_path_as_comment }}
```

### Add Data Job Lineage

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DataJobLineageAdd.java show_path_as_comment }}
```

</TabItem>
</Tabs>


## Advanced: How Patch works

To understand how patching works, it's important to understand a bit about our [models](../what/aspect.md). Entities are comprised of Aspects
which can be reasoned about as JSON representations of the object models. To be able to patch these we utilize [JsonPatch](https://jsonpatch.com/). The components of a JSON Patch are the path, operation, and value.

### Path

The JSON path refers to a value within the schema. This can be a single field or can be an entire object reference depending on what the path is.
For our patches we are primarily targeting single fields or even single array elements within a field. To be able to target array elements by id, we go through a translation process
of the schema to transform arrays into maps. This allows a path to reference a particular array element by key rather than by index, for example a specific tag urn being added to a dataset.

#### Examples

A patch path for targeting an upstream dataset:

`/upstreams/urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created_upstream,PROD)`

Breakdown:
* `/upstreams` -> References the upstreams field of the UpstreamLineage aspect, this is an array of Upstream objects where the key is the Urn
* `/urn:...` -> The dataset to be targeted by the operation

A patch path for targeting a fine-grained lineage upstream:

`/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),foo)/urn:li:query:queryId/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created_upstream,PROD),bar)`

Breakdown:
* `/fineGrainedLineages` -> References the fineGrainedLineages field on an UpstreamLineage, this is an array of FineGrainedLineage objects keyed on transformOperation, downstream urn, and query urn
* `/TRANSFORM` -> transformOperation, one of the fields determining the key for a fineGrainedLineage
* `/urn:li:schemaField:...` -> The downstream schemaField referenced in this schema, part of the key for a fineGrainedLineage
* `/urn:li:query:...` -> The query urn this relationship was derived from, part of the key for a fineGrainedLineage
* `/urn:li:schemaField:` -> The upstream urn that is being targeted by this patch operation

This showcases that in some cases the key for objects is simple, in others in can be complex to determine, but for our fully supported use cases we have 
SDK support on both the Java and Python side that will generate these patches for you as long as you supply the required method parameters. 
Path is generally the most complicated portion of a patch to reason about as it requires intimate knowledge of the schema and models.

### Operation

Operation is a limited enum of a few supported types pulled directly from the JSON Patch spec. DataHub only supports `ADD` and `REMOVE` of these options
as the other patch operations do not currently have a use case within our system.

#### Add

Add is a bit of a misnomer for the JSON Patch spec, it is not an explicit add but an upsert/replace. If the path specified does not exist, it will be created, 
but if the path already exists the value will be replaced. Patch operations apply at a path level so it is possible to do full replaces of arrays or objects in the schema
using adds, but generally the most useful use case for patch is to add elements to arrays without affecting the other elements as full upserts are supported by standard ingestion.

#### Remove

Remove operations require the path specified to be present, or an error will be thrown, otherwise they operate as one would expect. The specified path will be removed from the aspect.

### Value

Value is the actual information that will be stored at a path. If the path references an object then this will include the JSON key value pairs for that object.

#### Examples

An example UpstreamLineage object value:

```json
{
  "auditStamp": {
    "time": 0,
    "actor": "urn:li:corpuser:unknown"
  },
  "dataset": "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/my-folder/my-file.txt,PROD)",
  "type": "TRANSFORMED"
}
```
 
For the previous path example (`/upstreams/urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created_upstream,PROD)`), this object would represent the UpstreamLineage object for that path.
This specifies the required fields to properly represent that object. Note: by modifying this path, you could reference a single field within the UpstreamLineage object itself, like so:

```json
{
  "path": "/upstreams/urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created_upstream,PROD)/type",
  "op": "ADD",
  "value": "VIEW"
}
```

### Implementation details

#### Template Classes

Template classes are the mechanism that maps fields to their corresponding JSON paths. Since DataMaps are not true JSON, first we convert a RecordTemplate to a JSON String,
perform any additional process to map array fields to their keys, apply the patch, and then convert the JSON object back to a RecordTemplate to work with the rest of the application.

The template classes we currently support can be found in the `entity-registry` module. They are split up by aspect, with the GenericTemplate applying to any non-directly supported aspects.
The GenericTemplate allows for use cases that we have not gotten around to directly support yet, but puts more burden on the user to generate patches correctly.

The template classes are utilized in `EntityServiceImpl` where a MCP is determined to be either a patch or standard upsert which then routes through to the stored templates registered on the EntityRegistry.
The core logical flow each Template runs through is set up in the `Template` interface, with some more specific logic in the lower level interfaces for constructing/deconstructing array field keys.
Most of the complexity around these classes is knowledge of schema and JSON path traversals.

##### ArrayMergingTemplate & CompoundKeyTemplate

`ArrayMergingTemplate` is utilized for any aspect which has array fields and may either be used directly or use `CompoundKeyTemplate`. `ArrayMergingTemplate` is the simpler one that can only be used directly for
single value keys. `CompoundKeyTemplate` allows for support of multi-field keys. For more complex examples like FineGrainedLineage, further logic is needed to construct a key as it is not generalizable to other aspects, see `UpstreamLineageTemplate` for full special case implementation.

#### PatchBuilders

There are patch builder SDK classes for constructing patches in both Java and Python. The Java patch builders all extend `AbstractMultiFieldPatchBuilder` which sets up the
base functionality for patch builder subtypes. Each implementation of this abstract class is targeted at a particular aspect and contains specific field based update methods
for the most common use cases. On the Python side patch builders live in the `src/specific/` directory and are organized by entity type.
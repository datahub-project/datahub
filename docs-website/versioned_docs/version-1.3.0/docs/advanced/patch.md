---
title: Emitting Patch Updates to DataHub
slug: /advanced/patch
custom_edit_url: 'https://github.com/datahub-project/datahub/blob/master/docs/advanced/patch.md'
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Emitting Patch Updates to DataHub

## Why Would You Use Patch

By default, most of the SDK tutorials and APIs involve applying full upserts at the aspect level, e.g. replacing the aspect entirely.
This means that when you want to change even a single field within an aspect without modifying others, you need to do a read-modify-write to avoid overwriting existing fields.
To support these scenarios, DataHub supports `PATCH` operations to perform targeted changes for individual fields or values within arrays of fields are possible without impacting other existing metadata.

:::note

PATCH support is now supported generically via [OpenAPI](../api/openapi/openapi-usage-guide.md#generic-patching). Traditional PATCH support is only available for a selected set of aspects. The complete list of Aspects that are supported are maintained by the `SUPPORTED_TEMPLATES` constant [here](https://github.com/datahub-project/datahub/blob/master/entity-registry/src/main/java/com/linkedin/metadata/aspect/patch/template/AspectTemplateEngine.java#L23).

:::

## How To Use Patches

The Patch builders are available in both Python and Java SDKs:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

The Python Patch builders are entity-oriented and located in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/9588440549f3d99965085e97b214a7dabc181ed2/metadata-ingestion/src/datahub/specific) module and located in the `datahub.specific` module.
Patch builder helper classes exist for

- [Datasets](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/dataset.py)
- [Charts](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/chart.py)
- [Dashboards](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/dashboard.py)
- [Data Jobs (Tasks)](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/datajob.py)
- [Data Products](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/specific/dataproduct.py)

And we are gladly accepting contributions for Containers, Data Flows (Pipelines), Tags, Glossary Terms, Domains, and ML Models.

</TabItem>
<TabItem value="java" label="Java SDK">

The Java Patch builders are aspect-oriented and located in the [datahub-client](https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/datahub-client/src/main/java/datahub/client/patch) module under the `datahub.client.patch` namespace.

</TabItem>
</Tabs>

### Add & Remove Owners for Dataset

To add & remove specific owners for a dataset:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_owner_patch.py
from datahub.emitter.mce_builder import make_dataset_urn, make_group_urn, make_user_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)

# Create Dataset Patch to Add + Remove Owners
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_owner(
    OwnerClass(make_user_urn("user-to-add-id"), OwnershipTypeClass.TECHNICAL_OWNER)
)
patch_builder.remove_owner(make_group_urn("group-to-remove-id"))
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>

</Tabs>

### Add & Remove Tags for Dataset

To add & remove specific tags for a dataset:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_tag_patch.py
from datahub.emitter.mce_builder import make_tag_urn
from datahub.metadata.schema_classes import TagAssociationClass
from datahub.sdk import DataHubClient, DatasetUrn
from datahub.specific.dataset import DatasetPatchBuilder

client = DataHubClient.from_env()

# Create the Dataset updater.
patch_builder = DatasetPatchBuilder(
    DatasetUrn(platform="snowflake", name="fct_users_created", env="PROD")
)
patch_builder.add_tag(TagAssociationClass(make_tag_urn("tag-to-add-id")))
patch_builder.remove_tag("urn:li:tag:tag-to-remove-id")

# Do the update.
client.entities.update(patch_builder)

```

And for a specific schema field within the Dataset:

```python
# Inlined from /metadata-ingestion/examples/library/dataset_field_add_tag_patch.py
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import TagAssociationClass
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)

# Create Dataset Patch to Add + Remove Tag for 'profile_id' column
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.for_field("profile_id").add_tag(
    TagAssociationClass(make_tag_urn("tag-to-add-id"))
)
patch_builder.for_field("profile_id").remove_tag("urn:li:tag:tag-to-remove-id")
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>

</Tabs>

### Add & Remove Glossary Terms for Dataset

To add & remove specific glossary terms for a dataset:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_glossary_term_patch.py
from datahub.emitter.mce_builder import make_dataset_urn, make_term_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GlossaryTermAssociationClass
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)

# Create Dataset Patch to Add + Remove Term for 'profile_id' column
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_term(GlossaryTermAssociationClass(make_term_urn("term-to-add-id")))
patch_builder.remove_term(make_term_urn("term-to-remove-id"))
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

And for a specific schema field within the Dataset:

```python
# Inlined from /metadata-ingestion/examples/library/dataset_field_add_glossary_term_patch.py
from datahub.emitter.mce_builder import make_dataset_urn, make_term_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GlossaryTermAssociationClass
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)

# Create Dataset Patch to Add + Remove Term for 'profile_id' column
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.for_field("profile_id").add_term(
    GlossaryTermAssociationClass(make_term_urn("term-to-add-id"))
)
patch_builder.for_field("profile_id").remove_term(
    "urn:li:glossaryTerm:term-to-remove-id"
)
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>
</Tabs>

### Add & Remove Structured Properties for Dataset

To add & remove structured properties for a dataset:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_structured_properties_patch.py
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

# Create Dataset Patch to Add and Remove Structured Properties
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_structured_property(
    "urn:li:structuredProperty:retentionTimeInDays", 12
)
patch_builder.remove_structured_property(
    "urn:li:structuredProperty:customClassification"
)
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>

</Tabs>

### Add & Remove Upstream Lineage for Dataset

To add & remove a lineage edge connecting a dataset to it's upstream or input at both the dataset and schema field level:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_upstream_lineage_patch.py
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)
upstream_to_remove_urn = make_dataset_urn(
    platform="s3", name="fct_users_old", env="PROD"
)
upstream_to_add_urn = make_dataset_urn(platform="s3", name="fct_users_new", env="PROD")

# Create Dataset Patch to Add & Remove Upstream Lineage Edges
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.remove_upstream_lineage(upstream_to_remove_urn)
patch_builder.add_upstream_lineage(
    UpstreamClass(upstream_to_add_urn, DatasetLineageTypeClass.TRANSFORMED)
)

# ...And also include schema field lineage
upstream_field_to_add_urn = make_schema_field_urn(upstream_to_add_urn, "profile_id")
downstream_field_to_add_urn = make_schema_field_urn(dataset_urn, "profile_id")

patch_builder.add_fine_grained_lineage(
    FineGrainedLineageClass(
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        [upstream_field_to_add_urn],
        [downstream_field_to_add_urn],
    )
)

upstream_field_to_remove_urn = make_schema_field_urn(
    upstream_to_remove_urn, "profile_id"
)
downstream_field_to_remove_urn = make_schema_field_urn(dataset_urn, "profile_id")

patch_builder.remove_fine_grained_lineage(
    FineGrainedLineageClass(
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        [upstream_field_to_remove_urn],
        [downstream_field_to_remove_urn],
    )
)

patch_mcps = patch_builder.build()


# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>

</Tabs>

### Add & Remove Read-Only Custom Properties for Dataset

To add & remove specific custom properties for a dataset:

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_remove_custom_properties_patch.py
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

# Create Dataset Patch to Add + Remove Custom Properties
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_custom_property("cluster_name", "datahubproject.acryl.io")
patch_builder.remove_custom_property("retention_time")
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>
<TabItem value="java" label="Java SDK">

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAddRemove.java
package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DatasetCustomPropertiesAddRemove {

  private DatasetCustomPropertiesAddRemove() {}

  /**
   * Applies Add and Remove property operations on an existing custom properties aspect without
   * affecting any other properties
   *
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    MetadataChangeProposal datasetPropertiesProposal =
        new DatasetPropertiesPatchBuilder()
            .urn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
            .addCustomProperty("cluster_name", "datahubproject.acryl.io")
            .removeCustomProperty("retention_time")
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    try {
      Future<MetadataWriteResponse> response = emitter.emit(datasetPropertiesProposal);

      System.out.println(response.get().getResponseContent());
    } catch (Exception e) {
      log.error("Failed to emit metadata to DataHub", e);
      throw e;
    } finally {
      emitter.close();
    }
  }
}

```

</TabItem>
</Tabs>

### Add & Remove Data Job Lineage

<Tabs groupId="sdk-language">
<TabItem value="python" label="Python SDK" default>

```python
# Inlined from /metadata-ingestion/examples/library/datajob_add_lineage_patch.py
from datahub.emitter.mce_builder import (
    make_data_job_urn,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass as FineGrainedLineage,
    FineGrainedLineageDownstreamTypeClass as FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamTypeClass as FineGrainedLineageUpstreamType,
)
from datahub.specific.datajob import DataJobPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create DataJob URN
datajob_urn = make_data_job_urn(
    orchestrator="airflow", flow_id="dag_abc", job_id="task_456"
)

# Create DataJob Patch to Add Lineage
patch_builder = DataJobPatchBuilder(datajob_urn)
patch_builder.add_input_dataset(
    make_dataset_urn(platform="kafka", name="SampleKafkaDataset", env="PROD")
)
patch_builder.add_output_dataset(
    make_dataset_urn(platform="hive", name="SampleHiveDataset", env="PROD")
)
patch_builder.add_input_datajob(
    make_data_job_urn(orchestrator="airflow", flow_id="dag_abc", job_id="task_123")
)
patch_builder.add_input_dataset_field(
    make_schema_field_urn(
        parent_urn=make_dataset_urn(
            platform="hive", name="fct_users_deleted", env="PROD"
        ),
        field_path="user_id",
    )
)
patch_builder.add_output_dataset_field(
    make_schema_field_urn(
        parent_urn=make_dataset_urn(
            platform="hive", name="fct_users_created", env="PROD"
        ),
        field_path="user_id",
    )
)

# Update column-level lineage through the Data Job
lineage1 = FineGrainedLineage(
    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
    upstreams=[
        make_schema_field_urn(make_dataset_urn("postgres", "raw_data.users"), "user_id")
    ],
    downstreamType=FineGrainedLineageDownstreamType.FIELD,
    downstreams=[
        make_schema_field_urn(
            make_dataset_urn("postgres", "analytics.user_metrics"),
            "user_id",
        )
    ],
    transformOperation="IDENTITY",
    confidenceScore=1.0,
)
patch_builder.add_fine_grained_lineage(lineage1)
patch_builder.remove_fine_grained_lineage(lineage1)
# Replaces all existing fine-grained lineages
patch_builder.set_fine_grained_lineages([lineage1])

patch_mcps = patch_builder.build()

# Emit DataJob Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)

```

</TabItem>
<TabItem value="java" label="Java SDK">

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DataJobLineageAdd.java
package io.datahubproject.examples;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.DataJobInputOutputPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DataJobLineageAdd {

  private DataJobLineageAdd() {}

  /**
   * Adds lineage to an existing DataJob without affecting any lineage
   *
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    String token = "";
    try (RestEmitter emitter =
        RestEmitter.create(b -> b.server("http://localhost:8080").token(token))) {
      MetadataChangeProposal dataJobIOPatch =
          new DataJobInputOutputPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"))
              .addInputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .addOutputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .addInputDatajobEdge(
                  DataJobUrn.createFromString(
                      "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)"))
              .addInputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)"))
              .addOutputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
              .build();

      Future<MetadataWriteResponse> response = emitter.emit(dataJobIOPatch);

      System.out.println(response.get().getResponseContent());
    } catch (Exception e) {
      log.error("Failed to emit metadata to DataHub", e);
      throw new RuntimeException(e);
    }
  }
}

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

- `/upstreams` -> References the upstreams field of the UpstreamLineage aspect, this is an array of Upstream objects where the key is the Urn
- `/urn:...` -> The dataset to be targeted by the operation

A patch path for targeting a fine-grained lineage upstream:

`/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),foo)/urn:li:query:queryId/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created_upstream,PROD),bar)`

Breakdown:

- `/fineGrainedLineages` -> References the fineGrainedLineages field on an UpstreamLineage, this is an array of FineGrainedLineage objects keyed on transformOperation, downstream urn, and query urn
- `/TRANSFORM` -> transformOperation, one of the fields determining the key for a fineGrainedLineage
- `/urn:li:schemaField:...` -> The downstream schemaField referenced in this schema, part of the key for a fineGrainedLineage
- `/urn:li:query:...` -> The query urn this relationship was derived from, part of the key for a fineGrainedLineage
- `/urn:li:schemaField:` -> The upstream urn that is being targeted by this patch operation

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

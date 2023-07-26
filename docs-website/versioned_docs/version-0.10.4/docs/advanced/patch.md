---
title: "But First, Semantics: Upsert versus Patch"
slug: /advanced/patch
custom_edit_url: "https://github.com/datahub-project/datahub/blob/master/docs/advanced/patch.md"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# But First, Semantics: Upsert versus Patch

## Why Would You Use Patch

By default, most of the SDK tutorials and API-s involve applying full upserts at the aspect level. This means that typically, when you want to change one field within an aspect without modifying others, you need to do a read-modify-write to not overwrite existing fields.
To support these scenarios, DataHub supports PATCH based operations so that targeted changes to single fields or values within arrays of fields are possible without impacting other existing metadata.

:::note

Currently, PATCH support is only available for a selected set of aspects, so before pinning your hopes on using PATCH as a way to make modifications to aspect values, confirm whether your aspect supports PATCH semantics. The complete list of Aspects that are supported are maintained [here](https://github.com/datahub-project/datahub/blob/9588440549f3d99965085e97b214a7dabc181ed2/entity-registry/src/main/java/com/linkedin/metadata/models/registry/template/AspectTemplateEngine.java#L24). In the near future, we do have plans to automatically support PATCH semantics for aspects by default.

:::

## How To Use Patch

Examples for using Patch are sprinkled throughout the API guides.
Here's how to find the appropriate classes for the language for your choice.

<Tabs>
<TabItem value="Java" label="Java SDK">

The Java Patch builders are aspect-oriented and located in the [datahub-client](https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/datahub-client/src/main/java/datahub/client/patch) module under the `datahub.client.patch` namespace.

Here are a few illustrative examples using the Java Patch builders:

### Add Custom Properties

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAdd.java
package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import datahub.client.MetadataWriteResponse;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class DatasetCustomPropertiesAdd {

  private DatasetCustomPropertiesAdd() {

  }

  /**
   * Adds properties to an existing custom properties aspect without affecting any existing properties
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
      MetadataChangeProposal datasetPropertiesProposal = new DatasetPropertiesPatchBuilder()
          .urn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
          .addCustomProperty("cluster_name", "datahubproject.acryl.io")
          .addCustomProperty("retention_time", "2 years")
          .build();

      String token = "";
      RestEmitter emitter = RestEmitter.create(
          b -> b.server("http://localhost:8080")
              .token(token)
      );
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

### Add and Remove Custom Properties

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAddRemove.java
package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class DatasetCustomPropertiesAddRemove {

  private DatasetCustomPropertiesAddRemove() {

  }

  /**
   * Applies Add and Remove property operations on an existing custom properties aspect without
   * affecting any other properties
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
      MetadataChangeProposal datasetPropertiesProposal = new DatasetPropertiesPatchBuilder()
          .urn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
          .addCustomProperty("cluster_name", "datahubproject.acryl.io")
          .removeCustomProperty("retention_time")
          .build();

      String token = "";
      RestEmitter emitter = RestEmitter.create(
          b -> b.server("http://localhost:8080")
              .token(token)
      );
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

### Add Data Job Lineage

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DataJobLineageAdd.java
package io.datahubproject.examples;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import datahub.client.MetadataWriteResponse;
import datahub.client.patch.datajob.DataJobInputOutputPatchBuilder;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class DataJobLineageAdd {

  private DataJobLineageAdd() {

  }

  /**
   * Adds lineage to an existing DataJob without affecting any lineage
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    String token = "";
    try (RestEmitter emitter = RestEmitter.create(
        b -> b.server("http://localhost:8080")
            .token(token)
    )) {
      MetadataChangeProposal dataJobIOPatch = new DataJobInputOutputPatchBuilder().urn(UrnUtils
              .getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"))
          .addInputDatasetEdge(DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
          .addOutputDatasetEdge(DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
          .addInputDatajobEdge(DataJobUrn.createFromString("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)"))
          .addInputDatasetField(UrnUtils.getUrn(
              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)"))
          .addOutputDatasetField(UrnUtils.getUrn(
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
<TabItem value="Python" label="Python SDK" default>

The Python Patch builders are entity-oriented and located in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/9588440549f3d99965085e97b214a7dabc181ed2/metadata-ingestion/src/datahub/specific) module and located in the `datahub.specific` module.

Here are a few illustrative examples using the Python Patch builders:

### Add Properties to Dataset

```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_properties.py
import logging
from typing import Union

from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.specific.dataset import DatasetPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Get an emitter, either REST or Kafka, this example shows you both
def get_emitter() -> Union[DataHubRestEmitter, DatahubKafkaEmitter]:
    USE_REST_EMITTER = True
    if USE_REST_EMITTER:
        gms_endpoint = "http://localhost:8080"
        return DataHubRestEmitter(gms_server=gms_endpoint)
    else:
        kafka_server = "localhost:9092"
        schema_registry_url = "http://localhost:8081"
        return DatahubKafkaEmitter(
            config=KafkaEmitterConfig(
                connection=KafkaProducerConnectionConfig(
                    bootstrap=kafka_server, schema_registry_url=schema_registry_url
                )
            )
        )


dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

with get_emitter() as emitter:
    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_custom_property("cluster_name", "datahubproject.acryl.io")
        .add_custom_property("retention_time", "2 years")
        .build()
    ):
        emitter.emit(patch_mcp)


log.info(f"Added cluster_name, retention_time properties to dataset {dataset_urn}")

```

</TabItem>
</Tabs>

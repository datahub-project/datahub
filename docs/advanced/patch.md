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
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAdd.java show_path_as_comment }}
```

### Add and Remove Custom Properties

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAddRemove.java show_path_as_comment }}
```

### Add Data Job Lineage

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DataJobLineageAdd.java show_path_as_comment }}
```

</TabItem>
<TabItem value="Python" label="Python SDK" default>

The Python Patch builders are entity-oriented and located in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/9588440549f3d99965085e97b214a7dabc181ed2/metadata-ingestion/src/datahub/specific) module and located in the `datahub.specific` module.

Here are a few illustrative examples using the Python Patch builders:

### Add Properties to Dataset

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_properties.py show_path_as_comment }}
```

</TabItem>
</Tabs>


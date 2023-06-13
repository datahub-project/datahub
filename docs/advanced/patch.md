import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Patch Semantics

## Why Would You Use Patch

By default, ingestion does a full upsert at the aspect level. This means that when you want to change one field progrmamatically you need to do a read-modify-write to not overwrite existing fields. 
With patch based semantics for proposals, targeted changes to single fields or values within arrays of fields are possible without impacting other existing metadata.

## Examples of patch

<Tabs>
<TabItem value="customPropertiesJava" label="Java SDK: Add custom properties">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAdd.java show_path_as_comment }}
```

</TabItem>
<TabItem value="dataJobLineageJava" label="Java SDK: Add Data Job lineage">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DataJobLineageAdd.java show_path_as_comment }}
```

</TabItem>
<TabItem value="customPropertiesJavaRemove" label="Java SDK: Add and remove custom properties">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAddRemove.java show_path_as_comment }}
```

</TabItem>
<TabItem value="pythonSDKCustomProperties" label="Python SDK: Add Custom Properties" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_properties.py show_path_as_comment }}
```

</TabItem>
</Tabs>


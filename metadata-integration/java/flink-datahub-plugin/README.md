## DataHub Flink Plugin

This plugin uses the [JobListener](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/advanced/job_status_listener/)
interface provided by Flink to emit metadata to DataHub.

### Modelling

We have modelled Flink resources in the following way:

#### Flink Cluster

Any Flink Cluster (Application or Session Mode) is mapped to a `dataFlow` in DataHub.

We pull the ClusterName from either the kubernetes deployment, or a standalone cluster in a container.

#### Job

A Flink cluster can have one or more Jobs submitted to it, we model that as a `dataJob` attached to `dataFlow` created above.

The name (& URN) is generated from the PipelineOptions.Name configuration.

#### JobRun

When a job is submitted we submit a `dataProcessInstance` with status "Started".

When a job is finished we submit whether the job has status Successful or Failed depending on whether an error was thrown by Flink.

### Usage

The package needs to be included in the flinkShadowJar

```gradle
flinkShadowJar 'cko.dp:datahub-flink-plugin:0.1.3:all'
```

```java
import com.checkout.DatahubJobListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.checkout.shaded.com.linkedin.common.DatasetUrnArray;
import com.checkout.shaded.com.linkedin.common.FabricType;
import com.checkout.shaded.com.linkedin.common.urn.DataPlatformUrn;
import com.checkout.shaded.com.linkedin.common.urn.DatasetUrn;

import com.checkout.shaded.datahub.client.kafka.KafkaEmitter;
import com.checkout.shaded.datahub.client.kafka.KafkaEmitterConfig;

public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.set(PipelineOptions.NAME, "my job name");
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

    // Instantiate an emitter (of any type, although Kafka is recommended in case the GMS is not available).
    KafkaEmitter emitter = new KafkaEmitter(KafkaEmitterConfig.builder().build());
    DatahubJobListener jobListener = DatahubJobListener.builder()
            .configuration((Configuration) env.getConfiguration())
            .emitter(emitter)
            // Lineage can be set here and is emitted attached to the DataJob
            .upstreamDatasetsOverride(new com.checkout.shaded.com.linkedin.common.DatasetUrnArray(
                    new DatasetUrn(new DataPlatformUrn("kafka"), topic, FabricType.PROD)
            ))
            .downstreamDatasetsOverride(new DatasetUrnArray( // care is required from the user to make the URN generated exactly match what is already in DataHub (if its present already).
                    new DatasetUrn(new DataPlatformUrn("bigquery"), String.format("%s.%s.%s", gcpProjectName, gcpDatasetName, gcpTableName).toLowerCase(), FabricType.PROD)
            ))
            .build();
    env.registerJobListener(jobListener);

    // rest of Flink Job
}
```

### Further Work (please remove when completed)

- Setting up publishing of the jar.
- Make the Config parsing more robust to different use cases (right now its very tailored to how Checkout.com creates their jobs)
- Add Flink as a default Data Platform to DataHub
- Use [Flink 2.0's](https://openlineage.io/docs/next/integrations/flink/flink2/#usage) lineage feature to parse lineage.

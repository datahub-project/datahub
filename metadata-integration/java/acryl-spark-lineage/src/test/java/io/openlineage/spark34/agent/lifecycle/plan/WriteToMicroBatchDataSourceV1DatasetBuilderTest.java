package io.openlineage.spark34.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.Spark40DatasetBuilderFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.sources.DeltaSink;
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;

class WriteToMicroBatchDataSourceV1DatasetBuilderTest {

  @Test
  void spark4FactoryExtractsPathFromDeltaSinkWithoutCatalogTable() {
    OpenLineageContext context =
        OpenLineageContext.builder()
            .sparkSession(mock(SparkSession.class))
            .sparkContext(mock(SparkContext.class))
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();

    WriteToMicroBatchDataSourceV1DatasetBuilder builder =
        new Spark40DatasetBuilderFactory()
            .getOutputBuilders(context).stream()
                .filter(WriteToMicroBatchDataSourceV1DatasetBuilder.class::isInstance)
                .map(WriteToMicroBatchDataSourceV1DatasetBuilder.class::cast)
                .findFirst()
                .orElseThrow();

    WriteToMicroBatchDataSourceV1 write = mock(WriteToMicroBatchDataSourceV1.class);
    when(write.sink()).thenReturn(new DeltaSink(new Path("file:///tmp/existing-delta-output")));
    when(write.catalogTable()).thenReturn(Option.empty());
    when(write.schema()).thenReturn(new StructType());

    List<OpenLineage.OutputDataset> outputs = builder.apply(mock(SparkListenerEvent.class), write);

    assertEquals(1, outputs.size());
    assertEquals("file", outputs.get(0).getNamespace());
    assertEquals("/tmp/existing-delta-output", outputs.get(0).getName());
  }
}

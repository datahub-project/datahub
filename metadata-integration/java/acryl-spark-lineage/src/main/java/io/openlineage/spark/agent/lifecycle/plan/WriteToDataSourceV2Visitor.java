/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import scala.Option;

@Slf4j
public final class WriteToDataSourceV2Visitor
    extends QueryPlanVisitor<WriteToDataSourceV2, OutputDataset> {
  private static final String KAFKA_STREAMING_WRITE_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaStreamingWrite";
  private static final String FOREACH_BATCH_SINK_CLASS_NAME =
      "org.apache.spark.sql.execution.streaming.sources.ForeachBatchSink";

  public WriteToDataSourceV2Visitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    boolean result = plan instanceof WriteToDataSourceV2;
    if (log.isDebugEnabled()) {
      log.debug(
          "The supplied logical plan {} {} an instance of {}",
          plan.getClass().getCanonicalName(),
          result ? "IS" : "IS NOT",
          WriteToDataSourceV2.class.getCanonicalName());
    }
    return result;
  }

  @Override
  public List<OutputDataset> apply(LogicalPlan plan) {
    List<OutputDataset> result = Collections.emptyList();
    WriteToDataSourceV2 write = (WriteToDataSourceV2) plan;
    BatchWrite batchWrite = write.batchWrite();
    if (batchWrite instanceof MicroBatchWrite) {
      MicroBatchWrite microBatchWrite = (MicroBatchWrite) batchWrite;
      StreamingWrite streamingWrite = microBatchWrite.writeSupport();
      Class<? extends StreamingWrite> streamingWriteClass = streamingWrite.getClass();
      String streamingWriteClassName = streamingWriteClass.getCanonicalName();
      if (KAFKA_STREAMING_WRITE_CLASS_NAME.equals(streamingWriteClassName)) {
        result = handleKafkaStreamingWrite(streamingWrite);
      } else if (streamingWriteClassName != null
          && (streamingWriteClassName.contains("FileStreamSink")
              || streamingWriteClassName.contains("ForeachBatchSink")
              || streamingWriteClassName.contains("ConsoleSink")
              || streamingWriteClassName.contains("DeltaSink")
              || streamingWriteClassName.contains("ParquetSink"))) {
        result = handleFileBasedStreamingWrite(streamingWrite, write);
      } else {
        log.warn(
            "The streaming write class '{}' for '{}' is not supported",
            streamingWriteClass,
            MicroBatchWrite.class.getCanonicalName());
      }
    } else {
      log.warn("Unsupported batch write class: {}", batchWrite.getClass().getCanonicalName());
    }

    return result;
  }

  private @NotNull List<OutputDataset> handleFileBasedStreamingWrite(
      StreamingWrite streamingWrite, WriteToDataSourceV2 write) {
    log.debug(
        "Handling file-based streaming write: {}", streamingWrite.getClass().getCanonicalName());

    try {
      // Try to extract path from streaming write
      Optional<String> pathOpt = extractPathFromStreamingWrite(streamingWrite);
      if (!pathOpt.isPresent()) {
        log.warn("Could not extract path from file-based streaming write");
        return Collections.emptyList();
      }

      String path = pathOpt.get();
      log.debug("Found streaming write path: {}", path);

      // Create dataset from path
      URI uri = URI.create(path);
      DatasetIdentifier identifier = PathUtils.fromURI(uri);
      String namespace = identifier.getNamespace();
      String name = identifier.getName();

      log.debug("Creating output dataset with namespace: {}, name: {}", namespace, name);

      // Get schema from the write operation
      StructType schema = null;
      if (write.query() != null) {
        schema = write.query().schema();
      }

      // Use the inherited outputDataset() method to create the dataset
      OutputDataset dataset = outputDataset().getDataset(name, namespace, schema);
      return Collections.singletonList(dataset);

    } catch (Exception e) {
      log.error("Error extracting output dataset from file-based streaming write", e);
      return Collections.emptyList();
    }
  }

  private Optional<String> extractPathFromStreamingWrite(StreamingWrite streamingWrite) {
    try {
      // Try to get path using reflection from various sink types
      String className = streamingWrite.getClass().getCanonicalName();

      // For ForeachBatchSink, try to get the underlying sink's path
      if (className != null && className.contains("ForeachBatchSink")) {
        // ForeachBatchSink typically wraps another sink or has batch function
        // We need to extract path from the context of how it's used
        return tryExtractPathFromForeachBatch(streamingWrite);
      }

      // For file-based sinks, try standard path extraction
      if (className != null
          && (className.contains("FileStreamSink")
              || className.contains("ParquetSink")
              || className.contains("DeltaSink"))) {
        return tryExtractPathFromFileSink(streamingWrite);
      }

      // For console sink, return console identifier
      if (className != null && className.contains("ConsoleSink")) {
        return Optional.of("console://output");
      }

    } catch (Exception e) {
      log.debug("Error extracting path from streaming write: {}", e.getMessage());
    }

    return Optional.empty();
  }

  private Optional<String> tryExtractPathFromForeachBatch(StreamingWrite streamingWrite) {
    try {
      // ForeachBatchSink doesn't have a direct path since outputs are determined
      // dynamically by the user's foreachBatch function. The actual lineage
      // will be captured when the user's function executes batch operations.
      //
      // For now, we return empty to indicate that this sink doesn't have
      // a predetermined output path, and rely on the batch operations
      // within the foreachBatch function to generate proper lineage events.
      log.debug("ForeachBatchSink detected - outputs will be tracked from batch operations");
      return Optional.empty();
    } catch (Exception e) {
      log.debug("Could not extract path from ForeachBatchSink: {}", e.getMessage());
      return Optional.empty();
    }
  }

  private Optional<String> tryExtractPathFromFileSink(StreamingWrite streamingWrite) {
    try {
      // Try to extract path using reflection
      Optional<String> pathOpt = tryReadField(streamingWrite, "path");
      if (pathOpt.isPresent()) {
        return pathOpt;
      }

      // Try alternative field names
      pathOpt = tryReadField(streamingWrite, "outputPath");
      if (pathOpt.isPresent()) {
        return pathOpt;
      }

      pathOpt = tryReadField(streamingWrite, "location");
      if (pathOpt.isPresent()) {
        return pathOpt;
      }

    } catch (Exception e) {
      log.debug("Error extracting path from file sink: {}", e.getMessage());
    }

    return Optional.empty();
  }

  private <T> Optional<T> tryReadField(Object target, String fieldName) {
    try {
      T result = (T) FieldUtils.readDeclaredField(target, fieldName, true);
      return result == null ? Optional.empty() : Optional.of(result);
    } catch (IllegalAccessException e) {
      log.debug("Could not read field {}: {}", fieldName, e.getMessage());
      return Optional.empty();
    }
  }

  private @NotNull List<OutputDataset> handleKafkaStreamingWrite(StreamingWrite streamingWrite) {
    KafkaStreamWriteProxy proxy = new KafkaStreamWriteProxy(streamingWrite);
    Optional<String> topicOpt = proxy.getTopic();
    StructType schemaOpt = proxy.getSchema();

    Optional<String> bootstrapServersOpt = proxy.getBootstrapServers();
    String namespace = KafkaBootstrapServerResolver.resolve(bootstrapServersOpt);

    if (topicOpt.isPresent() && bootstrapServersOpt.isPresent()) {
      String topic = topicOpt.get();

      OutputDataset dataset = outputDataset().getDataset(topic, namespace, schemaOpt);
      return Collections.singletonList(dataset);
    } else {
      String topicPresent =
          topicOpt.isPresent() ? "Topic **IS** present" : "Topic **IS NOT** present";
      String bootstrapServersPresent =
          bootstrapServersOpt.isPresent()
              ? "Bootstrap servers **IS** present"
              : "Bootstrap servers **IS NOT** present";
      log.warn(
          "Both topic and bootstrapServers need to be present in order to construct an output dataset. {}. {}",
          bootstrapServersPresent,
          topicPresent);
      return Collections.emptyList();
    }
  }

  @Slf4j
  private static final class KafkaStreamWriteProxy {
    private final StreamingWrite streamingWrite;

    public KafkaStreamWriteProxy(StreamingWrite streamingWrite) {
      String incomingClassName = streamingWrite.getClass().getCanonicalName();
      if (!KAFKA_STREAMING_WRITE_CLASS_NAME.equals(incomingClassName)) {
        throw new IllegalArgumentException(
            "Expected the supplied argument to be of type '"
                + KAFKA_STREAMING_WRITE_CLASS_NAME
                + "' but received '"
                + incomingClassName
                + "' instead");
      }

      this.streamingWrite = streamingWrite;
    }

    public Optional<String> getTopic() {
      return this.<Option<String>>tryReadField(streamingWrite, "topic")
          .flatMap(ScalaConversionUtils::asJavaOptional);
    }

    public StructType getSchema() {
      Optional<StructType> schema = this.tryReadField(streamingWrite, "schema");
      return schema.orElseGet(StructType::new);
    }

    public Optional<String> getBootstrapServers() {
      Optional<Map<String, Object>> producerParams = tryReadField(streamingWrite, "producerParams");
      return producerParams.flatMap(
          props -> Optional.ofNullable((String) props.get("bootstrap.servers")));
    }

    private <T> Optional<T> tryReadField(Object target, String fieldName) {
      try {
        T result = (T) FieldUtils.readDeclaredField(target, fieldName, true);
        return result == null ? Optional.empty() : Optional.of(result);
      } catch (IllegalAccessException e) {
        return Optional.empty();
      }
    }
  }
}

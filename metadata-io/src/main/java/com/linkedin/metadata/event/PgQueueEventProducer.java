package com.linkedin.metadata.event;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHeader;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

/**
 * pgQueue-backed {@link EventProducer} used when {@code datahub.messaging.transport=pgqueue}.
 *
 * <p>Serializes the same Avro payloads as the Kafka {@code KafkaEventProducer} (Confluent wire
 * format: magic byte + schema id + Avro bytes) and enqueues them via {@link
 * MetadataQueueStore#enqueue}. Topics must exist in the pgQueue SqlSetup catalog ({@code
 * postgres.pgQueue.topics.*}) so partition DDL and retention align with Kafka topic names.
 *
 * <p>Wire format matches Confluent's standard so consumers using the schema registry API can
 * deserialize payloads the same way as Kafka-backed deployments after optional application-layer
 * decompression (see {@code message.payload_compression}).
 */
@Slf4j
public class PgQueueEventProducer extends EventProducer {

  /** Confluent wire format: 1-byte magic + 4-byte schema id prefix before the Avro payload. */
  static final byte CONFLUENT_MAGIC_BYTE = 0x0;

  static final String CONFLUENT_AVRO_CONTENT_TYPE = "application/vnd.confluent.avro+binary";

  private final MetadataQueueStore metadataQueueStore;
  private final TopicConvention topicConvention;
  private final SchemaRegistryService schemaRegistryService;
  private final QueueTopicDefaults topicDefaultsFallback;
  private final PgQueuePayloadCompression payloadCompression;
  private final Function<OperationContext, List<QueueMessageHeader>> headerResolver;

  public PgQueueEventProducer(
      @Nonnull MetadataQueueStore metadataQueueStore,
      @Nonnull TopicConvention topicConvention,
      @Nonnull SchemaRegistryService schemaRegistryService,
      @Nonnull QueueTopicDefaults topicDefaultsFallback,
      @Nonnull PgQueuePayloadCompression payloadCompression) {
    this(
        metadataQueueStore,
        topicConvention,
        schemaRegistryService,
        topicDefaultsFallback,
        payloadCompression,
        ctx -> List.of());
  }

  public PgQueueEventProducer(
      @Nonnull MetadataQueueStore metadataQueueStore,
      @Nonnull TopicConvention topicConvention,
      @Nonnull SchemaRegistryService schemaRegistryService,
      @Nonnull QueueTopicDefaults topicDefaultsFallback,
      @Nonnull PgQueuePayloadCompression payloadCompression,
      @Nonnull Function<OperationContext, List<QueueMessageHeader>> headerResolver) {
    this.metadataQueueStore = metadataQueueStore;
    this.topicConvention = topicConvention;
    this.schemaRegistryService = schemaRegistryService;
    this.topicDefaultsFallback = topicDefaultsFallback;
    this.payloadCompression = payloadCompression;
    this.headerResolver = headerResolver;
  }

  @Override
  public void flush() {
    // pgQueue writes are synchronous; nothing to flush.
  }

  @Override
  public Future<?> produceMCL(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull MetadataChangeLog metadataChangeLog) {
    final String topicName = getMetadataChangeLogTopicName(aspectSpec);
    try {
      GenericRecord record = EventUtils.pegasusToAvroMCL(metadataChangeLog);
      return enqueueConfluentGenericRecord(opContext, topicName, urn.toString(), record, "MCL");
    } catch (IOException e) {
      log.error("Failed to convert Pegasus MCL to Avro: urn={}", urn, e);
      return CompletableFuture.failedFuture(
          new ModelConversionException("Failed to convert Pegasus MCL to Avro", e));
    }
  }

  @Override
  public String getMetadataChangeLogTopicName(@Nonnull AspectSpec aspectSpec) {
    return aspectSpec.isTimeseries()
        ? topicConvention.getMetadataChangeLogTimeseriesTopicName()
        : topicConvention.getMetadataChangeLogVersionedTopicName();
  }

  @Override
  protected Future<?> doProduceMetadataChangeProposal(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull MetadataChangeProposal metadataChangeProposal) {
    final String topicName = topicConvention.getMetadataChangeProposalTopicName();
    try {
      GenericRecord record = EventUtils.pegasusToAvroMCP(metadataChangeProposal);
      return enqueueConfluentGenericRecord(opContext, topicName, urn.toString(), record, "MCP");
    } catch (IOException e) {
      log.error("Failed to convert Pegasus MCP to Avro: urn={}", urn, e);
      return CompletableFuture.failedFuture(
          new ModelConversionException("Failed to convert Pegasus MCP to Avro", e));
    }
  }

  @Override
  public String getMetadataChangeProposalTopicName() {
    return topicConvention.getMetadataChangeProposalTopicName();
  }

  @Override
  public Future<?> produceFailedMetadataChangeProposalAsync(
      @Nonnull OperationContext opContext,
      @Nonnull MetadataChangeProposal mcp,
      @Nonnull Set<Throwable> throwables) {
    final String topicName = topicConvention.getFailedMetadataChangeProposalTopicName();
    try {
      FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
      fmcp.setError(opContext.traceException(throwables));
      fmcp.setMetadataChangeProposal(mcp);
      GenericRecord record = EventUtils.pegasusToAvroFailedMCP(fmcp);
      return enqueueConfluentGenericRecord(
          opContext, topicName, mcp.getEntityUrn().toString(), record, "FMCP");
    } catch (IOException e) {
      log.error("Failed to convert FailedMetadataChangeProposal to Avro", e);
      return CompletableFuture.failedFuture(
          new ModelConversionException(
              "Failed to convert FailedMetadataChangeProposal to Avro", e));
    }
  }

  @Override
  public Future<?> producePlatformEvent(
      @Nonnull OperationContext opContext,
      @Nonnull String name,
      @Nullable String key,
      @Nonnull PlatformEvent payload) {
    final String topicName = topicConvention.getPlatformEventTopicName();
    final String routingKey = key == null ? name : key;
    try {
      GenericRecord record = EventUtils.pegasusToAvroPE(payload);
      return enqueueConfluentGenericRecord(
          opContext, topicName, routingKey, record, "platform event");
    } catch (IOException e) {
      log.error("Failed to convert Pegasus Platform Event to Avro: {}", payload, e);
      return CompletableFuture.failedFuture(
          new ModelConversionException("Failed to convert Pegasus Platform Event to Avro", e));
    }
  }

  @Override
  public String getPlatformEventTopicName() {
    return topicConvention.getPlatformEventTopicName();
  }

  /**
   * Publishes arbitrary Avro (Confluent wire format) to a logical queue topic — used by deprecated
   * MCE error paths (FMCE) and similar Kafka-parity topics when transport is pgQueue. Context-free
   * overload for legacy code paths that lack an {@link OperationContext}.
   */
  public Future<?> publishRawTopicConfluentAvro(
      @Nonnull String topicName,
      @Nullable String routingKey,
      @Nonnull GenericRecord record,
      @Nonnull String debugLabel) {
    return enqueueConfluentGenericRecord(
        null, topicName, routingKey != null ? routingKey : "", record, debugLabel);
  }

  private Future<?> enqueueConfluentGenericRecord(
      @Nullable OperationContext opContext,
      String topicName,
      String routingKey,
      GenericRecord record,
      String debugLabel) {
    Optional<Integer> schemaIdOpt = schemaRegistryService.getSchemaIdForTopic(topicName);
    if (schemaIdOpt.isEmpty()) {
      log.warn(
          "Skipping {}: schema-registry has no id for topic {}. "
              + "Verify SchemaRegistryService is available.",
          debugLabel,
          topicName);
      return CompletableFuture.completedFuture(null);
    }
    try {
      byte[] inner = encodeConfluentAvro(record, schemaIdOpt.get());
      enqueueConfluentPayload(opContext, topicName, routingKey, inner);
      log.debug("Enqueued {} to pgQueue topic {} (key={})", debugLabel, topicName, routingKey);
      return CompletableFuture.completedFuture(null);
    } catch (IOException e) {
      log.error("Failed to encode Avro for {} topic {}", debugLabel, topicName, e);
      return CompletableFuture.failedFuture(
          new ModelConversionException("Failed to encode Avro payload", e));
    } catch (Exception e) {
      log.error("Failed to enqueue {} to pgQueue topic {}", debugLabel, topicName, e);
      return CompletableFuture.completedFuture(null);
    }
  }

  private void enqueueConfluentPayload(
      @Nullable OperationContext opContext,
      @Nonnull String topicName,
      @Nonnull String routingKey,
      @Nonnull byte[] innerConfluentAvro) {
    byte[] stored = PgQueuePayloadCodec.encode(innerConfluentAvro, payloadCompression);
    List<QueueMessageHeader> headers =
        opContext != null ? headerResolver.apply(opContext) : List.of();
    metadataQueueStore.enqueue(
        topicName,
        routingKey,
        topicDefaultsFallback,
        QueueTopicMetadata.DEFAULT_PRIORITY,
        stored,
        Optional.of(CONFLUENT_AVRO_CONTENT_TYPE),
        headers,
        payloadCompression);
  }

  /**
   * Encodes the record using Confluent's Avro wire format: {@code [0x00][schemaId(BE int32)][avro
   * binary]}. Package-private for unit-test inspection.
   */
  static byte[] encodeConfluentAvro(@Nonnull GenericRecord record, int schemaId)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(CONFLUENT_MAGIC_BYTE);
    out.write(ByteBuffer.allocate(Integer.BYTES).putInt(schemaId).array());
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}

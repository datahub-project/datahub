package com.linkedin.metadata.dao.producer;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.producer.context.outbound.OutboundContextResolver;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The topic names that this emits to can be controlled by constructing this with a {@link
 * TopicConvention}. If none is given, defaults to a {@link TopicConventionImpl} with the default
 * delimiter of an underscore (_).
 */
@Slf4j
public class KafkaEventProducer extends EventProducer {

  private final Producer<String, ? extends IndexedRecord> _producer;
  private final TopicConvention _topicConvention;
  private final KafkaHealthChecker _kafkaHealthChecker;
  private final MetricUtils metricUtils;
  private final OutboundContextResolver outboundContextResolver;
  private boolean canWrite = true;

  @Override
  public void flush() {
    _producer.flush();
  }

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   * @param kafkaHealthChecker The {@link Callback} to invoke when the request is completed
   * @param outboundContextResolver runs registered enrichers against every outbound record before
   *     it is sent — OSS ships with no enrichers and the resolver is a no-op; Cloud plugs in tenant
   *     / trace / security header writers without modifying this class.
   */
  public KafkaEventProducer(
      @Nonnull final Producer<String, ? extends IndexedRecord> producer,
      @Nonnull final TopicConvention topicConvention,
      @Nonnull final KafkaHealthChecker kafkaHealthChecker,
      MetricUtils metricUtils,
      @Nonnull final OutboundContextResolver outboundContextResolver) {
    _producer = producer;
    _topicConvention = topicConvention;
    _kafkaHealthChecker = kafkaHealthChecker;
    this.metricUtils = metricUtils;
    this.outboundContextResolver = outboundContextResolver;
  }

  public void setWritable(boolean writable) {
    canWrite = writable;
  }

  @Override
  @WithSpan
  public Future<?> produceMCL(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(Optional.empty());
    }
    GenericRecord record;
    try {
      log.debug(
          String.format(
              "Converting Pegasus snapshot to Avro snapshot urn %s\nMetadataChangeLog: %s",
              urn, metadataChangeLog));
      record = EventUtils.pegasusToAvroMCL(metadataChangeLog);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus MAE to Avro: %s", metadataChangeLog), e);
      throw new ModelConversionException("Failed to convert Pegasus MAE to Avro", e);
    }

    String topic = getMetadataChangeLogTopicName(aspectSpec);
    ProducerRecord<String, GenericRecord> producerRecord =
        new ProducerRecord<>(topic, urn.toString(), record);
    outboundContextResolver.apply(producerRecord, opContext);
    return sendProducerRecord(
        producerRecord, _kafkaHealthChecker.getKafkaCallBack(metricUtils, "MCL", urn.toString()));
  }

  @Override
  public String getMetadataChangeLogTopicName(@Nonnull AspectSpec aspectSpec) {
    String topic = _topicConvention.getMetadataChangeLogVersionedTopicName();
    if (aspectSpec.isTimeseries()) {
      topic = _topicConvention.getMetadataChangeLogTimeseriesTopicName();
    }
    return topic;
  }

  @Override
  @WithSpan
  public Future<?> produceMetadataChangeProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final MetadataChangeProposal metadataChangeProposal) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(Optional.empty());
    }
    GenericRecord record;

    try {
      log.debug(
          String.format(
              "Converting Pegasus snapshot to Avro snapshot urn %s\nMetadataChangeProposal: %s",
              urn, metadataChangeProposal));
      record = EventUtils.pegasusToAvroMCP(metadataChangeProposal);
    } catch (IOException e) {
      log.error(
          String.format("Failed to convert Pegasus MCP to Avro: %s", metadataChangeProposal), e);
      throw new ModelConversionException("Failed to convert Pegasus MCP to Avro", e);
    }

    String topic = _topicConvention.getMetadataChangeProposalTopicName();
    ProducerRecord<String, GenericRecord> producerRecord =
        new ProducerRecord<>(topic, urn.toString(), record);
    outboundContextResolver.apply(producerRecord, opContext);
    return sendProducerRecord(
        producerRecord, _kafkaHealthChecker.getKafkaCallBack(metricUtils, "MCP", urn.toString()));
  }

  @Override
  public String getMetadataChangeProposalTopicName() {
    return _topicConvention.getMetadataChangeProposalTopicName();
  }

  @Override
  public Future<?> produceFailedMetadataChangeProposalAsync(
      @Nonnull OperationContext opContext,
      @Nonnull MetadataChangeProposal mcp,
      @Nonnull Set<Throwable> throwables) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(Optional.empty());
    }

    try {
      String topic = _topicConvention.getFailedMetadataChangeProposalTopicName();
      final FailedMetadataChangeProposal failedMetadataChangeProposal =
          createFailedMCPEvent(opContext, mcp, throwables);

      final GenericRecord record = EventUtils.pegasusToAvroFailedMCP(failedMetadataChangeProposal);
      log.debug(
          "Sending FailedMessages to topic - {}",
          _topicConvention.getFailedMetadataChangeProposalTopicName());
      log.info(
          "Error while processing FMCP: FailedMetadataChangeProposal - {}",
          failedMetadataChangeProposal);

      ProducerRecord<String, GenericRecord> producerRecord =
          new ProducerRecord<>(topic, mcp.getEntityUrn().toString(), record);
      outboundContextResolver.apply(producerRecord, opContext);
      return sendProducerRecord(
          producerRecord,
          _kafkaHealthChecker.getKafkaCallBack(metricUtils, "FMCP", mcp.getEntityUrn().toString()));
    } catch (IOException e) {
      log.error(
          "Error while sending FailedMetadataChangeProposal: Exception  - {}, FailedMetadataChangeProposal - {}",
          e.getStackTrace(),
          mcp);
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public Future<?> producePlatformEvent(
      @Nonnull OperationContext opContext,
      @Nonnull String name,
      @Nullable String key,
      @Nonnull PlatformEvent event) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(Optional.empty());
    }
    GenericRecord record;
    try {
      log.debug(
          String.format("Converting Pegasus Event to Avro Event urn %s\nEvent: %s", name, event));
      record = EventUtils.pegasusToAvroPE(event);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus Platform Event to Avro: %s", event), e);
      throw new ModelConversionException("Failed to convert Pegasus Platform Event to Avro", e);
    }

    final String topic = _topicConvention.getPlatformEventTopicName();
    ProducerRecord<String, GenericRecord> producerRecord =
        new ProducerRecord<>(topic, key == null ? name : key, record);
    outboundContextResolver.apply(producerRecord, opContext);
    return sendProducerRecord(
        producerRecord, _kafkaHealthChecker.getKafkaCallBack(metricUtils, "Platform Event", name));
  }

  @Override
  public String getPlatformEventTopicName() {
    return _topicConvention.getPlatformEventTopicName();
  }

  @Override
  public void produceDataHubUpgradeHistoryEvent(
      @Nonnull OperationContext opContext, @Nonnull DataHubUpgradeHistoryEvent event) {
    // We allow this to write even when in not writable mode to allow DH to start up
    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus Event to Avro Event\nEvent: %s", event));
      record = EventUtils.pegasusToAvroDUHE(event);
    } catch (IOException e) {
      log.error(
          String.format(
              "Failed to convert Pegasus DataHub Upgrade History Event to Avro: %s", event),
          e);
      throw new ModelConversionException("Failed to convert Pegasus Platform Event to Avro", e);
    }

    final String topic = _topicConvention.getDataHubUpgradeHistoryTopicName();
    ProducerRecord<String, GenericRecord> producerRecord =
        new ProducerRecord<>(topic, event.getVersion(), record);
    outboundContextResolver.apply(producerRecord, opContext);
    sendProducerRecord(
        producerRecord,
        _kafkaHealthChecker.getKafkaCallBack(
            metricUtils, "History Event", "Event Version: " + event.getVersion()));
  }

  @Nonnull
  private static FailedMetadataChangeProposal createFailedMCPEvent(
      @Nonnull OperationContext opContext,
      @Nonnull MetadataChangeProposal event,
      @Nonnull Set<Throwable> throwables) {
    final FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setError(opContext.traceException(throwables));
    fmcp.setMetadataChangeProposal(event);
    return fmcp;
  }

  /**
   * Sends a {@link ProducerRecord} via the wildcard-typed {@link #_producer}. The local-variable
   * extraction needed for {@link #outboundContextResolver}{@code .apply(...)} prevents the original
   * inline-construction wildcard capture, so we erase the producer's generics here with a single
   * suppressed cast rather than scatter cast noise across every produce* method.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private Future<?> sendProducerRecord(
      @Nonnull final ProducerRecord<String, GenericRecord> producerRecord,
      @Nonnull final Callback callback) {
    return ((Producer) _producer).send(producerRecord, callback);
  }
}

package com.linkedin.metadata.kafka.listener.mcl;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCL_EVENT_CONSUMER_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.listener.AbstractKafkaListenerRegistrar;
import com.linkedin.metadata.kafka.listener.GenericKafkaListener;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
@Slf4j
public class MCLKafkaListenerRegistrar
    extends AbstractKafkaListenerRegistrar<
        MetadataChangeLog, MetadataChangeLogHook, GenericRecord> {
  private final OperationContext systemOperationContext;
  private final ConfigurationProvider configurationProvider;

  @Autowired
  @Qualifier(MCL_EVENT_CONSUMER_NAME)
  private KafkaListenerContainerFactory<?> kafkaListenerContainerFactory;

  @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
  private String consumerGroupBase;

  @Value("${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}")
  private String mclVersionedTopicName;

  @Value(
      "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES + "}")
  private String mclTimeseriesTopicName;

  public MCLKafkaListenerRegistrar(
      KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
      @Qualifier(MCL_EVENT_CONSUMER_NAME)
          KafkaListenerContainerFactory<?> kafkaListenerContainerFactory,
      @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
          String consumerGroupBase,
      List<MetadataChangeLogHook> hooks,
      ObjectMapper objectMapper,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      ConfigurationProvider configurationProvider) {
    super(
        kafkaListenerEndpointRegistry,
        kafkaListenerContainerFactory,
        consumerGroupBase,
        hooks,
        objectMapper);
    this.systemOperationContext = systemOperationContext;
    this.configurationProvider = configurationProvider;
  }

  @Override
  protected String getProcessorType() {
    // Check if batch processing is enabled
    boolean batchEnabled = false;
    try {
      if (configurationProvider.getMetadataChangeLog() != null
          && configurationProvider.getMetadataChangeLog().getConsumer() != null
          && configurationProvider.getMetadataChangeLog().getConsumer().getBatch() != null) {
        batchEnabled =
            configurationProvider.getMetadataChangeLog().getConsumer().getBatch().isEnabled();
      }
    } catch (Exception e) {
      log.debug(
          "Error checking batch processing configuration, defaulting to individual processing", e);
    }

    return batchEnabled ? "BatchMetadataChangeLogProcessor" : "MetadataChangeLogProcessor";
  }

  @Override
  protected List<String> getTopicNames() {
    return List.of(mclVersionedTopicName, mclTimeseriesTopicName);
  }

  @Override
  protected boolean isFineGrainedLoggingEnabled() {
    return configurationProvider.getKafka().getConsumer().getMcl().isFineGrainedLoggingEnabled();
  }

  @Override
  protected String getAspectsToDropConfig() {
    return configurationProvider.getKafka().getConsumer().getMcl().getAspectsToDrop();
  }

  @Override
  @Nonnull
  public GenericKafkaListener<MetadataChangeLog, MetadataChangeLogHook, GenericRecord>
      createListener(
          @Nonnull String consumerGroupId,
          @Nonnull List<MetadataChangeLogHook> hooks,
          boolean fineGrainedLoggingEnabled,
          @Nonnull Map<String, Set<String>> aspectsToDrop) {

    // Check if batch processing is enabled
    boolean batchEnabled = false;
    try {
      if (configurationProvider.getMetadataChangeLog() != null
          && configurationProvider.getMetadataChangeLog().getConsumer() != null
          && configurationProvider.getMetadataChangeLog().getConsumer().getBatch() != null) {
        batchEnabled =
            configurationProvider.getMetadataChangeLog().getConsumer().getBatch().isEnabled();
      }
    } catch (Exception e) {
      log.debug(
          "Error checking batch processing configuration, defaulting to individual processing", e);
    }

    if (batchEnabled) {
      MCLBatchKafkaListener listener = new MCLBatchKafkaListener();
      return listener.init(
          systemOperationContext, consumerGroupId, hooks, fineGrainedLoggingEnabled, aspectsToDrop);
    } else {
      MCLKafkaListener listener = new MCLKafkaListener();
      return listener.init(
          systemOperationContext, consumerGroupId, hooks, fineGrainedLoggingEnabled, aspectsToDrop);
    }
  }
}

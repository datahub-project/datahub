package com.linkedin.metadata.kafka.listener.usage;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.SIMPLE_EVENT_CONSUMER_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.config.DataHubUsageEventsProcessorCondition;
import com.linkedin.metadata.kafka.hook.usage.DataHubUsageEventHook;
import com.linkedin.metadata.kafka.listener.AbstractKafkaListenerRegistrar;
import com.linkedin.metadata.kafka.listener.GenericKafkaListener;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
@Conditional(DataHubUsageEventsProcessorCondition.class)
public class DataHubUsageEventKafkaListenerRegistrar
    extends AbstractKafkaListenerRegistrar<JsonNode, DataHubUsageEventHook, String> {

  private final OperationContext systemOperationContext;
  private final ConfigurationProvider configurationProvider;

  @Value("${DATAHUB_USAGE_EVENT_NAME:" + Topics.DATAHUB_USAGE_EVENT + "}")
  private String dataHubUsageEventTopicName;

  public DataHubUsageEventKafkaListenerRegistrar(
      KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
      @Qualifier(SIMPLE_EVENT_CONSUMER_NAME)
          KafkaListenerContainerFactory<?> kafkaListenerContainerFactory,
      @Value(
              "${DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_ID:datahub-usage-event-consumer-job-client}")
          String consumerGroupBase,
      List<DataHubUsageEventHook> hooks,
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
    return "DataHubUsageEventProcessor";
  }

  @Override
  protected List<String> getTopicNames() {
    return List.of(dataHubUsageEventTopicName);
  }

  @Override
  protected boolean isFineGrainedLoggingEnabled() {
    return false;
  }

  @Override
  protected String getAspectsToDropConfig() {
    return null;
  }

  @Nonnull
  @Override
  public GenericKafkaListener<JsonNode, DataHubUsageEventHook, String> createListener(
      @Nonnull String consumerGroupId,
      @Nonnull List<DataHubUsageEventHook> hooks,
      boolean fineGrainedLoggingEnabled,
      @Nonnull Map<String, Set<String>> aspectsToDrop) {
    DataHubUsageEventKafkaListener listener =
        new DataHubUsageEventKafkaListener(
            objectMapper, configurationProvider.getAws().getEventBridge().getAuditEventExport());
    return listener.init(
        systemOperationContext, consumerGroupId, hooks, fineGrainedLoggingEnabled, aspectsToDrop);
  }
}

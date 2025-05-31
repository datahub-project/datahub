package com.linkedin.metadata.kafka.hook.usage.aws;

import static com.linkedin.mxe.Topics.DATAHUB_USAGE_EVENT;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.aws.EventBridgeConfiguration;
import com.linkedin.metadata.dataHubUsage.aws.EventBridgeBatchProcessor;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.kafka.hook.usage.DataHubUsageEventHook;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventBridgeAuditEventExportHook implements DataHubUsageEventHook {

  private final EventBridgeBatchProcessor eventBridgeBatchProcessor;
  private final EventBridgeConfiguration eventBridgeConfiguration;

  public EventBridgeAuditEventExportHook(ConfigurationProvider configurationProvider) {
    this.eventBridgeConfiguration = configurationProvider.getAws().getEventBridge();
    this.eventBridgeBatchProcessor =
        eventBridgeConfiguration.getAuditEventExport().isEnabled()
            ? new EventBridgeBatchProcessor(eventBridgeConfiguration)
            : null;
  }

  @Override
  public DataHubUsageEventHook init(@Nonnull OperationContext systemOperationContext) {
    return DataHubUsageEventHook.super.init(systemOperationContext);
  }

  @Nonnull
  @Override
  public String getConsumerGroupSuffix() {
    return eventBridgeConfiguration.getAuditEventExport().getConsumerGroupSuffix();
  }

  @Override
  public boolean isEnabled() {
    return eventBridgeConfiguration.getAuditEventExport().isEnabled();
  }

  @Override
  public void invoke(@Nonnull JsonNode event) throws Exception {
    if (eventBridgeConfiguration.getAuditEventExport().isEnabled()
        && eventBridgeBatchProcessor != null) {
      eventBridgeBatchProcessor.addEvent(event, DATAHUB_USAGE_EVENT);
      log.info(
          "Audit event added for export, type: {}, actor: {}, timestamp: {}",
          Optional.ofNullable(event.get(DataHubUsageEventConstants.TYPE))
              .orElse(JsonNodeFactory.instance.textNode(""))
              .asText(),
          Optional.ofNullable(event.get(DataHubUsageEventConstants.ACTOR_URN))
              .orElse(JsonNodeFactory.instance.textNode(""))
              .asText(),
          Optional.ofNullable(event.get(DataHubUsageEventConstants.TIMESTAMP))
              .orElse(JsonNodeFactory.instance.textNode(""))
              .asText());
    }
  }
}

package com.linkedin.metadata.kafka.listener.usage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.aws.AuditEventExportConfiguration;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.kafka.hook.usage.DataHubUsageEventHook;
import com.linkedin.metadata.kafka.listener.AbstractKafkaListener;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class DataHubUsageEventKafkaListener
    extends AbstractKafkaListener<JsonNode, DataHubUsageEventHook, String> {

  private final ObjectMapper objectMapper;
  private final Set<String> eventTypes;
  private final Set<String> aspectTypes;
  private final Set<String> userFilters;

  public DataHubUsageEventKafkaListener(
      ObjectMapper objectMapper, AuditEventExportConfiguration exportConfiguration) {
    this.objectMapper = objectMapper;
    if (StringUtils.isNotBlank(exportConfiguration.getUsageEventTypes())) {
      this.eventTypes = Set.of(exportConfiguration.getUsageEventTypes().split(","));
    } else {
      this.eventTypes = Collections.emptySet();
    }

    if (StringUtils.isNotBlank(exportConfiguration.getAspectTypes())) {
      this.aspectTypes = Set.of(exportConfiguration.getAspectTypes().split(","));
    } else {
      this.aspectTypes = Collections.emptySet();
    }

    if (StringUtils.isNotBlank(exportConfiguration.getUserFilters())) {
      this.userFilters = Set.of(exportConfiguration.getUserFilters().split(","));
    } else {
      this.userFilters = Collections.emptySet();
    }
  }

  @Override
  protected void setMDCContext(JsonNode event) {
    // No op for now
  }

  @Override
  protected boolean shouldSkipProcessing(JsonNode event) {
    // Usage source short circuit
    if (event.get(DataHubUsageEventConstants.USAGE_SOURCE) == null
        || !DataHubUsageEventConstants.BACKEND_SOURCE.equals(
            event.get(DataHubUsageEventConstants.USAGE_SOURCE).asText())) {
      return true;
    }

    // User filter short circuit
    if (event.get(DataHubUsageEventConstants.ACTOR_URN) != null
        && userFilters.contains(event.get(DataHubUsageEventConstants.ACTOR_URN).asText())) {
      return true;
    }

    // Event type or Aspect filter, only skip if it is neither a type match nor an aspect match
    return event.get(DataHubUsageEventConstants.TYPE) == null
        || (!eventTypes.contains(event.get(DataHubUsageEventConstants.TYPE).asText())
            && (event.get(DataHubUsageEventConstants.ASPECT_NAME) == null
                || !aspectTypes.contains(
                    event.get(DataHubUsageEventConstants.ASPECT_NAME).asText())));
  }

  @Override
  protected List<String> getFineGrainedLoggingAttributes(JsonNode event) {
    // No fine-grained logging implemented currently
    return Collections.emptyList();
  }

  @Override
  protected SystemMetadata getSystemMetadata(JsonNode event) {
    // If SystemMetadata is null then it won't actually create the span, create default for now
    // since no SystemMetadata
    // comes with the usage event.
    return SystemMetadataUtils.createDefaultSystemMetadata();
  }

  @Override
  protected String getEventDisplayString(JsonNode event) {
    return event.toPrettyString();
  }

  @Override
  public JsonNode convertRecord(@Nonnull String record) throws IOException {
    try {
      return objectMapper.readTree(record);
    } catch (Exception e) {
      log.error("Failed to parse event: {}", record);
      throw e;
    }
  }
}

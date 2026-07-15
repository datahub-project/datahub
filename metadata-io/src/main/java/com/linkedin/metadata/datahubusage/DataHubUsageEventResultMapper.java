package com.linkedin.metadata.datahubusage;

import com.linkedin.metadata.datahubusage.event.EventSource;
import com.linkedin.metadata.datahubusage.event.LoginSource;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

public final class DataHubUsageEventResultMapper {

  private DataHubUsageEventResultMapper() {}

  @Nonnull
  public static UsageEventResult fromSourceMap(
      @Nonnull OperationContext opContext, @Nonnull Map<String, ?> searchResultMap) {
    @SuppressWarnings("unchecked")
    Map<String, Object> cast = (Map<String, Object>) searchResultMap;
    InternalUsageEventResult.InternalUsageEventResultBuilder usageEventResultBuilder =
        InternalUsageEventResult.builder();
    usageEventResultBuilder.rawUsageEvent(new LinkedHashMap<>(cast));
    if (cast.get(DataHubUsageEventConstants.TYPE) instanceof String) {
      usageEventResultBuilder.eventType((String) cast.get(DataHubUsageEventConstants.TYPE));
    }
    if (cast.get(DataHubUsageEventConstants.ACTOR_URN) instanceof String) {
      usageEventResultBuilder.actorUrn((String) cast.get(DataHubUsageEventConstants.ACTOR_URN));
    }
    Object ts = cast.get(DataHubUsageEventConstants.TIMESTAMP);
    if (ts instanceof Number) {
      usageEventResultBuilder.timestamp(((Number) ts).longValue());
    }
    if (cast.get(DataHubUsageEventConstants.SOURCE_IP) instanceof String) {
      usageEventResultBuilder.sourceIP((String) cast.get(DataHubUsageEventConstants.SOURCE_IP));
    }
    if (cast.get(DataHubUsageEventConstants.EVENT_SOURCE) instanceof String) {
      usageEventResultBuilder.eventSource(
          EventSource.getSource((String) cast.get(DataHubUsageEventConstants.EVENT_SOURCE)));
    }
    if (cast.get(DataHubUsageEventConstants.LOGIN_SOURCE) instanceof String) {
      usageEventResultBuilder.loginSource(
          LoginSource.getSource((String) cast.get(DataHubUsageEventConstants.LOGIN_SOURCE)));
    }
    if (cast.get(DataHubUsageEventConstants.ENTITY_TYPE) instanceof String) {
      usageEventResultBuilder.entityType((String) cast.get(DataHubUsageEventConstants.ENTITY_TYPE));
    }
    if (cast.get(DataHubUsageEventConstants.ENTITY_URN) instanceof String) {
      usageEventResultBuilder.entityUrn((String) cast.get(DataHubUsageEventConstants.ENTITY_URN));
    }
    if (cast.get(DataHubUsageEventConstants.ASPECT_NAME) instanceof String) {
      usageEventResultBuilder.aspectName((String) cast.get(DataHubUsageEventConstants.ASPECT_NAME));
    }
    if (cast.get(DataHubUsageEventConstants.TRACE_ID) instanceof String) {
      usageEventResultBuilder.telemetryTraceId(
          (String) cast.get(DataHubUsageEventConstants.TRACE_ID));
    }
    if (cast.get(DataHubUsageEventConstants.USER_AGENT) instanceof String) {
      usageEventResultBuilder.userAgent((String) cast.get(DataHubUsageEventConstants.USER_AGENT));
    }
    InternalUsageEventResult usageEventResult = usageEventResultBuilder.build();
    return opContext.getObjectMapper().convertValue(usageEventResult, UsageEventResult.class);
  }
}

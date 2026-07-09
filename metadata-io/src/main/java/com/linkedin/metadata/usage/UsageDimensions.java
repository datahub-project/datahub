package com.linkedin.metadata.usage;

import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AttributionType;
import io.datahubproject.metadata.context.usage.AuthChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Builds low-cardinality dimension maps for rollup keys. */
public final class UsageDimensions {

  private UsageDimensions() {}

  @Nonnull
  public static Map<String, String> fromRequestContext(
      @Nonnull RequestContext requestContext,
      @Nullable String usageOperation,
      @Nullable String actorClassDimension) {
    Map<String, String> dimensions = new LinkedHashMap<>();
    if (usageOperation != null) {
      dimensions.put("usage_operation", usageOperation);
    }
    dimensions.put("request_api", requestContext.getRequestAPI().toMetricLabel());
    dimensions.put("agent_class", requestContext.getAgentClass().toMetricLabel());
    AuthChannel authChannel =
        Optional.ofNullable(requestContext.getAuthChannel()).orElse(AuthChannel.UNKNOWN);
    dimensions.put("auth_channel", authChannel.dimensionValue());
    if (actorClassDimension != null) {
      dimensions.put("actor_class", actorClassDimension);
    }
    return Map.copyOf(dimensions);
  }

  @Nonnull
  public static AttributionType resolveAttribution(@Nonnull RequestContext requestContext) {
    return AttributionType.fromAgentClass(requestContext.getAgentClass());
  }
}

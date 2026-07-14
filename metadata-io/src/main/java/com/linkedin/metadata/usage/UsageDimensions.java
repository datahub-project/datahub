package com.linkedin.metadata.usage;

import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AttributionType;
import io.datahubproject.metadata.context.usage.AuthChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Builds low-cardinality dimension maps for rollup keys. */
public final class UsageDimensions {

  public static final String USAGE_OPERATION = "usage_operation";
  public static final String REQUEST_API = "request_api";
  public static final String AGENT_CLASS = "agent_class";
  public static final String AUTH_CHANNEL = "auth_channel";
  public static final String INGESTION_RUNNER = "ingestion_runner";
  public static final String ACTOR_CLASS = "actor_class";

  /**
   * Preferred key order when serializing dimension maps (access-channel keys first, then {@link
   * #ACTOR_CLASS}).
   */
  public static final List<String> STABLE_KEY_ORDER =
      List.of(
          USAGE_OPERATION, REQUEST_API, AGENT_CLASS, AUTH_CHANNEL, INGESTION_RUNNER, ACTOR_CLASS);

  private UsageDimensions() {}

  @Nonnull
  public static Map<String, String> fromRequestContext(
      @Nonnull RequestContext requestContext,
      @Nullable String usageOperation,
      @Nullable String actorClassDimension) {
    Map<String, String> dimensions = new LinkedHashMap<>();
    if (usageOperation != null) {
      dimensions.put(USAGE_OPERATION, usageOperation);
    }
    dimensions.put(REQUEST_API, requestContext.getRequestAPI().toMetricLabel());
    dimensions.put(AGENT_CLASS, requestContext.getAgentClass().toMetricLabel());
    AuthChannel authChannel =
        Optional.ofNullable(requestContext.getAuthChannel()).orElse(AuthChannel.UNKNOWN);
    dimensions.put(AUTH_CHANNEL, authChannel.dimensionValue());
    if (actorClassDimension != null) {
      dimensions.put(ACTOR_CLASS, actorClassDimension);
    }
    return Map.copyOf(dimensions);
  }

  @Nonnull
  public static AttributionType resolveAttribution(@Nonnull RequestContext requestContext) {
    return AttributionType.fromAgentClass(requestContext.getAgentClass());
  }
}

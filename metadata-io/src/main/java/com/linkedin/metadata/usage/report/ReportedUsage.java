package com.linkedin.metadata.usage.report;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.usage.UsageDimensions;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Records externally reported usage into {@link UsageAggregationStore} using the same dimension
 * vocabulary as HTTP usage tracking.
 *
 * <p>Taxonomy and API allowlisting are enforced via {@code usage_operations.yaml} when the store
 * resolves the operation. Which metrics increment is controlled by {@code emit_when: reported} in
 * the metric registry.
 *
 * <p>This is metering, not access control: builds an attributed {@link RequestContext} and records
 * via {@link UsageAggregationStore#recordReportedUsage}. Reports are never rejected because an
 * attributed actor is missing, removed, or suspended.
 */
@Slf4j
public final class ReportedUsage {

  /** Report-payload keys (not rollup dimensions — see {@link UsageDimensions}). */
  public static final String PROP_ACTOR_URN = "actor_urn";

  public static final String PROP_USAGE_IDENTITY = "usage_identity";
  public static final String PROP_USER_AGENT = "user_agent";

  private ReportedUsage() {}

  /**
   * Records {@code quantity} for a report-driven event. Builds an attributed {@link RequestContext}
   * from {@code properties}. Requires {@link UsageDimensions#USAGE_OPERATION} and {@link
   * UsageDimensions#REQUEST_API}; the store rejects unknown operations or API mismatches against
   * {@code usage_operations.yaml}.
   *
   * @return false when required props are missing, quantity is non-positive, or the store rejects
   *     the report
   */
  public static boolean record(
      @Nonnull UsageAggregationStore usageAggregationStore,
      @Nonnull OperationContext systemOperationContext,
      long quantity,
      @Nonnull Map<String, Object> properties) {
    if (quantity <= 0) {
      return false;
    }

    String usageOperation = stringProp(properties, UsageDimensions.USAGE_OPERATION);
    if (usageOperation == null || usageOperation.isBlank()) {
      return false;
    }

    RequestContext.RequestAPI requestApi =
        parseRequestApi(stringProp(properties, UsageDimensions.REQUEST_API));
    if (requestApi == null) {
      return false;
    }

    String actorUrn = firstNonBlank(properties, PROP_ACTOR_URN, PROP_USAGE_IDENTITY);
    if (actorUrn == null || actorUrn.isBlank()) {
      // Stable unknown — UsageActorClassResolver classifies as regular (not system session).
      actorUrn = Constants.UNKNOWN_ACTOR;
    }
    String usageIdentity = firstNonBlank(properties, PROP_USAGE_IDENTITY, PROP_ACTOR_URN);
    if (usageIdentity == null || usageIdentity.isBlank()) {
      usageIdentity = actorUrn;
    }

    String userAgent = stringProp(properties, PROP_USER_AGENT);
    if (userAgent == null) {
      userAgent = "";
    }
    AuthChannel authChannel =
        parseAuthChannel(stringProp(properties, UsageDimensions.AUTH_CHANNEL));

    // Omit metricUtils: RequestContext construction would otherwise emit request-count Micrometer.
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(actorUrn)
            .sourceIP("0.0.0.0")
            .requestAPI(requestApi)
            .requestID("reported_usage:" + usageOperation)
            .userAgent(userAgent)
            .usageOperation(usageOperation)
            .usageIdentity(usageIdentity)
            .authChannel(authChannel)
            .build();

    boolean recorded =
        usageAggregationStore.recordReportedUsage(systemOperationContext, requestContext, quantity);
    if (recorded) {
      log.debug(
          "Recorded reported usage quantity={} operation={} requestApi={}",
          quantity,
          usageOperation,
          requestApi);
    }
    return recorded;
  }

  @Nullable
  private static RequestContext.RequestAPI parseRequestApi(@Nullable String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return RequestContext.RequestAPI.valueOf(raw.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  @Nonnull
  private static AuthChannel parseAuthChannel(@Nullable String raw) {
    if (raw == null || raw.isBlank()) {
      return AuthChannel.UNKNOWN;
    }
    try {
      return AuthChannel.valueOf(raw.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return AuthChannel.UNKNOWN;
    }
  }

  @Nullable
  private static String firstNonBlank(
      @Nonnull Map<String, Object> properties, @Nonnull String... keys) {
    for (String key : keys) {
      String value = stringProp(properties, key);
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  @Nullable
  private static String stringProp(@Nonnull Map<String, Object> properties, @Nonnull String key) {
    Object value = properties.get(key);
    if (value == null) {
      return null;
    }
    String asString = String.valueOf(value).trim();
    return asString.isEmpty() ? null : asString;
  }
}

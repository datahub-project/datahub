package com.linkedin.datahub.graphql.resolvers.assertion;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionHealth;
import com.linkedin.datahub.graphql.generated.AssertionHealthStatus;
import com.linkedin.datahub.graphql.generated.AssertionResultError;
import com.linkedin.datahub.graphql.generated.AssertionRunEvent;
import com.linkedin.datahub.graphql.generated.AssertionRunStatus;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.MonitorError;
import com.linkedin.datahub.graphql.generated.MonitorErrorType;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.types.dataset.mappers.AssertionRunEventMapper;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.AssertionHealthUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionHealthResolver implements DataFetcher<CompletableFuture<AssertionHealth>> {

  private static final Set<String> MONITOR_ASPECTS =
      ImmutableSet.of(Constants.MONITOR_KEY_ASPECT_NAME, Constants.MONITOR_INFO_ASPECT_NAME);

  private static final Set<String> SOURCE_ERROR_PERMANENT_HINTS =
      ImmutableSet.of(
          "permission",
          "insufficient privileges",
          "access control",
          "forbidden",
          "unauthorized",
          "authentication",
          "authorization",
          "access denied",
          "not found",
          "does not exist",
          "invalid",
          "syntax",
          "parse",
          "expected one",
          "non-numeric",
          "unable to retrieve valid connection");

  private static final Set<String> SOURCE_ERROR_TRANSIENT_HINTS =
      ImmutableSet.of(
          "timeout",
          "timed out",
          "temporary",
          "temporarily",
          "unavailable",
          "service unavailable",
          "internal server error",
          "rate limit",
          "too many requests",
          "connection reset",
          "connection refused",
          "network error",
          "try again");

  private static final Pattern HTTP_STATUS_PATTERN = Pattern.compile("(^|\\D)([45]\\d{2})(\\D|$)");

  private static final Set<String> SQLSTATE_ERROR_CLASSES = ImmutableSet.of("28", "42", "22", "23");
  private static final Set<String> SQLSTATE_TRANSIENT_CLASSES =
      ImmutableSet.of("08", "53", "57", "58");

  private final EntityClient _entityClient;
  private final AssertionService _assertionService;

  public AssertionHealthResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final AssertionService assertionService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _assertionService =
        Objects.requireNonNull(assertionService, "assertionService must not be null");
  }

  @Override
  public CompletableFuture<AssertionHealth> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Assertion assertion = (Assertion) environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> resolveHealth(context, assertion), this.getClass().getSimpleName(), "get");
  }

  @Nonnull
  private AssertionHealth resolveHealth(
      @Nonnull QueryContext context, @Nonnull Assertion assertion) {
    final Urn assertionUrn = UrnUtils.getUrn(assertion.getUrn());
    final MonitorDetails monitorDetails = fetchMonitorDetails(context, assertionUrn);
    final AssertionRunEvent latestRunEvent = fetchLatestRunEvent(context, assertionUrn);

    final MonitorError monitorError = monitorDetails.getError();
    final AssertionResultError evaluationError =
        latestRunEvent != null && latestRunEvent.getResult() != null
            ? latestRunEvent.getResult().getError()
            : null;
    final Long lastRunAt = latestRunEvent != null ? latestRunEvent.getTimestampMillis() : null;

    final AssertionHealthStatus monitorStatus = mapMonitorErrorToStatus(monitorError);
    final AssertionHealthStatus evaluationStatus = mapEvaluationErrorToStatus(evaluationError);
    final AssertionHealthStatus resolvedStatus =
        resolveStatus(
            monitorStatus,
            evaluationStatus,
            monitorDetails.isMonitorPresent(),
            latestRunEvent != null);

    final String displayMessage =
        resolveDisplayMessage(monitorError, evaluationError, monitorStatus, evaluationStatus);
    final String recommendedAction =
        resolveRecommendedAction(monitorError, evaluationError, monitorStatus, evaluationStatus);
    final String rawErrorMessage = resolveRawErrorMessage(monitorError, evaluationError);

    final AssertionHealth health = new AssertionHealth();
    health.setStatus(resolvedStatus);
    if (lastRunAt != null) {
      health.setLastRunAt(lastRunAt);
    }
    if (displayMessage != null) {
      health.setDisplayMessage(displayMessage);
    }
    if (recommendedAction != null) {
      health.setRecommendedAction(recommendedAction);
    }
    if (rawErrorMessage != null) {
      health.setRawErrorMessage(rawErrorMessage);
    }
    return health;
  }

  @Nullable
  private String resolveRawErrorMessage(
      @Nullable MonitorError monitorError, @Nullable AssertionResultError evaluationError) {
    if (monitorError != null && monitorError.getMessage() != null) {
      return monitorError.getMessage();
    }
    if (evaluationError != null) {
      return getPropertyValue(evaluationError, "message");
    }
    return null;
  }

  @Nonnull
  private MonitorDetails fetchMonitorDetails(
      @Nonnull QueryContext context, @Nonnull Urn assertionUrn) {
    try {
      final Urn monitorUrn =
          _assertionService.getMonitorUrnForAssertion(context.getOperationContext(), assertionUrn);
      if (monitorUrn == null) {
        return MonitorDetails.empty();
      }
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.MONITOR_ENTITY_NAME,
              monitorUrn,
              MONITOR_ASPECTS,
              false);
      if (entityResponse == null) {
        return MonitorDetails.present(null);
      }
      final Monitor monitor = MonitorMapper.map(context, entityResponse);
      if (monitor == null
          || monitor.getInfo() == null
          || monitor.getInfo().getStatus() == null
          || monitor.getInfo().getStatus().getError() == null) {
        return MonitorDetails.present(null);
      }
      return MonitorDetails.present(monitor.getInfo().getStatus().getError());
    } catch (Exception e) {
      log.warn("Failed to resolve monitor error for assertion {}", assertionUrn, e);
      return MonitorDetails.empty();
    }
  }

  @Nullable
  private AssertionRunEvent fetchLatestRunEvent(
      @Nonnull QueryContext context, @Nonnull Urn assertionUrn) {
    try {
      final List<EnvelopedAspect> aspects =
          _entityClient.getTimeseriesAspectValues(
              context.getOperationContext(),
              assertionUrn.toString(),
              Constants.ASSERTION_ENTITY_NAME,
              Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
              null,
              null,
              1,
              AssertionRunEventResolver.buildFilter(
                  null,
                  AssertionRunStatus.COMPLETE.toString(),
                  context.getOperationContext().getAspectRetriever()));
      if (aspects == null || aspects.isEmpty()) {
        return null;
      }
      return AssertionRunEventMapper.map(context, aspects.get(0));
    } catch (Exception e) {
      log.warn("Failed to resolve latest run event for assertion {}", assertionUrn, e);
      return null;
    }
  }

  @Nonnull
  private AssertionHealthStatus mapMonitorErrorToStatus(@Nullable MonitorError monitorError) {
    final com.linkedin.monitor.MonitorError modelError = toModelMonitorError(monitorError);
    final AssertionHealthUtils.MonitorHealthStatus status =
        AssertionHealthUtils.mapMonitorErrorToStatus(modelError);
    switch (status) {
      case HEALTHY:
        return AssertionHealthStatus.HEALTHY;
      case DEGRADED:
        return AssertionHealthStatus.DEGRADED;
      case ERROR:
        return AssertionHealthStatus.ERROR;
      case UNKNOWN:
        return AssertionHealthStatus.UNKNOWN;
      default:
        throw new IllegalStateException(String.format("Unhandled monitor error status %s", status));
    }
  }

  @Nullable
  private com.linkedin.monitor.MonitorError toModelMonitorError(
      @Nullable MonitorError monitorError) {
    if (monitorError == null) {
      return null;
    }
    final com.linkedin.monitor.MonitorError modelError = new com.linkedin.monitor.MonitorError();
    final MonitorErrorType type = monitorError.getType();
    if (type != null) {
      try {
        modelError.setType(com.linkedin.monitor.MonitorErrorType.valueOf(type.name()));
      } catch (IllegalArgumentException e) {
        return modelError;
      }
    }
    return modelError;
  }

  @Nonnull
  private AssertionHealthStatus mapEvaluationErrorToStatus(
      @Nullable AssertionResultError evaluationError) {
    if (evaluationError == null) {
      return AssertionHealthStatus.HEALTHY;
    }
    if (evaluationError.getType() == null) {
      return AssertionHealthStatus.UNKNOWN;
    }
    switch (evaluationError.getType()) {
        /* These errors are special, sometimes they are transient due to source
         * availability issues, and sometimes they are permanent due to source
         * configuration issues.
         */
      case SOURCE_CONNECTION_ERROR:
      case SOURCE_QUERY_FAILED:
        return mapSourceErrorToStatus(evaluationError);
      case INSUFFICIENT_DATA:
      case STATE_PERSISTENCE_FAILED:
      case METRIC_PERSISTENCE_FAILED:
      case RESULT_EMISSION_FAILED:
        return AssertionHealthStatus.DEGRADED;
      case INVALID_PARAMETERS:
      case INVALID_SOURCE_TYPE:
      case UNSUPPORTED_PLATFORM:
      case CUSTOM_SQL_ERROR:
      case FIELD_ASSERTION_ERROR:
      case MISSING_EVALUATION_PARAMETERS:
      case EVALUATOR_NOT_FOUND:
      case METRIC_RESOLVER_UNSUPPORTED_METRIC:
      case METRIC_RESOLVER_INVALID_SOURCE_TYPE:
      case UNKNOWN_ERROR:
        return AssertionHealthStatus.ERROR;
    }
    throw new IllegalStateException(
        String.format("Unhandled assertion result error type %s", evaluationError.getType()));
  }

  @Nonnull
  private AssertionHealthStatus mapSourceErrorToStatus(
      @Nonnull AssertionResultError evaluationError) {
    final Integer responseCode = getResponseCode(evaluationError);
    if (responseCode != null) {
      if (responseCode == 408 || responseCode == 429 || responseCode >= 500) {
        return AssertionHealthStatus.DEGRADED;
      }
      if (responseCode >= 400) {
        return AssertionHealthStatus.ERROR;
      }
    }

    final AssertionHealthStatus sqlStateStatus = mapSqlStateToStatus(evaluationError);
    if (sqlStateStatus != null) {
      return sqlStateStatus;
    }

    final String message = getPropertyValue(evaluationError, "message");
    if (message != null) {
      final String normalized = message.toLowerCase(Locale.ROOT);
      final Integer statusCode = extractHttpStatus(normalized);
      if (statusCode != null) {
        if (statusCode == 408 || statusCode == 429 || statusCode >= 500) {
          return AssertionHealthStatus.DEGRADED;
        }
        if (statusCode >= 400) {
          return AssertionHealthStatus.ERROR;
        }
      }
      if (containsAny(normalized, SOURCE_ERROR_PERMANENT_HINTS)) {
        return AssertionHealthStatus.ERROR;
      }
      if (containsAny(normalized, SOURCE_ERROR_TRANSIENT_HINTS)) {
        return AssertionHealthStatus.DEGRADED;
      }
    }
    return AssertionHealthStatus.DEGRADED;
  }

  @Nullable
  private Integer extractHttpStatus(@Nonnull String message) {
    final Matcher matcher = HTTP_STATUS_PATTERN.matcher(message);
    if (!matcher.find()) {
      return null;
    }
    try {
      return Integer.parseInt(matcher.group(2));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Nullable
  private Integer getResponseCode(@Nonnull AssertionResultError evaluationError) {
    final String responseCode = getPropertyValue(evaluationError, "response_code");
    if (responseCode == null) {
      return null;
    }
    try {
      return Integer.parseInt(responseCode);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Nullable
  private AssertionHealthStatus mapSqlStateToStatus(@Nonnull AssertionResultError evaluationError) {
    final String sqlState = getPropertyValue(evaluationError, "sqlstate");
    if (sqlState == null) {
      return null;
    }
    final String normalized = sqlState.trim().toUpperCase(Locale.ROOT);
    if (normalized.length() < 2) {
      return null;
    }
    final String sqlClass = normalized.substring(0, 2);
    if (SQLSTATE_ERROR_CLASSES.contains(sqlClass)) {
      return AssertionHealthStatus.ERROR;
    }
    if (SQLSTATE_TRANSIENT_CLASSES.contains(sqlClass)) {
      return AssertionHealthStatus.DEGRADED;
    }
    return null;
  }

  private boolean containsAny(@Nonnull String message, @Nonnull Set<String> hints) {
    for (String hint : hints) {
      if (message.contains(hint)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private String getPropertyValue(
      @Nonnull AssertionResultError evaluationError, @Nonnull String key) {
    if (evaluationError.getProperties() == null) {
      return null;
    }
    for (StringMapEntry entry : evaluationError.getProperties()) {
      if (entry != null && key.equals(entry.getKey())) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Nonnull
  private AssertionHealthStatus resolveStatus(
      @Nonnull AssertionHealthStatus monitorStatus,
      @Nonnull AssertionHealthStatus evaluationStatus,
      boolean monitorPresent,
      boolean runEventPresent) {
    if (monitorStatus == AssertionHealthStatus.ERROR
        || evaluationStatus == AssertionHealthStatus.ERROR) {
      return AssertionHealthStatus.ERROR;
    }
    if (monitorStatus == AssertionHealthStatus.DEGRADED
        || evaluationStatus == AssertionHealthStatus.DEGRADED) {
      return AssertionHealthStatus.DEGRADED;
    }
    if (monitorStatus == AssertionHealthStatus.HEALTHY
        || evaluationStatus == AssertionHealthStatus.HEALTHY) {
      return AssertionHealthStatus.HEALTHY;
    }
    if (monitorPresent || runEventPresent) {
      return AssertionHealthStatus.HEALTHY;
    }
    return AssertionHealthStatus.UNKNOWN;
  }

  @Nullable
  private String resolveRecommendedAction(
      @Nullable MonitorError monitorError,
      @Nullable AssertionResultError evaluationError,
      @Nonnull AssertionHealthStatus monitorStatus,
      @Nonnull AssertionHealthStatus evaluationStatus) {
    final int evaluationSeverity = severity(evaluationStatus);
    final int monitorSeverity = severity(monitorStatus);

    if (evaluationError != null && evaluationSeverity >= monitorSeverity) {
      return AssertionErrorMessageMapper.recommendedActionForEvaluationError(
          evaluationError.getType());
    }
    if (monitorError != null) {
      return AssertionErrorMessageMapper.recommendedActionForMonitorError(monitorError.getType());
    }
    return null;
  }

  @Nullable
  private String resolveDisplayMessage(
      @Nullable MonitorError monitorError,
      @Nullable AssertionResultError evaluationError,
      @Nonnull AssertionHealthStatus monitorStatus,
      @Nonnull AssertionHealthStatus evaluationStatus) {
    final int evaluationSeverity = severity(evaluationStatus);
    final int monitorSeverity = severity(monitorStatus);

    if (evaluationError != null && evaluationSeverity >= monitorSeverity) {
      return AssertionErrorMessageMapper.displayMessageForEvaluationError(
          evaluationError.getType());
    }
    if (monitorError != null) {
      return AssertionErrorMessageMapper.displayMessageForMonitorError(monitorError.getType());
    }
    return null;
  }

  private int severity(@Nonnull AssertionHealthStatus status) {
    switch (status) {
      case ERROR:
        return 3;
      case DEGRADED:
        return 2;
      case HEALTHY:
        return 1;
      default:
        return 0;
    }
  }

  @Value
  private static class MonitorDetails {
    @Nullable MonitorError error;
    boolean monitorPresent;

    static MonitorDetails empty() {
      return new MonitorDetails(null, false);
    }

    static MonitorDetails present(@Nullable MonitorError error) {
      return new MonitorDetails(error, true);
    }
  }
}

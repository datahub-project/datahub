package com.linkedin.datahub.graphql.resolvers.monitor;

import com.google.common.collect.ImmutableList;
import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionMetric;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Utility class for building MonitorAnomalyEvent instances. */
public class MonitorAnomalyEventUtils {

  /**
   * Builds a MonitorAnomalyEvent from an AssertionRunEvent.
   *
   * @param assertionUrn the URN of the assertion
   * @param runEvent the assertion run event
   * @param state the anomaly review state
   * @param nowMillis the current timestamp in milliseconds
   * @return the constructed MonitorAnomalyEvent
   */
  @Nonnull
  public static MonitorAnomalyEvent buildMonitorAnomalyEvent(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AnomalyReviewState state,
      @Nonnull final Long nowMillis) {

    final long runEventTimestampMillis = runEvent.getTimestampMillis();

    // Create anomaly source properties
    final AnomalySourceProperties anomalySourceProperties = new AnomalySourceProperties();
    if (runEvent.hasResult() && runEvent.getResult().hasMetric()) {
      anomalySourceProperties.setAssertionMetric(
          new AssertionMetric(runEvent.getResult().getMetric().data()));
    }

    // Create anomaly source
    final AnomalySource anomalySource = new AnomalySource();
    anomalySource.setSourceUrn(assertionUrn);
    anomalySource.setSourceEventTimestampMillis(runEventTimestampMillis);
    anomalySource.setType(AnomalySourceType.USER_FEEDBACK);
    anomalySource.setProperties(anomalySourceProperties);

    // Create anomaly event
    final TimeStamp now = new TimeStamp().setTime(nowMillis);
    final MonitorAnomalyEvent anomalyEvent = new MonitorAnomalyEvent();
    anomalyEvent.setTimestampMillis(nowMillis);
    anomalyEvent.setState(state);
    anomalyEvent.setSource(anomalySource);
    anomalyEvent.setCreated(now);
    anomalyEvent.setLastUpdated(now);

    return anomalyEvent;
  }

  /** Builds a map from source event timestamp to its latest anomaly event. */
  @Nonnull
  public static Map<Long, MonitorAnomalyEvent> buildAnomalyEventMapBySourceEvent(
      @Nonnull List<MonitorAnomalyEvent> anomalyEvents) {
    final Map<Long, MonitorAnomalyEvent> anomalyEventMap = new HashMap<>();

    for (MonitorAnomalyEvent anomalyEvent : anomalyEvents) {
      final Long sourceEventTimestampMillis =
          anomalyEvent.getSource().getSourceEventTimestampMillis();
      if (sourceEventTimestampMillis == null) {
        continue;
      }
      final MonitorAnomalyEvent maybeExistingEvent =
          anomalyEventMap.get(sourceEventTimestampMillis);
      // If an anomaly event already exists for this timestamp, skip if the new one is NOT more
      // recent
      if (maybeExistingEvent != null
          && anomalyEvent.getTimestampMillis() <= maybeExistingEvent.getTimestampMillis()) {
        continue;
      }
      // Use the timestamp as the key to map anomaly events to run events
      anomalyEventMap.put(sourceEventTimestampMillis, anomalyEvent);
    }

    return anomalyEventMap;
  }

  /** Builds a map from assertion metric timestamp to its latest anomaly event. */
  @Nonnull
  public static Map<Long, MonitorAnomalyEvent> buildAnomalyEventMapByMetric(
      @Nonnull List<MonitorAnomalyEvent> anomalyEvents) {
    final Map<Long, MonitorAnomalyEvent> anomalyEventMap = new HashMap<>();

    for (MonitorAnomalyEvent anomalyEvent : anomalyEvents) {
      if (anomalyEvent.getSource().hasProperties()
          && anomalyEvent.getSource().getProperties().hasAssertionMetric()
          && anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs()
              == null) {
        continue;
      }
      final MonitorAnomalyEvent maybeExistingEvent =
          anomalyEventMap.get(
              anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs());
      // If an anomaly event already exists for this timestamp, skip if the new one is NOT more
      // recent
      if (maybeExistingEvent != null
          && anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs()
              <= maybeExistingEvent
                  .getSource()
                  .getProperties()
                  .getAssertionMetric()
                  .getTimestampMs()) {
        continue;
      }
      // Use the timestamp as the key to map anomaly events to metrics
      anomalyEventMap.put(
          anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs(),
          anomalyEvent);
    }

    return anomalyEventMap;
  }

  /**
   * Gets the latest anomaly event timestamp for a given monitor.
   *
   * @param monitorUrn the URN of the monitor
   * @param operationContext the operation context
   * @param client the entity client
   * @return the latest anomaly event timestamp in milliseconds
   * @throws RemoteInvocationException if fetching fails
   */
  @Nonnull
  public static long getLatestAnomalyEventTimestamp(
      @Nonnull final OperationContext operationContext,
      @Nonnull final EntityClient client,
      @Nonnull final Urn monitorUrn)
      throws RemoteInvocationException {
    final List<EnvelopedAspect> aspects =
        client.getTimeseriesAspectValues(
            operationContext,
            monitorUrn.toString(),
            Constants.MONITOR_ENTITY_NAME,
            Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME,
            null,
            null,
            1, // limit
            null);
    if (aspects.isEmpty()) {
      return 0;
    }
    return GenericRecordUtils.deserializeAspect(
            aspects.get(0).getAspect().getValue(),
            aspects.get(0).getAspect().getContentType(),
            MonitorAnomalyEvent.class)
        .getTimestampMillis();
  }

  /**
   * Fetches anomaly events for a given monitor within the specified time range.
   *
   * @param operationContext the operation context
   * @param client the entity client
   * @param monitorUrn the URN of the monitor
   * @param maybeStartTimeMillis optional start time in milliseconds
   * @param maybeEndTimeMillis optional end time in milliseconds
   * @return list of anomaly events
   * @throws RemoteInvocationException if fetching fails
   */
  @Nonnull
  public static List<MonitorAnomalyEvent> fetchAnomalyEvents(
      @Nonnull OperationContext operationContext,
      @Nonnull EntityClient client,
      @Nonnull String monitorUrn,
      @Nullable Long maybeStartTimeMillis,
      @Nullable Long maybeEndTimeMillis)
      throws RemoteInvocationException {

    final List<EnvelopedAspect> aspects =
        client.getTimeseriesAspectValues(
            operationContext,
            monitorUrn,
            Constants.MONITOR_ENTITY_NAME,
            Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME,
            null,
            null,
            null, // no limit to capture all anomaly events in the time range
            maybeStartTimeMillis != null || maybeEndTimeMillis != null
                ? buildAnomalyFeedbackEventsFilter(maybeStartTimeMillis, maybeEndTimeMillis)
                : null,
            new SortCriterion().setField("timestampMillis").setOrder(SortOrder.DESCENDING));

    return aspects.stream()
        .map(
            aspect ->
                GenericRecordUtils.deserializeAspect(
                    aspect.getAspect().getValue(),
                    aspect.getAspect().getContentType(),
                    MonitorAnomalyEvent.class))
        .toList();
  }

  /**
   * Builds a filter for anomaly feedback events within the specified time range.
   *
   * @param maybeStartTimeMillis optional start time in milliseconds
   * @param maybeEndTimeMillis optional end time in milliseconds
   * @return the constructed filter
   */
  @Nonnull
  public static Filter buildAnomalyFeedbackEventsFilter(
      @Nullable Long maybeStartTimeMillis, @Nullable Long maybeEndTimeMillis) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            ImmutableList.of(
                                new Criterion()
                                    .setField("sourceEventTimestampMillis")
                                    .setCondition(Condition.BETWEEN)
                                    .setValues(
                                        new StringArray(
                                            List.of(
                                                maybeStartTimeMillis != null
                                                    ? maybeStartTimeMillis.toString()
                                                    : "0",
                                                maybeEndTimeMillis != null
                                                    ? maybeEndTimeMillis.toString()
                                                    : String.valueOf(Long.MAX_VALUE)))))))));
  }
}

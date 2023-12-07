package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface TimeseriesAspectService {

  /** Configure the Time-Series aspect service one time at boot-up. */
  void configure();

  /**
   * Count the number of entries using a filter
   *
   * @param entityName the name of the entity to count entries for
   * @param aspectName the name of the timeseries aspect to count for that entity
   * @param filter the filter to apply to the count
   * @return The count of the number of entries that match the filter
   */
  public long countByFilter(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nullable final Filter filter);

  /**
   * Retrieve a list of Time-Series Aspects for an individual entity, matching a set of optional
   * filters, sorted by the timestampMillis field descending.
   *
   * <p>This method allows you to optionally filter for events that fall into a particular time
   * window based on the timestampMillis field of the aspect, or simply retrieve the latest aspects
   * sorted by time.
   *
   * <p>Note that this does not always indicate the event time, and is often used to reflect the
   * reported time of a given event.
   *
   * @param urn the urn of the entity to retrieve aspects for
   * @param entityName the name of the entity to retrieve aspects for
   * @param aspectName the name of the timeseries aspect to retrieve for the entity
   * @param startTimeMillis the start of a time window in milliseconds, compared against the
   *     standard timestampMillis field
   * @param endTimeMillis the end of a time window in milliseconds, compared against the standard
   *     timestampMillis field
   * @param limit the maximum number of results to retrieve
   * @param filter a set of additional secondary filters to apply when finding the aspects
   * @return a list of {@link EnvelopedAspect} containing the Time-Series aspects that were found,
   *     or empty list if none were found.
   */
  @Nonnull
  default List<EnvelopedAspect> getAspectValues(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis,
      @Nullable final Integer limit,
      @Nullable final Filter filter) {
    return getAspectValues(
        urn, entityName, aspectName, startTimeMillis, endTimeMillis, limit, filter, null);
  }

  /**
   * Retrieve a list of Time-Series Aspects for an individual entity, matching a set of optional
   * filters, sorted by the timestampMillis field descending.
   *
   * <p>This method allows you to optionally filter for events that fall into a particular time
   * window based on the timestampMillis field of the aspect, or simply retrieve the latest aspects
   * sorted by time.
   *
   * <p>Note that this does not always indicate the event time, and is often used to reflect the
   * reported time of a given event.
   *
   * @param urn the urn of the entity to retrieve aspects for
   * @param entityName the name of the entity to retrieve aspects for
   * @param aspectName the name of the timeseries aspect to retrieve for the entity
   * @param startTimeMillis the start of a time window in milliseconds, compared against the
   *     standard timestampMillis field
   * @param endTimeMillis the end of a time window in milliseconds, compared against the standard
   *     timestampMillis field
   * @param limit the maximum number of results to retrieve
   * @param filter a set of additional secondary filters to apply when finding the aspects
   * @param sort the sort criterion for the result set. If not provided, defaults to sorting by
   *     timestampMillis descending.
   * @return a list of {@link EnvelopedAspect} containing the Time-Series aspects that were found,
   *     or empty list if none were found.
   */
  @Nonnull
  List<EnvelopedAspect> getAspectValues(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis,
      @Nullable final Integer limit,
      @Nullable final Filter filter,
      @Nullable final SortCriterion sort);

  /**
   * Perform a arbitrary aggregation query over a set of Time-Series aspects. This is used to answer
   * arbitrary questions about the Time-Series aspects that we have.
   *
   * @param entityName the name of the entity associated with the Time-Series aspect.
   * @param aspectName the name of the Time-Series aspect.
   * @param aggregationSpecs a specification of the types of metric-value aggregations that should
   *     be performed
   * @param filter an optional filter that should be applied prior to performing the requested
   *     aggregations.
   * @param groupingBuckets an optional set of buckets to group the aggregations on the timeline --
   *     For example, by a particular date or string value.
   * @return a "table" representation of the results of performing the aggregation, with a row per
   *     group.
   */
  @Nonnull
  GenericTable getAggregatedStats(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final AggregationSpec[] aggregationSpecs,
      @Nullable final Filter filter,
      @Nullable final GroupingBucket[] groupingBuckets);

  /**
   * Generic filter based deletion for Time-Series Aspects.
   *
   * @param entityName The name of the entity.
   * @param aspectName The name of the aspect.
   * @param filter A filter to be used for deletion of the documents on the index.
   * @return a summary of the aspects which were deleted
   */
  @Nonnull
  DeleteAspectValuesResult deleteAspectValues(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final Filter filter);

  /**
   * Generic filter based deletion for Time-Series Aspects.
   *
   * @param entityName The name of the entity.
   * @param aspectName The name of the aspect.
   * @param filter A filter to be used for deletion of the documents on the index.
   * @param options Options to control delete parameters
   * @return The Job ID of the deletion operation
   */
  @Nonnull
  String deleteAspectValuesAsync(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final Filter filter,
      @Nonnull final BatchWriteOperationsOptions options);

  /**
   * Reindex the index represented by entityName and aspect name, applying the filter
   *
   * @param entityName The name of the entity.
   * @param aspectName The name of the aspect.
   * @param filter A filter to be used when reindexing
   * @param options Options to control reindex parameters
   * @return The Job ID of the reindex operation
   */
  String reindexAsync(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options);

  /**
   * Rollback the Time-Series aspects associated with a particular runId. This is invoked as a part
   * of an ingestion rollback process.
   *
   * @param runId The runId that needs to be rolled back.
   * @return a summary of the aspects which were deleted
   */
  @Nonnull
  DeleteAspectValuesResult rollbackTimeseriesAspects(@Nonnull final String runId);

  /**
   * Upsert a raw timeseries aspect into a timeseries index. Note that this is a bit of a hack, and
   * leaks too much implementation detail around Elasticsearch.
   *
   * <p>TODO: Make this more general purpose.
   *
   * @param entityName the name of the entity
   * @param aspectName the name of an aspect
   * @param docId the doc id for the elasticsearch document - this serves as the primary key for the
   *     document.
   * @param document the raw document to insert.
   */
  void upsertDocument(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final String docId,
      @Nonnull final JsonNode document);

  List<TimeseriesIndexSizeResult> getIndexSizes();
}

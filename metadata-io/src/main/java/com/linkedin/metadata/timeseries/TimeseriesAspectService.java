package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public interface TimeseriesAspectService {

  void configure();

  void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull String docId,
      @Nonnull JsonNode document);

  List<EnvelopedAspect> getAspectValues(@Nonnull final Urn urn, @Nonnull String entityName, @Nonnull String aspectName,
      @Nullable Long startTimeMillis, @Nullable Long endTimeMillis, @Nullable Integer limit,
      @Nullable Boolean getLatestValue, @Nullable Filter filter);

  /**
   * Get the aggregated metrics for the given dataset or column from a time series aspect.
   */
  @Nonnull
  GenericTable getAggregatedStats(@Nonnull String entityName, @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs, @Nullable Filter filter, @Nullable GroupingBucket[] groupingBuckets);

  /**
   * Generic filter based deletion for timseries aspects.
   * @param entityName - The name of the entity.
   * @param aspectName  - The name of the aspect.
   * @param filter - The filter to be used for deletion of the documents on the index.
   * @return - number of documents deleted.
   */
  @Nonnull
  DeleteAspectValuesResult deleteAspectValues(@Nonnull String entityName, @Nonnull String aspectName,
      @Nonnull Filter filter);

  /**
   * Rollback the timeseries aspects associated with a runId.
   * @param runId The runId that needs to be rolledback.
   * @return
   */
  @Nonnull
  DeleteAspectValuesResult rollbackTimeseriesAspects(@Nonnull String runId);
}

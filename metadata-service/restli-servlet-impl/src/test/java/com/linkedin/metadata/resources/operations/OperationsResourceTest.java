package com.linkedin.metadata.resources.operations;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import com.linkedin.util.Pair;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import junit.framework.TestCase;
import org.testng.annotations.Test;


public class OperationsResourceTest extends TestCase {
  private static final String TASK_ID = "taskId123";
  TimeseriesAspectService mockTimeseriesAspectService = new TimeseriesAspectService() {
    @Override
    public void configure() {

    }

    @Override
    public long countByFilter(@Nonnull String entityName, @Nonnull String aspectName, @Nullable Filter filter) {
      if (filter == null || filter.equals(new Filter())) {
        return 30;
      }
      return 10;
    }

    @Nonnull
    @Override
    public List<EnvelopedAspect> getAspectValues(@Nonnull Urn urn, @Nonnull String entityName,
        @Nonnull String aspectName, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis,
        @Nullable Integer limit, @Nullable Filter filter, @Nullable SortCriterion sort) {
      return List.of();
    }

    @Nonnull
    @Override
    public GenericTable getAggregatedStats(@Nonnull String entityName, @Nonnull String aspectName,
        @Nonnull AggregationSpec[] aggregationSpecs, @Nullable Filter filter,
        @Nullable GroupingBucket[] groupingBuckets) {
      return new GenericTable();
    }

    @Nonnull
    @Override
    public DeleteAspectValuesResult deleteAspectValues(@Nonnull String entityName, @Nonnull String aspectName,
        @Nonnull Filter filter) {
      return new DeleteAspectValuesResult();
    }

    @Override
    public void reindex(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull Filter filter,
        @Nonnull BatchWriteOperationsOptions options) {

    }

    @Nonnull
    @Override
    public String deleteAspectValuesAsync(@Nonnull String entityName, @Nonnull String aspectName,
        @Nonnull Filter filter, @Nonnull BatchWriteOperationsOptions options) {
      return TASK_ID;
    }

    @Nonnull
    @Override
    public DeleteAspectValuesResult rollbackTimeseriesAspects(@Nonnull String runId) {
      return new DeleteAspectValuesResult();
    }

    @Override
    public void upsertDocument(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull String docId,
        @Nonnull JsonNode document) {

    }

    @Override
    public List<TimeseriesIndexSizeResult> getIndexSizes() {
      return List.of();
    }
  };

  @Test
  public void testDryRun() {
    String entityType = "dataset";
    String aspectName = "datasetusagestatistics";
    long endTimeMillis = 3000;
    OperationsResource testResource = new OperationsResource(mockTimeseriesAspectService);
    String output = testResource.executeTruncateTimeseriesAspect(entityType, aspectName, endTimeMillis, true, null,
        null);
    assertTrue(output.contains("This was a dry run"));
    output = testResource.executeTruncateTimeseriesAspect(entityType, aspectName, endTimeMillis, false, null,
        null);
    assertEquals(TASK_ID, output);
  }
}
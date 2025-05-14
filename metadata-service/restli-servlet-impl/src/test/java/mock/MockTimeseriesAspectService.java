package mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.TimeseriesScrollResult;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MockTimeseriesAspectService implements TimeseriesAspectService {

  public static final long DEFAULT_COUNT = 30;
  public static final long DEFAULT_FILTERED_COUNT = 10;
  public static final String DEFAULT_TASK_ID = "taskId123";

  private final long _count;
  private final long _filteredCount;
  private final String _taskId;

  public MockTimeseriesAspectService() {
    this._count = DEFAULT_COUNT;
    this._filteredCount = DEFAULT_FILTERED_COUNT;
    this._taskId = DEFAULT_TASK_ID;
  }

  public MockTimeseriesAspectService(long count, long filteredCount, String taskId) {
    this._count = count;
    this._filteredCount = filteredCount;
    this._taskId = taskId;
  }

  @Override
  public long countByFilter(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter) {
    if (filter != null && !filter.equals(new Filter())) {
      return _filteredCount;
    }
    return _count;
  }

  @Nonnull
  @Override
  public List<EnvelopedAspect> getAspectValues(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort) {
    return List.of();
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, EnvelopedAspect>> getLatestTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      @Nullable Map<String, Long> beforeTimeMillis) {
    return Map.of();
  }

  @Nonnull
  @Override
  public GenericTable getAggregatedStats(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {
    return new GenericTable();
  }

  @Nonnull
  @Override
  public DeleteAspectValuesResult deleteAspectValues(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter) {
    return new DeleteAspectValuesResult();
  }

  @Nonnull
  @Override
  public String deleteAspectValuesAsync(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    return _taskId;
  }

  @Override
  public String reindexAsync(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    return _taskId;
  }

  @Nonnull
  @Override
  public DeleteAspectValuesResult rollbackTimeseriesAspects(
      @Nonnull OperationContext operationContext, @Nonnull String runId) {
    return new DeleteAspectValuesResult();
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {}

  @Override
  public List<TimeseriesIndexSizeResult> getIndexSizes(@Nonnull OperationContext operationContext) {
    return List.of();
  }

  @Nonnull
  @Override
  public TimeseriesScrollResult scrollAspects(
      @Nonnull OperationContext operationContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      int count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return TimeseriesScrollResult.builder().build();
  }
}

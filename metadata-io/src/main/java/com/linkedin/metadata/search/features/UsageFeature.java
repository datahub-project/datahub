package com.linkedin.metadata.search.features;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;


@Slf4j
@RequiredArgsConstructor
public class UsageFeature extends BatchFeatureExtractor {

  private final TimeseriesAspectService _timeseriesAspectService;
  private final int batchSize;

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public List<Features> extractFeaturesForBatch(List<SearchEntity> entities) {
    Map<String, Long> urnToUsageCount = getUrnToUsageCount(
        entities.stream().map(SearchEntity::getEntity).map(Object::toString).collect(Collectors.toSet()));
    return entities.stream()
        .map(entity -> new Features(ImmutableMap.of(Features.Name.QUERY_COUNT,
            urnToUsageCount.getOrDefault(entity.getEntity().toString(), 0L).doubleValue())))
        .collect(Collectors.toList());
  }

  private Map<String, Long> getUrnToUsageCount(Set<String> urnsToQuery) {

    AggregationSpec queryCountAggregation =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("totalSqlQueries");

    GroupingBucket groupByUrn = new GroupingBucket().setKey("urn").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    GenericTable usageStatsTable = _timeseriesAspectService.getAggregatedStats("dataset", "datasetUsageStatistics",
        new AggregationSpec[]{queryCountAggregation}, getFilterForUrn(urnsToQuery), new GroupingBucket[]{groupByUrn});

    if (!usageStatsTable.hasRows()) {
      return Collections.emptyMap();
    }

    return usageStatsTable.getRows()
        .stream()
        .collect(Collectors.toMap(row -> row.get(0), row -> Long.parseLong(row.get(1))));
  }

  private Filter getFilterForUrn(Set<String> urns) {
    DateTime now = DateTime.now();
    DateTime monthAgo = now.minusMonths(1);
    Criterion startTimeCriterion = new Criterion().setField("timestampMillis")
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(Long.toString(monthAgo.getMillis()));
    Criterion endTimeCriterion = new Criterion().setField("timestampMillis")
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(Long.toString(now.getMillis()));
    List<ConjunctiveCriterion> urnMatchCriterion = urns.stream()
        .map(urn -> new ConjunctiveCriterion().setAnd(
            new CriterionArray(startTimeCriterion, endTimeCriterion, new Criterion().setField("urn").setValue(urn))))
        .collect(Collectors.toList());
    return new Filter().setOr(new ConjunctiveCriterionArray(urnMatchCriterion));
  }
}

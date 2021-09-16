package com.linkedin.metadata.search.features;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
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
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;


@Slf4j
@RequiredArgsConstructor
public class UsageFeature implements FeatureExtractor {

  private final TimeseriesAspectService _timeseriesAspectService;

  @Override
  public List<Features> extractFeatures(List<SearchEntity> entities) {
    Map<String, Long> urnToUsageCount = getUrnToUsageCount();
    return entities.stream()
        .map(entity -> new Features(ImmutableMap.of(Features.Name.QUERY_COUNT,
            urnToUsageCount.getOrDefault(entity.getEntity().toString(), 0L).doubleValue())))
        .collect(Collectors.toList());
  }

  private Map<String, Long> getUrnToUsageCount() {
    DateTime now = DateTime.now();
    DateTime monthAgo = now.minusMonths(1);
    Criterion startTimeCriterion = new Criterion().setField("timestampMillis")
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(Long.toString(monthAgo.getMillis()));
    Criterion endTimeCriterion = new Criterion().setField("timestampMillis")
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(Long.toString(now.getMillis()));

    AggregationSpec queryCountAggregation =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("totalSqlQueries");

    GroupingBucket groupByUrn = new GroupingBucket().setKey("urn").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    GenericTable usageStatsTable = _timeseriesAspectService.getAggregatedStats("dataset", "datasetUsageStatistics",
        new AggregationSpec[]{queryCountAggregation},
        new Filter().setCriteria(new CriterionArray(ImmutableList.of(startTimeCriterion, endTimeCriterion))),
        new GroupingBucket[]{groupByUrn});

    if (!usageStatsTable.hasRows()) {
      return Collections.emptyMap();
    }

    return usageStatsTable.getRows()
        .stream()
        .collect(Collectors.toMap(row -> row.get(0), row -> Long.parseLong(row.get(1))));
  }
}

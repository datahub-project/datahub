package com.linkedin.datahub.graphql.analytics.resolver;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsUtil;
import com.linkedin.datahub.graphql.generated.AnalyticsChart;
import com.linkedin.datahub.graphql.generated.AnalyticsChartGroup;
import com.linkedin.datahub.graphql.generated.BarChart;
import com.linkedin.datahub.graphql.generated.BarSegment;
import com.linkedin.datahub.graphql.generated.MetadataAnalyticsInput;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/** Retrieves the Charts to be rendered of the Analytics screen of the DataHub application. */
@RequiredArgsConstructor
@Slf4j
public final class GetMetadataAnalyticsResolver implements DataFetcher<List<AnalyticsChartGroup>> {

  private final EntityClient _entityClient;

  @Override
  public final List<AnalyticsChartGroup> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final MetadataAnalyticsInput input =
        bindArgument(environment.getArgument("input"), MetadataAnalyticsInput.class);

    try {
      final AnalyticsChartGroup group = new AnalyticsChartGroup();
      group.setGroupId("FilteredMetadataAnalytics");
      group.setTitle("");
      group.setCharts(getCharts(input, context.getOperationContext()));
      return ImmutableList.of(group);
    } catch (Exception e) {
      log.error("Failed to retrieve metadata analytics!", e);
      return Collections.emptyList(); // Simply return nothing.
    }
  }

  private List<AnalyticsChart> getCharts(MetadataAnalyticsInput input, OperationContext opContext)
      throws Exception {
    final List<AnalyticsChart> charts = new ArrayList<>();

    List<String> entities = Collections.emptyList();
    if (input.getEntityType() != null) {
      entities = ImmutableList.of(EntityTypeMapper.getName(input.getEntityType()));
    }

    String query = "*";
    if (!StringUtils.isEmpty(input.getQuery())) {
      query = input.getQuery();
    }

    Filter filter = null;
    if (!StringUtils.isEmpty(input.getDomain()) && !input.getDomain().equals("ALL")) {
      filter = QueryUtils.newFilter("domains.keyword", input.getDomain());
    }

    SearchResult searchResult =
        _entityClient.searchAcrossEntities(
            opContext, entities, query, filter, 0, 0, Collections.emptyList());

    List<AggregationMetadata> aggregationMetadataList =
        searchResult.getMetadata().getAggregations();

    Optional<AggregationMetadata> domainAggregation =
        aggregationMetadataList.stream()
            .filter(metadata -> metadata.getName().equals("domains"))
            .findFirst();

    if (StringUtils.isEmpty(input.getDomain()) && domainAggregation.isPresent()) {
      List<NamedBar> domainChart = buildBarChart(domainAggregation.get());
      AnalyticsUtil.hydrateDisplayNameForBars(
          opContext,
          _entityClient,
          domainChart,
          Constants.DOMAIN_ENTITY_NAME,
          ImmutableSet.of(Constants.DOMAIN_PROPERTIES_ASPECT_NAME),
          AnalyticsUtil::getDomainName);
      charts.add(BarChart.builder().setTitle("Data Assets by Domain").setBars(domainChart).build());
    }

    Optional<AggregationMetadata> platformAggregation =
        aggregationMetadataList.stream()
            .filter(metadata -> metadata.getName().equals("platform"))
            .findFirst();

    if (platformAggregation.isPresent()) {
      List<NamedBar> platformChart = buildBarChart(platformAggregation.get());
      AnalyticsUtil.hydrateDisplayNameForBars(
          opContext,
          _entityClient,
          platformChart,
          Constants.DATA_PLATFORM_ENTITY_NAME,
          ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME),
          AnalyticsUtil::getPlatformName);
      charts.add(
          BarChart.builder().setTitle("Data Assets by Platform").setBars(platformChart).build());
    }

    Optional<AggregationMetadata> termAggregation =
        aggregationMetadataList.stream()
            .filter(metadata -> metadata.getName().equals("glossaryTerms"))
            .findFirst();

    if (termAggregation.isPresent()) {
      List<NamedBar> termChart = buildBarChart(termAggregation.get());
      AnalyticsUtil.hydrateDisplayNameForBars(
          opContext,
          _entityClient,
          termChart,
          Constants.GLOSSARY_TERM_ENTITY_NAME,
          ImmutableSet.of(
              Constants.GLOSSARY_TERM_KEY_ASPECT_NAME, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
          AnalyticsUtil::getTermName);
      charts.add(BarChart.builder().setTitle("Data Assets by Term").setBars(termChart).build());
    }

    Optional<AggregationMetadata> envAggregation =
        aggregationMetadataList.stream()
            .filter(metadata -> metadata.getName().equals("origin"))
            .findFirst();

    if (envAggregation.isPresent()) {
      List<NamedBar> termChart = buildBarChart(envAggregation.get());
      if (termChart.size() > 1) {
        charts.add(
            BarChart.builder().setTitle("Data Assets by Environment").setBars(termChart).build());
      }
    }

    return charts;
  }

  private List<NamedBar> buildBarChart(AggregationMetadata aggregation) {
    return aggregation.getAggregations().entrySet().stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .limit(10)
        .map(
            entry ->
                NamedBar.builder()
                    .setName(entry.getKey())
                    .setSegments(
                        ImmutableList.of(
                            BarSegment.builder()
                                .setLabel("Count")
                                .setValue(entry.getValue().intValue())
                                .build()))
                    .build())
        .collect(Collectors.toList());
  }
}

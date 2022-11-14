package com.linkedin.datahub.graphql.analytics.resolver;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsUtil;
import com.linkedin.datahub.graphql.generated.AnalyticsChart;
import com.linkedin.datahub.graphql.generated.AnalyticsChartGroup;
import com.linkedin.datahub.graphql.generated.BarChart;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.generated.TableChart;
import com.linkedin.datahub.graphql.generated.TimeSeriesChart;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;


/**
 * Retrieves the Charts to be rendered of the Analytics screen of the DataHub application.
 */
@Slf4j
@RequiredArgsConstructor
public final class GetChartsResolver implements DataFetcher<List<AnalyticsChartGroup>> {

  private final AnalyticsService _analyticsService;
  private final EntityClient _entityClient;

  @Override
  public final List<AnalyticsChartGroup> get(DataFetchingEnvironment environment) throws Exception {
    Authentication authentication = ResolverUtils.getAuthentication(environment);
    try {
      return ImmutableList.of(AnalyticsChartGroup.builder()
          .setGroupId("DataHubUsageAnalytics")
          .setTitle("DataHub Usage Analytics")
          .setCharts(getProductAnalyticsCharts(authentication))
          .build(), AnalyticsChartGroup.builder()
          .setGroupId("GlobalMetadataAnalytics")
          .setTitle("Data Landscape Summary")
          .setCharts(getGlobalMetadataAnalyticsCharts(authentication))
          .build());
    } catch (Exception e) {
      log.error("Failed to retrieve analytics charts!", e);
      return Collections.emptyList(); // Simply return nothing.
    }
  }

  /**
   * TODO: Config Driven Charts Instead of Hardcoded.
   */
  private List<AnalyticsChart> getProductAnalyticsCharts(Authentication authentication) throws Exception {
    final List<AnalyticsChart> charts = new ArrayList<>();
    final DateTime now = DateTime.now();
    final DateTime aWeekAgo = now.minusWeeks(1);
    final DateRange lastWeekDateRange =
        new DateRange(String.valueOf(aWeekAgo.getMillis()), String.valueOf(now.getMillis()));

    final DateTime twoMonthsAgo = now.minusMonths(2);
    final DateRange twoMonthsDateRange =
        new DateRange(String.valueOf(twoMonthsAgo.getMillis()), String.valueOf(now.getMillis()));

    // Chart 1:  Time Series Chart
    String wauTitle = "Weekly Active Users";
    DateInterval weeklyInterval = DateInterval.WEEK;

    final List<NamedLine> wauTimeseries =
        _analyticsService.getTimeseriesChart(_analyticsService.getUsageIndexName(), twoMonthsDateRange, weeklyInterval,
            Optional.empty(), ImmutableMap.of(), Collections.emptyMap(), Optional.of("browserId"));
    charts.add(TimeSeriesChart.builder()
        .setTitle(wauTitle)
        .setDateRange(twoMonthsDateRange)
        .setInterval(weeklyInterval)
        .setLines(wauTimeseries)
        .build());

    // Chart 2:  Time Series Chart
    String searchesTitle = "Searches Last Week";
    DateInterval dailyInterval = DateInterval.DAY;
    String searchEventType = "SearchEvent";

    final List<NamedLine> searchesTimeseries =
        _analyticsService.getTimeseriesChart(_analyticsService.getUsageIndexName(), lastWeekDateRange, dailyInterval,
            Optional.empty(), ImmutableMap.of("type", ImmutableList.of(searchEventType)), Collections.emptyMap(),
            Optional.empty());
    charts.add(TimeSeriesChart.builder()
        .setTitle(searchesTitle)
        .setDateRange(lastWeekDateRange)
        .setInterval(dailyInterval)
        .setLines(searchesTimeseries)
        .build());

    // Chart 3: Table Chart
    final String topSearchTitle = "Top Search Queries";
    final List<String> columns = ImmutableList.of("Query", "Count");

    final List<Row> topSearchQueries =
        _analyticsService.getTopNTableChart(_analyticsService.getUsageIndexName(), Optional.of(lastWeekDateRange),
            "query.keyword", ImmutableMap.of("type", ImmutableList.of(searchEventType)), Collections.emptyMap(),
            Optional.empty(), 10, AnalyticsUtil::buildCellWithSearchLandingPage);
    charts.add(TableChart.builder().setTitle(topSearchTitle).setColumns(columns).setRows(topSearchQueries).build());

    // Chart 4: Bar Graph Chart
    final String sectionViewsTitle = "Section Views across Entity Types";
    final List<NamedBar> sectionViewsPerEntityType =
        _analyticsService.getBarChart(_analyticsService.getUsageIndexName(), Optional.of(lastWeekDateRange),
            ImmutableList.of("entityType.keyword", "section.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntitySectionViewEvent")), Collections.emptyMap(),
            Optional.empty(), true);
    charts.add(BarChart.builder().setTitle(sectionViewsTitle).setBars(sectionViewsPerEntityType).build());

    // Chart 5: Bar Graph Chart
    final String actionsByTypeTitle = "Actions by Entity Type";
    final List<NamedBar> eventsByEventType =
        _analyticsService.getBarChart(_analyticsService.getUsageIndexName(), Optional.of(lastWeekDateRange),
            ImmutableList.of("entityType.keyword", "actionType.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntityActionEvent")), Collections.emptyMap(), Optional.empty(),
            true);
    charts.add(BarChart.builder().setTitle(actionsByTypeTitle).setBars(eventsByEventType).build());

    // Chart 6: Table Chart
    final String topViewedTitle = "Top Viewed Dataset";
    final List<String> columns5 = ImmutableList.of("Dataset", "#Views");

    final List<Row> topViewedDatasets =
        _analyticsService.getTopNTableChart(_analyticsService.getUsageIndexName(), Optional.of(lastWeekDateRange),
            "entityUrn.keyword", ImmutableMap.of("type", ImmutableList.of("EntityViewEvent"), "entityType.keyword",
                ImmutableList.of(EntityType.DATASET.name())), Collections.emptyMap(), Optional.empty(), 10,
            AnalyticsUtil::buildCellWithEntityLandingPage);
    AnalyticsUtil.hydrateDisplayNameForTable(_entityClient, topViewedDatasets, Constants.DATASET_ENTITY_NAME,
        ImmutableSet.of(Constants.DATASET_KEY_ASPECT_NAME), AnalyticsUtil::getDatasetName, authentication);
    charts.add(TableChart.builder().setTitle(topViewedTitle).setColumns(columns5).setRows(topViewedDatasets).build());

    return charts;
  }

  private List<AnalyticsChart> getGlobalMetadataAnalyticsCharts(Authentication authentication) throws Exception {
    final List<AnalyticsChart> charts = new ArrayList<>();
    // Chart 1: Entities per domain
    final List<NamedBar> entitiesPerDomain =
        _analyticsService.getBarChart(_analyticsService.getAllEntityIndexName(), Optional.empty(),
            ImmutableList.of("domains.keyword", "platform.keyword"), Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty(), false);
    AnalyticsUtil.hydrateDisplayNameForBars(_entityClient, entitiesPerDomain, Constants.DOMAIN_ENTITY_NAME,
        ImmutableSet.of(Constants.DOMAIN_PROPERTIES_ASPECT_NAME), AnalyticsUtil::getDomainName, authentication);
    AnalyticsUtil.hydrateDisplayNameForSegments(_entityClient, entitiesPerDomain, Constants.DATA_PLATFORM_ENTITY_NAME,
        ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME), AnalyticsUtil::getPlatformName, authentication);
    if (!entitiesPerDomain.isEmpty()) {
      charts.add(BarChart.builder().setTitle("Entities per Domain").setBars(entitiesPerDomain).build());
    }

    // Chart 2: Entities per platform
    final List<NamedBar> entitiesPerPlatform =
        _analyticsService.getBarChart(_analyticsService.getAllEntityIndexName(), Optional.empty(),
            ImmutableList.of("platform.keyword"), Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty(), false);
    AnalyticsUtil.hydrateDisplayNameForBars(_entityClient, entitiesPerPlatform, Constants.DATA_PLATFORM_ENTITY_NAME,
        ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME), AnalyticsUtil::getPlatformName, authentication);
    if (!entitiesPerPlatform.isEmpty()) {
      charts.add(BarChart.builder().setTitle("Entities per Platform").setBars(entitiesPerPlatform).build());
    }

    // Chart 3: Entities per term
    final List<NamedBar> entitiesPerTerm =
        _analyticsService.getBarChart(_analyticsService.getAllEntityIndexName(), Optional.empty(),
            ImmutableList.of("glossaryTerms.keyword"), Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty(), false);
    AnalyticsUtil.hydrateDisplayNameForBars(_entityClient, entitiesPerTerm, Constants.GLOSSARY_TERM_ENTITY_NAME,
        ImmutableSet.of(Constants.GLOSSARY_TERM_KEY_ASPECT_NAME), AnalyticsUtil::getTermName, authentication);
    if (!entitiesPerTerm.isEmpty()) {
      charts.add(BarChart.builder().setTitle("Entities per Term").setBars(entitiesPerTerm).build());
    }

    // Chart 4: Entities per fabric type
    final List<NamedBar> entitiesPerEnv =
        _analyticsService.getBarChart(_analyticsService.getAllEntityIndexName(), Optional.empty(),
            ImmutableList.of("origin.keyword"), Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty(), false);
    if (entitiesPerEnv.size() > 1) {
      charts.add(BarChart.builder().setTitle("Entities per Environment").setBars(entitiesPerEnv).build());
    }

    return charts;
  }
}

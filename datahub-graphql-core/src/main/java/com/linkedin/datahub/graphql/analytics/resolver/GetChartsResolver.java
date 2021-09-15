package com.linkedin.datahub.graphql.analytics.resolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.AnalyticsChart;
import com.linkedin.datahub.graphql.generated.AnalyticsChartGroup;
import com.linkedin.datahub.graphql.generated.BarChart;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.generated.TableChart;
import com.linkedin.datahub.graphql.generated.TimeSeriesChart;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;


/**
 * Retrieves the Charts to be rendered of the Analytics screen of the DataHub application.
 */
public final class GetChartsResolver implements DataFetcher<List<AnalyticsChartGroup>> {

  private final AnalyticsService _analyticsService;

  public GetChartsResolver(final AnalyticsService analyticsService) {
    _analyticsService = analyticsService;
  }

  @Override
  public final List<AnalyticsChartGroup> get(DataFetchingEnvironment environment) throws Exception {
    final AnalyticsChartGroup group = new AnalyticsChartGroup();
    group.setTitle("Product Analytics");
    group.setCharts(getProductAnalyticsCharts());
    return ImmutableList.of(group);
  }

  /**
   * TODO: Config Driven Charts Instead of Hardcoded.
   */
  private List<AnalyticsChart> getProductAnalyticsCharts() {
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
        _analyticsService.getTimeseriesChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, twoMonthsDateRange, weeklyInterval,
            Optional.empty(), ImmutableMap.of(), Optional.of("browserId"));
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
        _analyticsService.getTimeseriesChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, lastWeekDateRange, dailyInterval,
            Optional.empty(), ImmutableMap.of("type", ImmutableList.of(searchEventType)), Optional.empty());
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
        _analyticsService.getTopNTableChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(lastWeekDateRange),
            "query.keyword", ImmutableMap.of("type", ImmutableList.of(searchEventType)), Optional.empty(), 10);
    charts.add(TableChart.builder().setTitle(topSearchTitle).setColumns(columns).setRows(topSearchQueries).build());

    // Chart 4: Bar Graph Chart
    final String sectionViewsTitle = "Section Views across Entity Types";
    final List<NamedBar> sectionViewsPerEntityType =
        _analyticsService.getBarChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(lastWeekDateRange),
            ImmutableList.of("entityType.keyword", "section.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntitySectionViewEvent")), Optional.empty());
    charts.add(BarChart.builder().setTitle(sectionViewsTitle).setBars(sectionViewsPerEntityType).build());

    // Chart 5: Bar Graph Chart
    final String actionsByTypeTitle = "Actions by Entity Type";
    final List<NamedBar> eventsByEventType =
        _analyticsService.getBarChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(lastWeekDateRange),
            ImmutableList.of("entityType.keyword", "actionType.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntityActionEvent")), Optional.empty());
    charts.add(BarChart.builder().setTitle(actionsByTypeTitle).setBars(eventsByEventType).build());

    // Chart 6: Table Chart
    final String topViewedTitle = "Top Viewed Dataset";
    final List<String> columns5 = ImmutableList.of("Dataset", "#Views");

    final List<Row> topViewedDatasets =
        _analyticsService.getTopNTableChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(lastWeekDateRange),
            "dataset_name.keyword", ImmutableMap.of("type", ImmutableList.of("EntityViewEvent")), Optional.empty(), 10);
    charts.add(TableChart.builder().setTitle(topViewedTitle).setColumns(columns5).setRows(topViewedDatasets).build());
    
    return charts;
  }
}

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
    final DateTime endDate = DateTime.now();
    final DateTime startDate = endDate.minusWeeks(1);
    final DateRange dateRange =
        new DateRange(String.valueOf(startDate.getMillis()), String.valueOf(endDate.getMillis()));

    // Chart 1:  Time Series Chart
    String title = "Searches Last Week";
    DateInterval granularity = DateInterval.DAY;
    String eventType = "SearchEvent";

    final List<NamedLine> searchesTimeseries =
        _analyticsService.getTimeseriesChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, dateRange, granularity,
            Optional.empty(), ImmutableMap.of("type", ImmutableList.of("SearchEvent")), Optional.empty());
    charts.add(TimeSeriesChart.builder()
        .setTitle(title)
        .setDateRange(dateRange)
        .setInterval(granularity)
        .setLines(searchesTimeseries)
        .build());

    // Chart 2: Table Chart
    final String title2 = "Top Search Queries";
    final List<String> columns = ImmutableList.of("Query", "Count");

    final List<Row> topSearchQueries =
        _analyticsService.getTopNTableChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(dateRange),
            "query.keyword", ImmutableMap.of("type", ImmutableList.of(eventType)), Optional.empty(), 10);
    charts.add(TableChart.builder().setTitle(title2).setColumns(columns).setRows(topSearchQueries).build());

    // Chart 3: Bar Graph Chart
    final String title3 = "Section Views across Entity Types";
    final List<NamedBar> sectionViewsPerEntityType =
        _analyticsService.getBarChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(dateRange),
            ImmutableList.of("entityType.keyword", "section.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntitySectionViewEvent")), Optional.empty());
    charts.add(BarChart.builder().setTitle(title3).setBars(sectionViewsPerEntityType).build());

    // Chart 4: Bar Graph Chart
    final String title4 = "Actions by Entity Type";
    final List<NamedBar> eventsByEventType =
        _analyticsService.getBarChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(dateRange),
            ImmutableList.of("entityType.keyword", "actionType.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntityActionEvent")), Optional.empty());
    charts.add(BarChart.builder().setTitle(title4).setBars(eventsByEventType).build());

    // Chart 5: Table Chart
    final String title5 = "Top Viewed Dataset";
    final List<String> columns5 = ImmutableList.of("Dataset", "#Views");

    final List<Row> topViewedDatasets =
        _analyticsService.getTopNTableChart(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(dateRange),
            "dataset_name.keyword", ImmutableMap.of("type", ImmutableList.of("EntityViewEvent")), Optional.empty(), 10);
    charts.add(TableChart.builder().setTitle(title5).setColumns(columns5).setRows(topViewedDatasets).build());
    
    return charts;
  }
}

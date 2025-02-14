package com.linkedin.datahub.graphql.analytics.resolver;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_LAST_MODIFIED_FIELD_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsUtil;
import com.linkedin.datahub.graphql.generated.AnalyticsChart;
import com.linkedin.datahub.graphql.generated.AnalyticsChartGroup;
import com.linkedin.datahub.graphql.generated.BarChart;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityProfileParams;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LinkParams;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.generated.TableChart;
import com.linkedin.datahub.graphql.generated.TimeSeriesChart;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.util.DateUtil;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

/** Retrieves the Charts to be rendered of the Analytics screen of the DataHub application. */
@Slf4j
@RequiredArgsConstructor
public final class GetChartsResolver implements DataFetcher<List<AnalyticsChartGroup>> {

  private final AnalyticsService _analyticsService;
  private final EntityClient _entityClient;

  @Override
  public List<AnalyticsChartGroup> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    try {
      return ImmutableList.of(
          AnalyticsChartGroup.builder()
              .setGroupId("DataHubUsageAnalytics")
              .setTitle("Usage Analytics")
              .setCharts(getProductAnalyticsCharts(context.getOperationContext()))
              .build(),
          AnalyticsChartGroup.builder()
              .setGroupId("GlobalMetadataAnalytics")
              .setTitle("Data Landscape Summary")
              .setCharts(getGlobalMetadataAnalyticsCharts(context.getOperationContext()))
              .build());
    } catch (Exception e) {
      log.error("Failed to retrieve analytics charts!", e);
      return Collections.emptyList(); // Simply return nothing.
    }
  }

  private TimeSeriesChart getActiveUsersTimeSeriesChart(
      final DateTime beginning,
      final DateTime end,
      final String title,
      final DateInterval interval) {

    final DateRange dateRange =
        new DateRange(String.valueOf(beginning.getMillis()), String.valueOf(end.getMillis()));

    final List<NamedLine> timeSeriesLines =
        _analyticsService.getTimeseriesChart(
            _analyticsService.getUsageIndexName(),
            dateRange,
            interval,
            Optional.empty(),
            ImmutableMap.of(),
            Collections.emptyMap(),
            Optional.of("browserId"));

    return TimeSeriesChart.builder()
        .setTitle(title)
        .setDateRange(dateRange)
        .setInterval(interval)
        .setLines(timeSeriesLines)
        .build();
  }

  @Nullable
  private AnalyticsChart getTopUsersChart(OperationContext opContext) {
    try {
      final DateUtil dateUtil = new DateUtil();
      final DateRange trailingMonthDateRange = dateUtil.getTrailingMonthDateRange();
      final List<String> columns = ImmutableList.of("Name", "Title", "Email");

      final String topUsersTitle = "Top Users (Last 30 Days)";
      final List<Row> topUserRows =
          _analyticsService.getTopNTableChart(
              _analyticsService.getUsageIndexName(),
              Optional.of(trailingMonthDateRange),
              "actorUrn.keyword",
              Collections.emptyMap(),
              ImmutableMap.of(
                  "actorUrn.keyword",
                  ImmutableList.of("urn:li:corpuser:admin", "urn:li:corpuser:datahub")),
              Optional.empty(),
              30,
              AnalyticsUtil::buildCellWithEntityLandingPage);
      AnalyticsUtil.convertToUserInfoRows(opContext, _entityClient, topUserRows);
      return TableChart.builder()
          .setTitle(topUsersTitle)
          .setColumns(columns)
          .setRows(topUserRows)
          .build();
    } catch (Exception e) {
      log.error("Failed to retrieve top users chart!", e);
      return null;
    }
  }

  private SearchResult searchForNewUsers(@Nonnull final OperationContext opContext)
      throws Exception {
    // Search for new users in the past month.
    final DateUtil dateUtil = new DateUtil();
    final DateRange trailingMonthDateRange = dateUtil.getTrailingMonthDateRange();
    return _entityClient.search(
        opContext,
        CORP_USER_ENTITY_NAME,
        "*",
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion(
                                            CORP_USER_STATUS_LAST_MODIFIED_FIELD_NAME,
                                            Condition.GREATER_THAN,
                                            String.valueOf(
                                                trailingMonthDateRange.getStart())))))))),
        Collections.singletonList(
            new SortCriterion()
                .setField(CORP_USER_STATUS_LAST_MODIFIED_FIELD_NAME)
                .setOrder(SortOrder.DESCENDING)),
        0,
        100);
  }

  @Nonnull
  private Row buildNewUsersRow(@Nonnull final SearchEntity entity) {
    final Row row = new Row();
    row.setValues(ImmutableList.of(entity.getEntity().toString()));
    final Cell cell = new Cell();
    cell.setValue(entity.getEntity().toString());
    cell.setEntity(UrnToEntityMapper.map(null, entity.getEntity()));
    cell.setLinkParams(
        new LinkParams(
            null, new EntityProfileParams(entity.getEntity().toString(), EntityType.CORP_USER)));
    row.setCells(ImmutableList.of(cell));
    return row;
  }

  @Nullable
  private AnalyticsChart getNewUsersChart(OperationContext opContext) {
    try {
      final List<String> columns = ImmutableList.of("Name", "Title", "Email");
      final String newUsersTitle = "Active Users (Last 30 Days)";
      final SearchResult result = searchForNewUsers(opContext);
      final List<Row> newUserRows = new ArrayList<>();
      for (SearchEntity entity : result.getEntities()) {
        newUserRows.add(buildNewUsersRow(entity));
      }
      AnalyticsUtil.convertToUserInfoRows(opContext, _entityClient, newUserRows);
      return TableChart.builder()
          .setTitle(newUsersTitle)
          .setColumns(columns)
          .setRows(newUserRows)
          .build();
    } catch (Exception e) {
      log.error("Failed to retrieve new users chart!", e);
      return null;
    }
  }

  /** TODO: Config Driven Charts Instead of Hardcoded. */
  private List<AnalyticsChart> getProductAnalyticsCharts(OperationContext opContext)
      throws Exception {
    final List<AnalyticsChart> charts = new ArrayList<>();
    DateUtil dateUtil = new DateUtil();
    final DateTime startOfNextWeek = dateUtil.getStartOfNextWeek();
    final DateTime startOfThisMonth = dateUtil.getStartOfThisMonth();
    final DateTime startOfNextMonth = dateUtil.getStartOfNextMonth();
    final DateRange trailingWeekDateRange = dateUtil.getTrailingWeekDateRange();

    // WAU
    charts.add(
        getActiveUsersTimeSeriesChart(
            startOfNextWeek.minusWeeks(10),
            startOfNextWeek.minusMillis(1),
            "Weekly Active Users",
            DateInterval.WEEK));

    // MAU
    charts.add(
        getActiveUsersTimeSeriesChart(
            startOfNextMonth.minusMonths(12),
            startOfThisMonth.minusMillis(1),
            "Monthly Active Users",
            DateInterval.MONTH));

    // New users chart - past month
    final AnalyticsChart newUsersChart = getNewUsersChart(opContext);
    if (newUsersChart != null) {
      charts.add(newUsersChart);
    }

    // Top users chart - past month
    final AnalyticsChart topUsersChart = getTopUsersChart(opContext);
    if (topUsersChart != null) {
      charts.add(topUsersChart);
    }

    String searchesTitle = "Number of Searches";
    DateInterval dailyInterval = DateInterval.DAY;
    String searchEventType = "SearchEvent";

    final List<NamedLine> searchesTimeseries =
        _analyticsService.getTimeseriesChart(
            _analyticsService.getUsageIndexName(),
            trailingWeekDateRange,
            dailyInterval,
            Optional.empty(),
            ImmutableMap.of("type", ImmutableList.of(searchEventType)),
            Collections.emptyMap(),
            Optional.empty());
    charts.add(
        TimeSeriesChart.builder()
            .setTitle(searchesTitle)
            .setDateRange(trailingWeekDateRange)
            .setInterval(dailyInterval)
            .setLines(searchesTimeseries)
            .build());

    final String topSearchTitle = "Top Searches (Past Week)";
    final List<String> columns = ImmutableList.of("Query", "Count");

    final List<Row> topSearchQueries =
        _analyticsService.getTopNTableChart(
            _analyticsService.getUsageIndexName(),
            Optional.of(trailingWeekDateRange),
            "query.keyword",
            ImmutableMap.of("type", ImmutableList.of(searchEventType)),
            Collections.emptyMap(),
            Optional.empty(),
            10,
            AnalyticsUtil::buildCellWithSearchLandingPage);
    charts.add(
        TableChart.builder()
            .setTitle(topSearchTitle)
            .setColumns(columns)
            .setRows(topSearchQueries)
            .build());

    final String topViewedDatasetsTitle = "Top Viewed Datasets (Past Week)";
    final List<String> columns5 = ImmutableList.of("Name", "View Count");
    final List<Row> topViewedDatasets =
        _analyticsService.getTopNTableChart(
            _analyticsService.getUsageIndexName(),
            Optional.of(trailingWeekDateRange),
            "entityUrn.keyword",
            ImmutableMap.of(
                "type",
                ImmutableList.of("EntityViewEvent"),
                "entityType.keyword",
                ImmutableList.of(EntityType.DATASET.name())),
            Collections.emptyMap(),
            Optional.empty(),
            10,
            AnalyticsUtil::buildCellWithEntityLandingPage);
    AnalyticsUtil.hydrateDisplayNameForTable(
        opContext,
        _entityClient,
        topViewedDatasets,
        Constants.DATASET_ENTITY_NAME,
        ImmutableSet.of(
            Constants.DATASET_KEY_ASPECT_NAME, Constants.DATASET_PROPERTIES_ASPECT_NAME),
        AnalyticsUtil::getDatasetName);
    charts.add(
        TableChart.builder()
            .setTitle(topViewedDatasetsTitle)
            .setColumns(columns5)
            .setRows(topViewedDatasets)
            .build());

    final String topViewedDashboardsTitle = "Top Viewed Dashboards (Past Week)";
    final List<String> columns6 = ImmutableList.of("Name", "View Count");
    final List<Row> topViewedDashboards =
        _analyticsService.getTopNTableChart(
            _analyticsService.getUsageIndexName(),
            Optional.of(trailingWeekDateRange),
            "entityUrn.keyword",
            ImmutableMap.of(
                "type",
                ImmutableList.of("EntityViewEvent"),
                "entityType.keyword",
                ImmutableList.of(EntityType.DASHBOARD.name())),
            Collections.emptyMap(),
            Optional.empty(),
            10,
            AnalyticsUtil::buildCellWithEntityLandingPage);
    AnalyticsUtil.hydrateDisplayNameForTable(
        opContext,
        _entityClient,
        topViewedDashboards,
        Constants.DASHBOARD_ENTITY_NAME,
        ImmutableSet.of(Constants.DASHBOARD_INFO_ASPECT_NAME),
        AnalyticsUtil::getDashboardName);
    charts.add(
        TableChart.builder()
            .setTitle(topViewedDashboardsTitle)
            .setColumns(columns6)
            .setRows(topViewedDashboards)
            .build());

    final String sectionViewsTitle = "Tab Views By Entity Type (Past Week)";
    final List<NamedBar> sectionViewsPerEntityType =
        _analyticsService.getBarChart(
            _analyticsService.getUsageIndexName(),
            Optional.of(trailingWeekDateRange),
            ImmutableList.of("entityType.keyword", "section.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntitySectionViewEvent")),
            Collections.emptyMap(),
            Optional.empty(),
            true);
    charts.add(
        BarChart.builder().setTitle(sectionViewsTitle).setBars(sectionViewsPerEntityType).build());

    final String actionsByTypeTitle = "Actions By Entity Type (Past Week)";
    final List<NamedBar> eventsByEventType =
        _analyticsService.getBarChart(
            _analyticsService.getUsageIndexName(),
            Optional.of(trailingWeekDateRange),
            ImmutableList.of("entityType.keyword", "actionType.keyword"),
            ImmutableMap.of("type", ImmutableList.of("EntityActionEvent")),
            Collections.emptyMap(),
            Optional.empty(),
            true);
    charts.add(BarChart.builder().setTitle(actionsByTypeTitle).setBars(eventsByEventType).build());

    return charts;
  }

  private List<AnalyticsChart> getGlobalMetadataAnalyticsCharts(OperationContext opContext)
      throws Exception {
    final List<AnalyticsChart> charts = new ArrayList<>();
    // Chart 1: Entities per domain
    final List<NamedBar> entitiesPerDomain =
        _analyticsService.getBarChart(
            _analyticsService.getAllEntityIndexName(),
            Optional.empty(),
            ImmutableList.of("domains.keyword", "platform.keyword"),
            Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")),
            Optional.empty(),
            false);
    AnalyticsUtil.hydrateDisplayNameForBars(
        opContext,
        _entityClient,
        entitiesPerDomain,
        Constants.DOMAIN_ENTITY_NAME,
        ImmutableSet.of(Constants.DOMAIN_PROPERTIES_ASPECT_NAME),
        AnalyticsUtil::getDomainName);
    AnalyticsUtil.hydrateDisplayNameForSegments(
        opContext,
        _entityClient,
        entitiesPerDomain,
        Constants.DATA_PLATFORM_ENTITY_NAME,
        ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME),
        AnalyticsUtil::getPlatformName);
    if (!entitiesPerDomain.isEmpty()) {
      charts.add(
          BarChart.builder().setTitle("Entities By Domain").setBars(entitiesPerDomain).build());
    }

    // Chart 2: Entities per platform
    final List<NamedBar> entitiesPerPlatform =
        _analyticsService.getBarChart(
            _analyticsService.getAllEntityIndexName(),
            Optional.empty(),
            ImmutableList.of("platform.keyword"),
            Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")),
            Optional.empty(),
            false);
    AnalyticsUtil.hydrateDisplayNameForBars(
        opContext,
        _entityClient,
        entitiesPerPlatform,
        Constants.DATA_PLATFORM_ENTITY_NAME,
        ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME),
        AnalyticsUtil::getPlatformName);
    if (!entitiesPerPlatform.isEmpty()) {
      charts.add(
          BarChart.builder().setTitle("Assets By Platform").setBars(entitiesPerPlatform).build());
    }

    // Chart 3: Entities per term
    final List<NamedBar> entitiesPerTerm =
        _analyticsService.getBarChart(
            _analyticsService.getAllEntityIndexName(),
            Optional.empty(),
            ImmutableList.of("glossaryTerms.keyword"),
            Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")),
            Optional.empty(),
            false);
    AnalyticsUtil.hydrateDisplayNameForBars(
        opContext,
        _entityClient,
        entitiesPerTerm,
        Constants.GLOSSARY_TERM_ENTITY_NAME,
        ImmutableSet.of(
            Constants.GLOSSARY_TERM_KEY_ASPECT_NAME, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
        AnalyticsUtil::getTermName);
    if (!entitiesPerTerm.isEmpty()) {
      charts.add(
          BarChart.builder().setTitle("Entities With Term").setBars(entitiesPerTerm).build());
    }

    // Chart 4: Entities per fabric type
    final List<NamedBar> entitiesPerEnv =
        _analyticsService.getBarChart(
            _analyticsService.getAllEntityIndexName(),
            Optional.empty(),
            ImmutableList.of("origin.keyword"),
            Collections.emptyMap(),
            ImmutableMap.of("removed", ImmutableList.of("true")),
            Optional.empty(),
            false);
    if (entitiesPerEnv.size() > 1) {
      charts.add(
          BarChart.builder().setTitle("Entities By Environment").setBars(entitiesPerEnv).build());
    }

    return charts;
  }
}

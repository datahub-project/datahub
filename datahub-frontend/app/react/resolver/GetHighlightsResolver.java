package react.resolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.DateRange;
import graphql.Highlight;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.joda.time.DateTime;
import react.analytics.AnalyticsService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * Retrieves the Highlights to be rendered of the Analytics screen of the DataHub application.
 */
public final class GetHighlightsResolver implements DataFetcher<List<Highlight>> {

  private final AnalyticsService _analyticsService;

  public GetHighlightsResolver(final AnalyticsService analyticsService) {
    _analyticsService = analyticsService;
  }

  @Override
  public final List<Highlight> get(DataFetchingEnvironment environment) throws Exception {
    return getHighlights();
  }

  /**
   * TODO: Config Driven Charts Instead of Hardcoded.
   */
  private List<Highlight> getHighlights() {
    final List<Highlight> highlights = new ArrayList<>();

    DateTime endDate = DateTime.now();
    DateTime startDate = endDate.minusWeeks(1);
    DateTime lastWeekStartDate = startDate.minusWeeks(1);
    DateRange dateRange = new DateRange(String.valueOf(startDate.getMillis()), String.valueOf(endDate.getMillis()));
    DateRange dateRangeLastWeek =
        new DateRange(String.valueOf(lastWeekStartDate.getMillis()), String.valueOf(startDate.getMillis()));
    
    highlights.add(getWeeklyChangeStats("Weekly Active Browsers", AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, "browserId"), dateRange, dateRangeLastWeek);
    highlights.add(getWeeklyChangeStats("Weekly Active Users", AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, "corp_user_username"), dateRange, dateRangeLastWeek);

    // Entity metdata statistics
    highlights.add(getEntityMetadataStats("Datasets", AnalyticsService.DATASET_INDEX));
    highlights.add(getEntityMetadataStats("Dashboards", AnalyticsService.DASHBOARD_INDEX));
    highlights.add(getEntityMetadataStats("Charts", AnalyticsService.CHART_INDEX));
    highlights.add(getEntityMetadataStats("Pipelines", AnalyticsService.DATA_FLOW_INDEX));
    highlights.add(getEntityMetadataStats("Tasks", AnalyticsService.DATA_JOB_INDEX));
    return highlights;
  }

  private Highlight getWeeklyChangeStats(String title, String index, String property, DateRange dateRange, DateRange dateRangeLastWeek) {
    int weeklyActive =
        _analyticsService.getHighlights(index, Optional.of(dateRange),
            ImmutableMap.of(), ImmutableMap.of(), Optional.of(property));

    int weeklyActiveLastWeek =
        _analyticsService.getHighlights(index, Optional.of(dateRangeLastWeek),
            ImmutableMap.of(), ImmutableMap.of(), Optional.of(property));

    String bodyText = "";
    if (weeklyActiveLastWeek > 0) {
      Double percentChange =
          (Double.valueOf(weeklyActive) - Double.valueOf(weeklyActiveLastWeek)) / Double.valueOf(
              weeklyActiveLastWeek) * 100;

      String directionChange = percentChange > 0 ? "increase" : "decrease";

      bodyText = Double.isInfinite(percentChange) ? ""
          : String.format("%.2f%% %s from last week", percentChange, directionChange);
    }
    return Highlight.builder().setTitle(title).setValue(weeklyActive).setBody(bodyText).build();
  }

  private Highlight getEntityMetadataStats(String title, String index) {
    int numEntities = _analyticsService.getHighlights(index, Optional.empty(), ImmutableMap.of(),
        ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty());
    int numEntitiesWithOwners =
        _analyticsService.getHighlights(index, Optional.empty(), ImmutableMap.of("hasOwners", ImmutableList.of("true")),
            ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty());
    String bodyText = "";
    if (numEntities > 0) {
      double percentChange = 100.0 * numEntitiesWithOwners / numEntities;
      bodyText = String.format("%.2f%% have owners assigned!", percentChange);
    }
    return Highlight.builder().setTitle(title).setValue(numEntities).setBody(bodyText).build();
  }
}

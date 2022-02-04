package com.linkedin.datahub.graphql.analytics.resolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.Highlight;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.joda.time.DateTime;


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

    // Highlight 1: The Highlights!
    String title = "Weekly Active Users";
    String eventType = "SearchEvent";

    int weeklyActiveUsers =
        _analyticsService.getHighlights(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(dateRange),
            ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));

    int weeklyActiveUsersLastWeek =
        _analyticsService.getHighlights(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX, Optional.of(dateRangeLastWeek),
            ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));

    String bodyText = "";
    if (weeklyActiveUsersLastWeek > 0) {
      Double percentChange =
          (Double.valueOf(weeklyActiveUsers) - Double.valueOf(weeklyActiveUsersLastWeek)) / Double.valueOf(
              weeklyActiveUsersLastWeek) * 100;

      String directionChange = percentChange > 0 ? "increase" : "decrease";

      bodyText = Double.isInfinite(percentChange) ? ""
          : String.format("%.2f%% %s from last week", percentChange, directionChange);
    }

    highlights.add(Highlight.builder().setTitle(title).setValue(weeklyActiveUsers).setBody(bodyText).build());

    // Entity metdata statistics
    highlights.add(getEntityMetadataStats("Datasets", AnalyticsService.DATASET_INDEX));
    highlights.add(getEntityMetadataStats("Dashboards", AnalyticsService.DASHBOARD_INDEX));
    highlights.add(getEntityMetadataStats("Charts", AnalyticsService.CHART_INDEX));
    highlights.add(getEntityMetadataStats("Pipelines", AnalyticsService.DATA_FLOW_INDEX));
    highlights.add(getEntityMetadataStats("Tasks", AnalyticsService.DATA_JOB_INDEX));
    return highlights;
  }

  private Highlight getEntityMetadataStats(String title, String index) {
    int numEntities = getNumEntitiesFiltered(index, ImmutableMap.of());
    int numEntitiesWithOwners =
            getNumEntitiesFiltered(index, ImmutableMap.of("hasOwners", ImmutableList.of("true")));
    int numEntitiesWithTags =
            getNumEntitiesFiltered(index, ImmutableMap.of("hasTags", ImmutableList.of("true")));
    int numEntitiesWithDescription =
            getNumEntitiesFiltered(index, ImmutableMap.of("hasDescription", ImmutableList.of("true")));
    int numEntitiesWithDomains =
            getNumEntitiesFiltered(index, ImmutableMap.of("hasDomain", ImmutableList.of("true")));

    String bodyText = "";
    if (numEntities > 0) {
      double percentWithOwners = 100.0 * numEntitiesWithOwners / numEntities;
      double percentWithTags = 100.0 * numEntitiesWithTags / numEntities;
      double percentWithDescription = 100.0 * numEntitiesWithDescription / numEntities;
      double percentWithDomains = 100.0 * numEntitiesWithDomains / numEntities;
      bodyText = String.format(
              "%.2f%% have owners, %.2f%% have tags, %.2f%% have description, %.2f%% have domain assigned!",
              percentWithOwners, percentWithTags, percentWithDescription, percentWithDomains);
    }
    return Highlight.builder().setTitle(title).setValue(numEntities).setBody(bodyText).build();
  }

  private int getNumEntitiesFiltered(String index, Map<String, List<String>> filters) {
    return _analyticsService.getHighlights(index, Optional.empty(), filters,
            ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty());
  }
}

package com.linkedin.datahub.graphql.analytics.resolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Highlight;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;


/**
 * Retrieves the Highlights to be rendered of the Analytics screen of the DataHub application.
 */
@RequiredArgsConstructor
public final class GetHighlightsResolver implements DataFetcher<List<Highlight>> {

  private final AnalyticsService _analyticsService;

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
        _analyticsService.getHighlights(_analyticsService.getUsageIndexName(), Optional.of(dateRange),
            ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));

    int weeklyActiveUsersLastWeek =
        _analyticsService.getHighlights(_analyticsService.getUsageIndexName(), Optional.of(dateRangeLastWeek),
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
    getEntityMetadataStats("Datasets", EntityType.DATASET).ifPresent(highlights::add);
    getEntityMetadataStats("Dashboards", EntityType.DASHBOARD).ifPresent(highlights::add);
    getEntityMetadataStats("Charts", EntityType.CHART).ifPresent(highlights::add);
    getEntityMetadataStats("Pipelines", EntityType.DATA_FLOW).ifPresent(highlights::add);
    getEntityMetadataStats("Tasks", EntityType.DATA_JOB).ifPresent(highlights::add);
    getEntityMetadataStats("Domains", EntityType.DOMAIN).ifPresent(highlights::add);
    return highlights;
  }

  private Optional<Highlight> getEntityMetadataStats(String title, EntityType entityType) {
    String index = _analyticsService.getEntityIndexName(entityType);
    int numEntities = getNumEntitiesFiltered(index, ImmutableMap.of());
    // If there are no entities for the type, do not show the highlight
    if (numEntities == 0) {
      return Optional.empty();
    }
    int numEntitiesWithOwners = getNumEntitiesFiltered(index, ImmutableMap.of("hasOwners", ImmutableList.of("true")));
    int numEntitiesWithTags = getNumEntitiesFiltered(index, ImmutableMap.of("hasTags", ImmutableList.of("true")));
    int numEntitiesWithDescription =
        getNumEntitiesFiltered(index, ImmutableMap.of("hasDescription", ImmutableList.of("true")));

    String bodyText = "";
    if (numEntities > 0) {
      double percentWithOwners = 100.0 * numEntitiesWithOwners / numEntities;
      double percentWithTags = 100.0 * numEntitiesWithTags / numEntities;
      double percentWithDescription = 100.0 * numEntitiesWithDescription / numEntities;
      if (entityType == EntityType.DOMAIN) {
        // Don't show percent with domain when asking for stats regarding domains
        bodyText = String.format("%.2f%% have owners, %.2f%% have tags, %.2f%% have description!", percentWithOwners,
            percentWithTags, percentWithDescription);
      } else {
        int numEntitiesWithDomains =
            getNumEntitiesFiltered(index, ImmutableMap.of("hasDomain", ImmutableList.of("true")));
        double percentWithDomains = 100.0 * numEntitiesWithDomains / numEntities;
        bodyText =
            String.format("%.2f%% have owners, %.2f%% have tags, %.2f%% have description, %.2f%% have domain assigned!",
                percentWithOwners, percentWithTags, percentWithDescription, percentWithDomains);
      }
    }
    return Optional.of(Highlight.builder().setTitle(title).setValue(numEntities).setBody(bodyText).build());
  }

  private int getNumEntitiesFiltered(String index, Map<String, List<String>> filters) {
    return _analyticsService.getHighlights(index, Optional.empty(), filters,
        ImmutableMap.of("removed", ImmutableList.of("true")), Optional.empty());
  }
}

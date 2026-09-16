package com.linkedin.datahub.graphql.analytics.resolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.EntityStats;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Highlight;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

/** Retrieves the Highlights to be rendered of the Analytics screen of the DataHub application. */
@RequiredArgsConstructor
@Slf4j
public final class GetHighlightsResolver implements DataFetcher<List<Highlight>> {

  private static final String BROWSER_ID = "browserId";
  private static final String CURRENT_SUFFIX = "_current";
  private static final String PREVIOUS_SUFFIX = "_previous";
  private static final String WEEKLY = "weekly";
  private static final String MONTHLY = "monthly";

  private static final String HAS_OWNERS = "hasOwners";
  private static final String HAS_TAGS = "hasTags";
  private static final String HAS_GLOSSARY_TERMS = "hasGlossaryTerms";
  private static final String HAS_DESCRIPTION = "hasDescription";
  private static final String HAS_DOMAIN = "hasDomain";

  private static final List<String> ENTITY_FACETS =
      ImmutableList.of(HAS_OWNERS, HAS_TAGS, HAS_GLOSSARY_TERMS, HAS_DESCRIPTION, HAS_DOMAIN);

  /** Entity types shown as metadata-statistics highlights, in display order. */
  private static final Map<EntityType, String> ENTITY_TITLES =
      ImmutableMap.<EntityType, String>builder()
          .put(EntityType.DATASET, "Datasets")
          .put(EntityType.DASHBOARD, "Dashboards")
          .put(EntityType.CHART, "Charts")
          .put(EntityType.DATA_FLOW, "Pipelines")
          .put(EntityType.DATA_JOB, "Tasks")
          .put(EntityType.DOMAIN, "Domains")
          .build();

  private final AnalyticsService _analyticsService;

  @Override
  public final List<Highlight> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    try {
      return getHighlights(context.getOperationContext());
    } catch (Exception e) {
      log.error("Failed to retrieve analytics highlights!", e);
      return Collections.emptyList(); // Simply return nothing.
    }
  }

  /** TODO: Config Driven Charts Instead of Hardcoded. */
  private List<Highlight> getHighlights(@Nonnull final OperationContext opContext) {
    final List<Highlight> highlights = new ArrayList<>();

    final DateTime endDate = DateTime.now();
    final Map<String, DateRange> ranges = new LinkedHashMap<>();
    addPeriod(ranges, WEEKLY, endDate, date -> date.minusWeeks(1));
    addPeriod(ranges, MONTHLY, endDate, date -> date.minusMonths(1));

    final Map<String, Integer> activeUsers =
        _analyticsService.getUniqueCountsByRange(
            opContext, _analyticsService.getUsageIndexName(opContext), ranges, BROWSER_ID);

    highlights.add(
        buildTimeBasedHighlight(
            "Weekly Active Users", "%.2f%% %s from last week", activeUsers, WEEKLY));
    highlights.add(
        buildTimeBasedHighlight(
            "Monthly Active Users", "%.2f%% %s from last month", activeUsers, MONTHLY));

    // Entity metadata statistics
    final Map<EntityType, EntityStats> entityStats =
        _analyticsService.getEntityStats(
            opContext, ImmutableList.copyOf(ENTITY_TITLES.keySet()), ENTITY_FACETS);
    ENTITY_TITLES.forEach(
        (entityType, title) ->
            buildEntityMetadataHighlight(title, entityType, entityStats.get(entityType))
                .ifPresent(highlights::add));

    return highlights;
  }

  private void addPeriod(
      final Map<String, DateRange> ranges,
      final String period,
      final DateTime endDateTime,
      final Function<DateTime, DateTime> periodStartFunc) {
    DateTime startDate = periodStartFunc.apply(endDateTime);
    DateTime timeBeforeThat = periodStartFunc.apply(startDate);
    ranges.put(
        period + CURRENT_SUFFIX,
        new DateRange(
            String.valueOf(startDate.getMillis()), String.valueOf(endDateTime.getMillis())));
    ranges.put(
        period + PREVIOUS_SUFFIX,
        new DateRange(
            String.valueOf(timeBeforeThat.getMillis()), String.valueOf(startDate.getMillis())));
  }

  private Highlight buildTimeBasedHighlight(
      final String title,
      final String changeString,
      final Map<String, Integer> activeUsers,
      final String period) {
    int activeUsersThisRange = activeUsers.getOrDefault(period + CURRENT_SUFFIX, 0);
    int activeUsersLastRange = activeUsers.getOrDefault(period + PREVIOUS_SUFFIX, 0);

    String bodyText = "";
    if (activeUsersLastRange > 0) {
      double percentChange =
          (double) (activeUsersThisRange - activeUsersLastRange)
              / (double) activeUsersLastRange
              * 100;

      String directionChange = percentChange > 0 ? "increase" : "decrease";

      bodyText =
          Double.isInfinite(percentChange)
              ? ""
              : String.format(changeString, percentChange, directionChange);
    }
    return Highlight.builder()
        .setTitle(title)
        .setValue(activeUsersThisRange)
        .setBody(bodyText)
        .build();
  }

  private Optional<Highlight> buildEntityMetadataHighlight(
      final String title, final EntityType entityType, final EntityStats stats) {
    // If there are no entities for the type, do not show the highlight
    if (stats == null || stats.getTotal() == 0) {
      return Optional.empty();
    }

    int numEntities = stats.getTotal();
    double percentWithOwners = 100.0 * stats.countWithFacet(HAS_OWNERS) / numEntities;
    double percentWithTags = 100.0 * stats.countWithFacet(HAS_TAGS) / numEntities;
    double percentWithGlossaryTerms =
        100.0 * stats.countWithFacet(HAS_GLOSSARY_TERMS) / numEntities;
    double percentWithDescription = 100.0 * stats.countWithFacet(HAS_DESCRIPTION) / numEntities;

    String bodyText;
    if (entityType == EntityType.DOMAIN) {
      // Don't show percent with domain when asking for stats regarding domains
      bodyText =
          String.format(
              "%.2f%% have owners, %.2f%% have tags, %.2f%% have glossary terms, %.2f%% have description!",
              percentWithOwners, percentWithTags, percentWithGlossaryTerms, percentWithDescription);
    } else {
      double percentWithDomains = 100.0 * stats.countWithFacet(HAS_DOMAIN) / numEntities;
      bodyText =
          String.format(
              "%.2f%% have owners, %.2f%% have tags, %.2f%% have glossary terms, %.2f%% have description, %.2f%% have domain assigned!",
              percentWithOwners,
              percentWithTags,
              percentWithGlossaryTerms,
              percentWithDescription,
              percentWithDomains);
    }
    return Optional.of(
        Highlight.builder().setTitle(title).setValue(numEntities).setBody(bodyText).build());
  }
}

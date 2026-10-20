package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Closed predicate set for ladder select and stop. Select is a conjunction. Stop is a disjunction.
 */
public final class SearchTemplateLadderPredicates {
  public static final Set<String> SHAPES =
      Set.of(
          "scoreSort",
          "pageSize",
          "notColumnFilter",
          "singleTerm",
          "multiWord",
          "dotted",
          "spacelessIdentifier",
          "identity");

  public static final Set<String> SELECT = SHAPES;

  private SearchTemplateLadderPredicates() {}

  public static boolean selectMatches(
      @Nullable SearchTemplateLadder ladder,
      @Nonnull String query,
      @Nullable Filter filter,
      @Nullable List<SortCriterion> sort,
      @Nullable Integer size) {
    if (ladder == null
        || !ladder.hasSelect()
        || ladder.getSelect() == null
        || ladder.getSelect().isEmpty()) {
      return true;
    }
    for (String name : ladder.getSelect()) {
      if (!shape(name, query, filter, sort, size)) {
        return false;
      }
    }
    return true;
  }

  /** An empty stop keeps the rung. A miss with no {@code next} also keeps it. */
  public static boolean stopMatches(
      @Nullable SearchTemplateLadder ladder,
      long numEntities,
      @Nonnull String query,
      @Nullable Filter filter,
      @Nullable List<SortCriterion> sort,
      @Nullable Integer size) {
    if (ladder == null
        || !ladder.hasStop()
        || ladder.getStop() == null
        || ladder.getStop().isEmpty()) {
      return true;
    }
    for (SearchTemplateLadderStop condition : ladder.getStop()) {
      if (condition.hasMinHits()
          && condition.getMinHits() != null
          && numEntities >= condition.getMinHits()) {
        return true;
      }
      if (condition.hasEmptyWhen()
          && condition.getEmptyWhen() != null
          && numEntities == 0
          && shape(condition.getEmptyWhen(), query, filter, sort, size)) {
        return true;
      }
    }
    return false;
  }

  public static boolean shape(
      @Nonnull String name,
      @Nonnull String query,
      @Nullable Filter filter,
      @Nullable List<SortCriterion> sort,
      @Nullable Integer size) {
    SearchTemplateQueryShape parsed = SearchTemplateQueryShape.of(query);
    return switch (name) {
      case "scoreSort" -> SearchTemplateLight.relevanceSort(sort);
      case "pageSize" -> size == null || size > 0;
      case "notColumnFilter" -> !SearchTemplateLight.hasColumnNameFilter(filter);
      case "singleTerm" -> !query.isBlank() && query.trim().indexOf(' ') < 0;
      case "multiWord" -> parsed.multiWord;
      case "dotted" -> parsed.dotted;
      case "spacelessIdentifier" -> SearchTemplateQueryShape.stopOnEmptyLight(query);
      case "identity" -> SearchTemplateQueryShape.isIdentity(query);
      default -> false;
    };
  }
}

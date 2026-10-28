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
 * A single leading {@code -} inverts a predicate.
 */
public final class SearchTemplateLadderPredicates {
  public static final Set<String> SHAPES =
      Set.of(
          "scoreSort",
          "pageSize",
          "columnFilter",
          "singleTerm",
          "multiWord",
          "dotted",
          "spacelessIdentifier",
          "identity",
          "longQuery");

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

  /**
   * The predicate name after one leading {@code -}, or null when the name is not in the closed set.
   * {@code --longQuery} and a bare {@code -} are rejected.
   */
  @Nullable
  public static String base(@Nullable String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    String predicate = name;
    if (predicate.charAt(0) == '-') {
      if (predicate.length() == 1 || predicate.charAt(1) == '-') {
        return null;
      }
      predicate = predicate.substring(1);
    }
    return SHAPES.contains(predicate) ? predicate : null;
  }

  public static boolean shape(
      @Nonnull String name,
      @Nonnull String query,
      @Nullable Filter filter,
      @Nullable List<SortCriterion> sort,
      @Nullable Integer size) {
    String predicate = base(name);
    if (predicate == null) {
      return false;
    }
    SearchTemplateQueryShape parsed = SearchTemplateQueryShape.of(query);
    boolean value =
        switch (predicate) {
          case "scoreSort" -> SearchTemplateLight.relevanceSort(sort);
          case "pageSize" -> size == null || size > 0;
          case "columnFilter" -> SearchTemplateLight.hasColumnNameFilter(filter);
          case "singleTerm" -> !query.isBlank() && query.trim().indexOf(' ') < 0;
          case "multiWord" -> parsed.multiWord;
          case "dotted" -> parsed.dotted;
          case "spacelessIdentifier" -> SearchTemplateQueryShape.stopOnEmptyLight(query);
          case "identity" -> SearchTemplateQueryShape.isIdentity(query);
          case "longQuery" -> parsed.longQuery;
          default -> false;
        };
    return name.charAt(0) == '-' ? !value : value;
  }
}

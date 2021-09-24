package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;


public class ESUtils {

  private static final String DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD = "urn";

  /*
   * Refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html for list of reserved
   * characters in an Elasticsearch regular expression.
   */
  private static final String ELASTICSEARCH_REGEXP_RESERVED_CHARACTERS = "?+*|{}[]()";

  private ESUtils() {

  }

  /**
   * Constructs the filter query given filter map.
   *
   * <p>Multiple values can be selected for a filter, and it is currently modeled as string separated by comma
   *
   * @param filter the search filter
   * @return built filter query
   */
  @Nonnull
  public static BoolQueryBuilder buildFilterQuery(@Nullable Filter filter) {
    BoolQueryBuilder boolFilter = new BoolQueryBuilder();
    if (filter == null) {
      return boolFilter;
    }
    for (Criterion criterion : filter.getCriteria()) {
      boolFilter.must(getQueryBuilderFromCriterionForSearch(criterion));
    }
    return boolFilter;
  }

  /**
   * Builds search query using criterion.
   * This method is similar to SearchUtils.getQueryBuilderFromCriterion().
   * The only difference is this method use match query instead of term query for EQUAL.
   *
   * @param criterion {@link Criterion} single criterion which contains field, value and a comparison operator
   * @return QueryBuilder
   */
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterionForSearch(@Nonnull Criterion criterion) {
    final Condition condition = criterion.getCondition();
    if (condition == Condition.EQUAL) {
      BoolQueryBuilder filters = new BoolQueryBuilder();
      filters.should(QueryBuilders.matchQuery(criterion.getField(), criterion.getValue().trim()));
      return filters;
    } else {
      return SearchUtils.getQueryBuilderFromCriterion(criterion);
    }
  }

  /**
   * Populates source field of search query with the sort order as per the criterion provided.
   *
   * <p>
   * If no sort criterion is provided then the default sorting criterion is chosen which is descending order of score
   * Furthermore to resolve conflicts, the results are further sorted by ascending order of urn
   * If the input sort criterion is urn itself, then no additional sort criterion is applied as there will be no conflicts.
   * </p>
   *
   * @param searchSourceBuilder {@link SearchSourceBuilder} that needs to be populated with sort order
   * @param sortCriterion {@link SortCriterion} to be applied to the search results
   */
  public static void buildSortOrder(@Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable SortCriterion sortCriterion) {
    if (sortCriterion == null) {
      searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
    } else {
      final SortOrder esSortOrder =
          (sortCriterion.getOrder() == com.linkedin.metadata.query.SortOrder.ASCENDING) ? SortOrder.ASC
              : SortOrder.DESC;
      searchSourceBuilder.sort(new FieldSortBuilder(sortCriterion.getField()).order(esSortOrder));
    }
    if (sortCriterion == null || !sortCriterion.getField().equals(DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD)) {
      searchSourceBuilder.sort(new FieldSortBuilder(DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD).order(SortOrder.ASC));
    }
  }

  /**
   * Escapes the Elasticsearch reserved characters in the given input string.
   *
   * @param input input string
   * @return input string in which reserved characters are escaped
   */
  @Nonnull
  public static String escapeReservedCharacters(@Nonnull String input) {
    for (char reservedChar : ELASTICSEARCH_REGEXP_RESERVED_CHARACTERS.toCharArray()) {
      input = input.replace(String.valueOf(reservedChar), "\\" + reservedChar);
    }
    return input;
  }
}
package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import static com.linkedin.metadata.search.utils.SearchUtils.*;


@Slf4j
public class ESUtils {

  private static final String DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD = "urn";

  public static final String KEYWORD_SUFFIX = ".keyword";

  /*
   * Refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html for list of reserved
   * characters in an Elasticsearch regular expression.
   */
  private static final String ELASTICSEARCH_REGEXP_RESERVED_CHARACTERS = "?+*|{}[]()#@&<>~";

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
    BoolQueryBuilder orQueryBuilder = new BoolQueryBuilder();
    if (filter == null) {
      return orQueryBuilder;
    }
    if (filter.getOr() != null) {
      // If caller is using the new Filters API, build boolean query from that.
      filter.getOr().forEach(or -> {
        final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
        or.getAnd().forEach(criterion -> {
          if (!criterion.getValue().trim().isEmpty()) {
            andQueryBuilder.must(getQueryBuilderFromCriterionForSearch(criterion));
          }
        });
        orQueryBuilder.should(andQueryBuilder);
      });
    } else if (filter.getCriteria() != null) {
      // Otherwise, build boolean query from the deprecated "criteria" field.
      log.warn("Received query Filter with a deprecated field 'criteria'. Use 'or' instead.");
      final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
      filter.getCriteria().forEach(criterion -> {
        if (!criterion.getValue().trim().isEmpty()) {
          andQueryBuilder.must(getQueryBuilderFromCriterionForSearch(criterion));
        }
      });
      orQueryBuilder.should(andQueryBuilder);
    }
    return orQueryBuilder;
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

      // TODO(https://github.com/linkedin/datahub-gma/issues/51): support multiple values a field can take without using
      // delimiters like comma. This is a hack to support equals with URN that has a comma in it.
      if (SearchUtils.isUrn(criterion.getValue())) {
        filters.should(QueryBuilders.matchQuery(criterion.getField(), criterion.getValue().trim()));
        return filters;
      }

      Arrays.stream(criterion.getValue().trim().split("\\s*,\\s*"))
          .forEach(elem -> filters.should(QueryBuilders.matchQuery(criterion.getField(), elem)));
      return filters;
    } else {
      return getQueryBuilderFromCriterion(criterion);
    }
  }

  /**
   * Builds search query given a {@link Criterion}, containing field, value and association/condition between the two.
   *
   * <p>If the condition between a field and value (specified in {@link Criterion}) is EQUAL, we construct a Terms query.
   * In this case, a field can take multiple values, specified using comma as a delimiter - this method will split
   * tokens accordingly. This is done because currently there is no support of associating two different {@link Criterion}
   * in a {@link Filter} with an OR operator - default operator is AND.
   *
   * <p>This approach of supporting multiple values using comma as delimiter, prevents us from specifying a value that has comma
   * as one of it's characters. This is particularly true when one of the values is an urn e.g. "urn:li:example:(1,2,3)".
   * Hence we do not split the value (using comma as delimiter) if the value starts with "urn:li:".
   * TODO(https://github.com/linkedin/datahub-gma/issues/51): support multiple values a field can take without using
   * delimiters like comma.
   *
   * <p>If the condition between a field and value is not the same as EQUAL, a Range query is constructed. This
   * condition does not support multiple values for the same field.
   *
   * <p>When CONTAIN, START_WITH and END_WITH conditions are used, the underlying logic is using wildcard query which is
   * not performant according to ES. For details, please refer to:
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html#wildcard-query-field-params
   *
   * @param criterion {@link Criterion} single criterion which contains field, value and a comparison operator
   */
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterion(@Nonnull Criterion criterion) {
    final Condition condition = criterion.getCondition();
    if (condition == Condition.EQUAL) {
      // TODO(https://github.com/linkedin/datahub-gma/issues/51): support multiple values a field can take without using
      // delimiters like comma. This is a hack to support equals with URN that has a comma in it.
      if (isUrn(criterion.getValue())) {
        return QueryBuilders.termsQuery(criterion.getField(), criterion.getValue().trim());
      }
      return QueryBuilders.termsQuery(criterion.getField(), criterion.getValue().trim().split("\\s*,\\s*"));
    } else if (condition == Condition.GREATER_THAN) {
      return QueryBuilders.rangeQuery(criterion.getField()).gt(criterion.getValue().trim());
    } else if (condition == Condition.GREATER_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(criterion.getField()).gte(criterion.getValue().trim());
    } else if (condition == Condition.LESS_THAN) {
      return QueryBuilders.rangeQuery(criterion.getField()).lt(criterion.getValue().trim());
    } else if (condition == Condition.LESS_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(criterion.getField()).lte(criterion.getValue().trim());
    } else if (condition == Condition.CONTAIN) {
      return QueryBuilders.wildcardQuery(criterion.getField(),
          "*" + ESUtils.escapeReservedCharacters(criterion.getValue().trim()) + "*");
    } else if (condition == Condition.START_WITH) {
      return QueryBuilders.wildcardQuery(criterion.getField(),
          ESUtils.escapeReservedCharacters(criterion.getValue().trim()) + "*");
    } else if (condition == Condition.END_WITH) {
      return QueryBuilders.wildcardQuery(criterion.getField(),
          "*" + ESUtils.escapeReservedCharacters(criterion.getValue().trim()));
    }
    throw new UnsupportedOperationException("Unsupported condition: " + condition);
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
          (sortCriterion.getOrder() == com.linkedin.metadata.query.filter.SortOrder.ASCENDING) ? SortOrder.ASC
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
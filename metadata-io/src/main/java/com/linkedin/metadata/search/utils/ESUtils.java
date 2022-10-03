package com.linkedin.metadata.search.utils;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
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

import static com.linkedin.metadata.search.utils.SearchUtils.isUrn;


@Slf4j
public class ESUtils {

  private static final String DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD = "urn";

  public static final String KEYWORD_SUFFIX = ".keyword";
  public static final int MAX_RESULT_SIZE = 10000;

  // we use this to make sure we filter for editable & non-editable fields
  public static final String[][] EDITABLE_FIELD_TO_QUERY_PAIRS = {
      {"fieldGlossaryTags", "editedFieldGlossaryTags"},
      {"fieldGlossaryTerms", "editedFieldGlossaryTerms"},
      {"fieldDescriptions", "editedFieldDescriptions"},
      {"description", "editedDescription"},
  };

  public static final Set<String> BOOLEAN_FIELDS = ImmutableSet.of(
      "removed"
  );

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
    BoolQueryBuilder finalQueryBuilder = QueryBuilders.boolQuery();
    if (filter == null) {
      return finalQueryBuilder;
    }
    if (filter.getOr() != null) {
      // If caller is using the new Filters API, build boolean query from that.
      filter.getOr().forEach(or -> finalQueryBuilder.should(ESUtils.buildConjunctiveFilterQuery(or)));
    } else if (filter.getCriteria() != null) {
      // Otherwise, build boolean query from the deprecated "criteria" field.
      log.warn("Received query Filter with a deprecated field 'criteria'. Use 'or' instead.");
      final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
      filter.getCriteria().forEach(criterion -> {
        if (!criterion.getValue().trim().isEmpty() || criterion.hasValues()
                || criterion.getCondition() == Condition.IS_NULL) {
          andQueryBuilder.must(getQueryBuilderFromCriterion(criterion));
        }
      });
      finalQueryBuilder.should(andQueryBuilder);
    }
    return finalQueryBuilder;
  }

  @Nonnull
  public static BoolQueryBuilder buildConjunctiveFilterQuery(@Nonnull ConjunctiveCriterion conjunctiveCriterion) {
    final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
    conjunctiveCriterion.getAnd().forEach(criterion -> {
      if (!criterion.getValue().trim().isEmpty() || criterion.hasValues()
              || criterion.getCondition() == Condition.IS_NULL) {
        if (!criterion.isNegated()) {
          andQueryBuilder.must(getQueryBuilderFromCriterion(criterion));
        } else {
          andQueryBuilder.mustNot(getQueryBuilderFromCriterion(criterion));
        }
      }
    });
    return andQueryBuilder;
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
   * TODO(https://github.com/datahub-project/datahub-gma/issues/51): support multiple values a field can take without using
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
    String fieldName = toFacetField(criterion.getField());

    Optional<String[]> pairMatch = Arrays.stream(EDITABLE_FIELD_TO_QUERY_PAIRS)
        .filter(pair -> Arrays.stream(pair).anyMatch(pairValue -> pairValue.equals(fieldName)))
        .findFirst();

    if (pairMatch.isPresent()) {
      final BoolQueryBuilder orQueryBuilder = new BoolQueryBuilder();
      String[] pairMatchValue = pairMatch.get();
      for (String field: pairMatchValue) {
        Criterion criterionToQuery = new Criterion();
        criterionToQuery.setCondition(criterion.getCondition());
        criterionToQuery.setNegated(criterion.isNegated());
        criterionToQuery.setValue(criterion.getValue());
        criterionToQuery.setField(field + KEYWORD_SUFFIX);
        orQueryBuilder.should(getQueryBuilderFromCriterionForSingleField(criterionToQuery));
      }
      return orQueryBuilder;
    }

    return getQueryBuilderFromCriterionForSingleField(criterion);
  }
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterionForSingleField(@Nonnull Criterion criterion) {
    final Condition condition = criterion.getCondition();
    String fieldName = toFacetField(criterion.getField());

    if (condition == Condition.EQUAL) {
      // If values is set, use terms query to match one of the values
      if (!criterion.getValues().isEmpty()) {
        if (BOOLEAN_FIELDS.contains(fieldName) && criterion.getValues().size() == 1) {
          return QueryBuilders.termQuery(fieldName, Boolean.parseBoolean(criterion.getValues().get(0)));
        }
        return QueryBuilders.termsQuery(criterion.getField(), criterion.getValues());
      }

      // TODO(https://github.com/datahub-project/datahub-gma/issues/51): support multiple values a field can take without using
      // delimiters like comma. This is a hack to support equals with URN that has a comma in it.
      if (isUrn(criterion.getValue())) {
        return QueryBuilders.matchQuery(criterion.getField(), criterion.getValue().trim());
      }
      BoolQueryBuilder filters = new BoolQueryBuilder();
      Arrays.stream(criterion.getValue().trim().split("\\s*,\\s*"))
          .forEach(elem -> filters.should(QueryBuilders.matchQuery(criterion.getField(), elem)));
      return filters;
    } else if (condition == Condition.IS_NULL) {
      return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(criterion.getField()));
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

  @Nullable
  public static String toFacetField(@Nonnull final String filterField) {
    return filterField.replace(ESUtils.KEYWORD_SUFFIX, "");
  }
}

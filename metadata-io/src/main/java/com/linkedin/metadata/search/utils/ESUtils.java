package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig.KEYWORD_FIELDS;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig.PATH_HIERARCHY_FIELDS;
import static com.linkedin.metadata.search.utils.SearchUtils.isUrn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.SuggestBuilders;
import org.opensearch.search.suggest.SuggestionBuilder;
import org.opensearch.search.suggest.term.TermSuggestionBuilder;

/** TODO: Add more robust unit tests for this critical class. */
@Slf4j
public class ESUtils {

  private static final String DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD = "urn";
  public static final String KEYWORD_ANALYZER = "keyword";
  public static final String KEYWORD_SUFFIX = ".keyword";
  public static final int MAX_RESULT_SIZE = 10000;
  public static final String OPAQUE_ID_HEADER = "X-Opaque-Id";
  public static final String HEADER_VALUE_DELIMITER = "|";

  // Field types
  public static final String KEYWORD_FIELD_TYPE = "keyword";
  public static final String BOOLEAN_FIELD_TYPE = "boolean";
  public static final String DATE_FIELD_TYPE = "date";
  public static final String DOUBLE_FIELD_TYPE = "double";
  public static final String LONG_FIELD_TYPE = "long";
  public static final String OBJECT_FIELD_TYPE = "object";
  public static final String TEXT_FIELD_TYPE = "text";
  public static final String TOKEN_COUNT_FIELD_TYPE = "token_count";
  // End of field types

  public static final Set<SearchableAnnotation.FieldType> FIELD_TYPES_STORED_AS_KEYWORD =
      Set.of(
          SearchableAnnotation.FieldType.KEYWORD,
          SearchableAnnotation.FieldType.TEXT,
          SearchableAnnotation.FieldType.TEXT_PARTIAL,
          SearchableAnnotation.FieldType.WORD_GRAM);
  public static final Set<SearchableAnnotation.FieldType> FIELD_TYPES_STORED_AS_TEXT =
      Set.of(
          SearchableAnnotation.FieldType.BROWSE_PATH,
          SearchableAnnotation.FieldType.BROWSE_PATH_V2,
          SearchableAnnotation.FieldType.URN,
          SearchableAnnotation.FieldType.URN_PARTIAL);
  public static final String ENTITY_NAME_FIELD = "_entityName";
  public static final String NAME_SUGGESTION = "nameSuggestion";

  // we use this to make sure we filter for editable & non-editable fields. Also expands out
  // top-level properties
  // to field level properties
  public static final Map<String, List<String>> FIELDS_TO_EXPANDED_FIELDS_LIST =
      new HashMap<String, List<String>>() {
        {
          put("tags", ImmutableList.of("tags", "fieldTags", "editedFieldTags"));
          put(
              "glossaryTerms",
              ImmutableList.of("glossaryTerms", "fieldGlossaryTerms", "editedFieldGlossaryTerms"));
          put("fieldTags", ImmutableList.of("fieldTags", "editedFieldTags"));
          put(
              "fieldGlossaryTerms",
              ImmutableList.of("fieldGlossaryTerms", "editedFieldGlossaryTerms"));
          put(
              "fieldDescriptions",
              ImmutableList.of("fieldDescriptions", "editedFieldDescriptions"));
          put("description", ImmutableList.of("description", "editedDescription"));
        }
      };

  public static final Set<String> BOOLEAN_FIELDS = ImmutableSet.of("removed");

  /*
   * Refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html for list of reserved
   * characters in an Elasticsearch regular expression.
   */
  private static final String ELASTICSEARCH_REGEXP_RESERVED_CHARACTERS = "?+*|{}[]()#@&<>~";

  private ESUtils() {}

  /**
   * Constructs the filter query given filter map.
   *
   * <p>Multiple values can be selected for a filter, and it is currently modeled as string
   * separated by comma
   *
   * @param filter the search filter
   * @param isTimeseries whether filtering on timeseries index which has differing field type
   *     conventions
   * @return built filter query
   */
  @Nonnull
  public static BoolQueryBuilder buildFilterQuery(@Nullable Filter filter, boolean isTimeseries) {
    BoolQueryBuilder finalQueryBuilder = QueryBuilders.boolQuery();
    if (filter == null) {
      return finalQueryBuilder;
    }
    if (filter.getOr() != null) {
      // If caller is using the new Filters API, build boolean query from that.
      filter
          .getOr()
          .forEach(
              or ->
                  finalQueryBuilder.should(ESUtils.buildConjunctiveFilterQuery(or, isTimeseries)));
    } else if (filter.getCriteria() != null) {
      // Otherwise, build boolean query from the deprecated "criteria" field.
      log.warn("Received query Filter with a deprecated field 'criteria'. Use 'or' instead.");
      final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
      filter
          .getCriteria()
          .forEach(
              criterion -> {
                if (!criterion.getValue().trim().isEmpty()
                    || criterion.hasValues()
                    || criterion.getCondition() == Condition.IS_NULL) {
                  andQueryBuilder.must(getQueryBuilderFromCriterion(criterion, isTimeseries));
                }
              });
      finalQueryBuilder.should(andQueryBuilder);
    }
    return finalQueryBuilder;
  }

  @Nonnull
  public static BoolQueryBuilder buildConjunctiveFilterQuery(
      @Nonnull ConjunctiveCriterion conjunctiveCriterion, boolean isTimeseries) {
    final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
    conjunctiveCriterion
        .getAnd()
        .forEach(
            criterion -> {
              if (Set.of(Condition.EXISTS, Condition.IS_NULL).contains(criterion.getCondition())
                  || !criterion.getValue().trim().isEmpty()
                  || criterion.hasValues()) {
                if (!criterion.isNegated()) {
                  // `filter` instead of `must` (enables caching and bypasses scoring)
                  andQueryBuilder.filter(getQueryBuilderFromCriterion(criterion, isTimeseries));
                } else {
                  andQueryBuilder.mustNot(getQueryBuilderFromCriterion(criterion, isTimeseries));
                }
              }
            });
    return andQueryBuilder;
  }

  /**
   * Builds search query given a {@link Criterion}, containing field, value and
   * association/condition between the two.
   *
   * <p>If the condition between a field and value (specified in {@link Criterion}) is EQUAL, we
   * construct a Terms query. In this case, a field can take multiple values, specified using comma
   * as a delimiter - this method will split tokens accordingly. This is done because currently
   * there is no support of associating two different {@link Criterion} in a {@link Filter} with an
   * OR operator - default operator is AND.
   *
   * <p>This approach of supporting multiple values using comma as delimiter, prevents us from
   * specifying a value that has comma as one of it's characters. This is particularly true when one
   * of the values is an urn e.g. "urn:li:example:(1,2,3)". Hence we do not split the value (using
   * comma as delimiter) if the value starts with "urn:li:".
   * TODO(https://github.com/datahub-project/datahub-gma/issues/51): support multiple values a field
   * can take without using delimiters like comma.
   *
   * <p>If the condition between a field and value is not the same as EQUAL, a Range query is
   * constructed. This condition does not support multiple values for the same field.
   *
   * <p>When CONTAIN, START_WITH and END_WITH conditions are used, the underlying logic is using
   * wildcard query which is not performant according to ES. For details, please refer to:
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html#wildcard-query-field-params
   *
   * @param criterion {@link Criterion} single criterion which contains field, value and a
   *     comparison operator
   */
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterion(
      @Nonnull final Criterion criterion, boolean isTimeseries) {
    final String fieldName = toFacetField(criterion.getField());

    /*
     * Check the field-name for a "sibling" field, or one which should ALWAYS
     * be matched in disjunction with the targeted field (OR).
     *
     * This essentially equates to filter expansion based on a particular field.
     * First we handle this expansion, if required, otherwise we build the filter as usual
     * without expansion.
     */
    final Optional<List<String>> maybeFieldToExpand =
        Optional.ofNullable(FIELDS_TO_EXPANDED_FIELDS_LIST.get(fieldName));

    if (maybeFieldToExpand.isPresent()) {
      return getQueryBuilderFromCriterionForFieldToExpand(
          maybeFieldToExpand.get(), criterion, isTimeseries);
    }

    return getQueryBuilderFromCriterionForSingleField(criterion, isTimeseries);
  }

  public static String getElasticTypeForFieldType(SearchableAnnotation.FieldType fieldType) {
    if (FIELD_TYPES_STORED_AS_KEYWORD.contains(fieldType)) {
      return KEYWORD_FIELD_TYPE;
    } else if (FIELD_TYPES_STORED_AS_TEXT.contains(fieldType)) {
      return TEXT_FIELD_TYPE;
    } else if (fieldType == SearchableAnnotation.FieldType.BOOLEAN) {
      return BOOLEAN_FIELD_TYPE;
    } else if (fieldType == SearchableAnnotation.FieldType.COUNT) {
      return LONG_FIELD_TYPE;
    } else if (fieldType == SearchableAnnotation.FieldType.DATETIME) {
      return DATE_FIELD_TYPE;
    } else if (fieldType == SearchableAnnotation.FieldType.OBJECT) {
      return OBJECT_FIELD_TYPE;
    } else {
      log.warn("FieldType {} has no mappings implemented", fieldType);
      return null;
    }
  }

  /**
   * Populates source field of search query with the sort order as per the criterion provided.
   *
   * <p>If no sort criterion is provided then the default sorting criterion is chosen which is
   * descending order of score Furthermore to resolve conflicts, the results are further sorted by
   * ascending order of urn If the input sort criterion is urn itself, then no additional sort
   * criterion is applied as there will be no conflicts. When sorting, set the unmappedType param to
   * arbitrary "keyword" so we essentially ignore sorting where indices do not have the field we are
   * sorting on.
   *
   * @param searchSourceBuilder {@link SearchSourceBuilder} that needs to be populated with sort
   *     order
   * @param sortCriterion {@link SortCriterion} to be applied to the search results
   */
  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable SortCriterion sortCriterion,
      List<EntitySpec> entitySpecs) {
    if (sortCriterion == null) {
      searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
    } else {
      Optional<SearchableAnnotation.FieldType> fieldTypeForDefault = Optional.empty();
      for (EntitySpec entitySpec : entitySpecs) {
        List<SearchableFieldSpec> fieldSpecs = entitySpec.getSearchableFieldSpecs();
        for (SearchableFieldSpec fieldSpec : fieldSpecs) {
          SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
          if (annotation.getFieldName().equals(sortCriterion.getField())
              || annotation.getFieldNameAliases().contains(sortCriterion.getField())) {
            fieldTypeForDefault = Optional.of(fieldSpec.getSearchableAnnotation().getFieldType());
            break;
          }
        }
        if (fieldTypeForDefault.isPresent()) {
          break;
        }
      }
      if (fieldTypeForDefault.isEmpty()) {
        log.warn(
            "Sort criterion field "
                + sortCriterion.getField()
                + " was not found in any entity spec to be searched");
      }
      final SortOrder esSortOrder =
          (sortCriterion.getOrder() == com.linkedin.metadata.query.filter.SortOrder.ASCENDING)
              ? SortOrder.ASC
              : SortOrder.DESC;
      FieldSortBuilder sortBuilder =
          new FieldSortBuilder(sortCriterion.getField()).order(esSortOrder);
      if (fieldTypeForDefault.isPresent()) {
        String esFieldtype = getElasticTypeForFieldType(fieldTypeForDefault.get());
        if (esFieldtype != null) {
          sortBuilder.unmappedType(esFieldtype);
        }
      }
      searchSourceBuilder.sort(sortBuilder);
    }
    if (sortCriterion == null
        || !sortCriterion.getField().equals(DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD)) {
      searchSourceBuilder.sort(
          new FieldSortBuilder(DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD).order(SortOrder.ASC));
    }
  }

  /**
   * Populates source field of search query with the suggestions query so that we get search
   * suggestions back. Right now we are only supporting suggestions based on the virtual _entityName
   * field alias.
   */
  public static void buildNameSuggestions(
      @Nonnull SearchSourceBuilder searchSourceBuilder, @Nullable String textInput) {
    SuggestionBuilder<TermSuggestionBuilder> builder =
        SuggestBuilders.termSuggestion(ENTITY_NAME_FIELD).text(textInput);
    SuggestBuilder suggestBuilder = new SuggestBuilder();
    suggestBuilder.addSuggestion(NAME_SUGGESTION, builder);
    searchSourceBuilder.suggest(suggestBuilder);
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

  @Nonnull
  public static String toFacetField(@Nonnull final String filterField) {
    return filterField.replace(ESUtils.KEYWORD_SUFFIX, "");
  }

  @Nonnull
  public static String toKeywordField(
      @Nonnull final String filterField, @Nonnull final boolean skipKeywordSuffix) {
    return skipKeywordSuffix
            || KEYWORD_FIELDS.contains(filterField)
            || PATH_HIERARCHY_FIELDS.contains(filterField)
            || filterField.contains(".")
        ? filterField
        : filterField + ESUtils.KEYWORD_SUFFIX;
  }

  public static RequestOptions buildReindexTaskRequestOptions(
      String version, String indexName, String tempIndexName) {
    return RequestOptions.DEFAULT.toBuilder()
        .addHeader(OPAQUE_ID_HEADER, getOpaqueIdHeaderValue(version, indexName, tempIndexName))
        .build();
  }

  public static String getOpaqueIdHeaderValue(
      String version, String indexName, String tempIndexName) {
    return String.join(HEADER_VALUE_DELIMITER, version, indexName, tempIndexName);
  }

  public static boolean prefixMatch(String id, String version, String indexName) {
    return Optional.ofNullable(id)
        .map(t -> t.startsWith(String.join(HEADER_VALUE_DELIMITER, version, indexName)))
        .orElse(false);
  }

  public static String extractTargetIndex(String id) {
    return id.split("[" + HEADER_VALUE_DELIMITER + "]", 3)[2];
  }

  public static void setSearchAfter(
      SearchSourceBuilder searchSourceBuilder,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nullable String keepAlive) {
    if (sort != null && sort.length > 0) {
      searchSourceBuilder.searchAfter(sort);
    }
    if (StringUtils.isNotBlank(pitId) && keepAlive != null) {
      PointInTimeBuilder pointInTimeBuilder = new PointInTimeBuilder(pitId);
      pointInTimeBuilder.setKeepAlive(TimeValue.parseTimeValue(keepAlive, "keepAlive"));
      searchSourceBuilder.pointInTimeBuilder(pointInTimeBuilder);
    }
  }

  @Nonnull
  private static QueryBuilder getQueryBuilderFromCriterionForFieldToExpand(
      @Nonnull final List<String> fields,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries) {
    final BoolQueryBuilder orQueryBuilder = new BoolQueryBuilder();
    for (String field : fields) {
      Criterion criterionToQuery = new Criterion();
      criterionToQuery.setCondition(criterion.getCondition());
      criterionToQuery.setNegated(criterion.isNegated());
      if (criterion.hasValues()) {
        criterionToQuery.setValues(criterion.getValues());
      }
      if (criterion.hasValue()) {
        criterionToQuery.setValue(criterion.getValue());
      }
      criterionToQuery.setField(toKeywordField(field, isTimeseries));
      orQueryBuilder.should(
          getQueryBuilderFromCriterionForSingleField(criterionToQuery, isTimeseries));
    }
    return orQueryBuilder;
  }

  @Nonnull
  private static QueryBuilder getQueryBuilderFromCriterionForSingleField(
      @Nonnull Criterion criterion, @Nonnull boolean isTimeseries) {
    final Condition condition = criterion.getCondition();
    final String fieldName = toFacetField(criterion.getField());

    if (condition == Condition.IS_NULL) {
      return QueryBuilders.boolQuery()
          .mustNot(QueryBuilders.existsQuery(criterion.getField()))
          .queryName(fieldName);
    } else if (condition == Condition.EXISTS) {
      return QueryBuilders.boolQuery()
          .must(QueryBuilders.existsQuery(criterion.getField()))
          .queryName(fieldName);
    } else if (criterion.hasValues() || criterion.hasValue()) {
      if (condition == Condition.EQUAL) {
        return buildEqualsConditionFromCriterion(fieldName, criterion, isTimeseries);
        // TODO: Support multi-match on the following operators (using new 'values' field)
      } else if (condition == Condition.GREATER_THAN) {
        return QueryBuilders.rangeQuery(criterion.getField())
            .gt(criterion.getValue().trim())
            .queryName(fieldName);
      } else if (condition == Condition.GREATER_THAN_OR_EQUAL_TO) {
        return QueryBuilders.rangeQuery(criterion.getField())
            .gte(criterion.getValue().trim())
            .queryName(fieldName);
      } else if (condition == Condition.LESS_THAN) {
        return QueryBuilders.rangeQuery(criterion.getField())
            .lt(criterion.getValue().trim())
            .queryName(fieldName);
      } else if (condition == Condition.LESS_THAN_OR_EQUAL_TO) {
        return QueryBuilders.rangeQuery(criterion.getField())
            .lte(criterion.getValue().trim())
            .queryName(fieldName);
      } else if (condition == Condition.CONTAIN) {
        return QueryBuilders.wildcardQuery(
                toKeywordField(criterion.getField(), isTimeseries),
                "*" + ESUtils.escapeReservedCharacters(criterion.getValue().trim()) + "*")
            .queryName(fieldName);
      } else if (condition == Condition.START_WITH) {
        return QueryBuilders.wildcardQuery(
                toKeywordField(criterion.getField(), isTimeseries),
                ESUtils.escapeReservedCharacters(criterion.getValue().trim()) + "*")
            .queryName(fieldName);
      } else if (condition == Condition.END_WITH) {
        return QueryBuilders.wildcardQuery(
                toKeywordField(criterion.getField(), isTimeseries),
                "*" + ESUtils.escapeReservedCharacters(criterion.getValue().trim()))
            .queryName(fieldName);
      }
    }
    throw new UnsupportedOperationException("Unsupported condition: " + condition);
  }

  private static QueryBuilder buildEqualsConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries) {
    /*
     * If the newer 'values' field of Criterion.pdl is set, then we
     * handle using the following code to allow multi-match.
     */
    if (!criterion.getValues().isEmpty()) {
      return buildEqualsConditionFromCriterionWithValues(fieldName, criterion, isTimeseries);
    }
    /*
     * Otherwise, we are likely using the deprecated 'value' field.
     * We handle using the legacy code path below.
     */
    return buildEqualsFromCriterionWithValue(fieldName, criterion, isTimeseries);
  }

  /**
   * Builds an instance of {@link QueryBuilder} representing an EQUALS condition which was created
   * using the new multi-match 'values' field of Criterion.pdl model.
   */
  private static QueryBuilder buildEqualsConditionFromCriterionWithValues(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries) {
    if (BOOLEAN_FIELDS.contains(fieldName) && criterion.getValues().size() == 1) {
      // Handle special-cased Boolean fields.
      // here we special case boolean fields we recognize the names of and hard-cast
      // the first provided value to a boolean to do the comparison.
      // Ideally, we should detect the type of the field from the entity-registry in order
      // to determine how to cast.
      return QueryBuilders.termQuery(fieldName, Boolean.parseBoolean(criterion.getValues().get(0)))
          .queryName(fieldName);
    }
    return QueryBuilders.termsQuery(
            toKeywordField(criterion.getField(), isTimeseries), criterion.getValues())
        .queryName(fieldName);
  }

  /**
   * Builds an instance of {@link QueryBuilder} representing an EQUALS condition which was created
   * using the deprecated 'value' field of Criterion.pdl model.
   *
   * <p>Previously, we supported comma-separate values inside of a single string field, thus we have
   * to account for splitting and matching against each value below.
   *
   * <p>For all new code, we should be using the new 'values' field for performing multi-match. This
   * is simply retained for backwards compatibility of the search API.
   */
  private static QueryBuilder buildEqualsFromCriterionWithValue(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries) {
    // If the value is an URN style value, then we do not attempt to split it by comma (for obvious
    // reasons)
    if (isUrn(criterion.getValue())) {
      return QueryBuilders.matchQuery(
              toKeywordField(criterion.getField(), isTimeseries), criterion.getValue().trim())
          .queryName(fieldName)
          .analyzer(KEYWORD_ANALYZER);
    }
    final BoolQueryBuilder filters = new BoolQueryBuilder();
    // Cannot assume the existence of a .keyword or other subfield (unless contains `.`)
    // Cannot assume the type of the underlying field or subfield thus KEYWORD_ANALYZER is forced
    List<String> fields =
        criterion.getField().contains(".")
            ? List.of(criterion.getField())
            : List.of(criterion.getField(), criterion.getField() + ".*");
    Arrays.stream(criterion.getValue().trim().split("\\s*,\\s*"))
        .forEach(
            elem ->
                filters.should(
                    QueryBuilders.multiMatchQuery(elem, fields.toArray(new String[0]))
                        .queryName(fieldName)
                        .analyzer(KEYWORD_ANALYZER)));
    return filters;
  }
}

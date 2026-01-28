package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.*;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.query.filter.Condition.ANCESTORS_INCL;
import static com.linkedin.metadata.query.filter.Condition.DESCENDANTS_INCL;
import static com.linkedin.metadata.query.filter.Condition.RELATED_INCL;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.SUBFIELDS;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig.KEYWORD_FIELDS;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig.PATH_HIERARCHY_FIELDS;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.SliceOptions;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;
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
  public static final String OPAQUE_ID_HEADER = "X-Opaque-Id";
  public static final String HEADER_VALUE_DELIMITER = "|";
  public static final String REMOVED = "removed";
  public static final String ALIAS_FIELD_TYPE = "alias";
  public static final String TYPE = "type";
  public static final String KEYWORD = "keyword";
  public static final String FIELDS = "fields";
  public static final String SYSTEM_CREATED_FIELD = "systemCreated";
  public static final String COPY_TO = "copy_to";
  public static final String INDEX = "index";
  public static final String PATH = "path";
  public static final String PROPERTIES = "properties";

  // Field types
  public static final String KEYWORD_FIELD_TYPE = "keyword";
  public static final String BOOLEAN_FIELD_TYPE = "boolean";
  public static final String DATE_FIELD_TYPE = "date";
  public static final String DOUBLE_FIELD_TYPE = "double";
  public static final String FLOAT_FIELD_TYPE = "float";
  public static final String INTEGER_FIELD_TYPE = "integer";
  public static final String LONG_FIELD_TYPE = "long";
  public static final String SHORT_FIELD_TYPE = "short";
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

  public static final Set<Condition> RANGE_QUERY_CONDITIONS =
      Set.of(
          Condition.GREATER_THAN,
          Condition.GREATER_THAN_OR_EQUAL_TO,
          Condition.LESS_THAN,
          Condition.LESS_THAN_OR_EQUAL_TO);
  public static final String ENTITY_NAME_FIELD = "_entityName";
  public static final String NAME_SUGGESTION = "nameSuggestion";

  // we use this to make sure we filter for editable & non-editable fields. Also expands out
  // top-level properties
  // to field level properties
  public static final Map<String, List<String>> FIELDS_TO_EXPANDED_FIELDS_LIST =
      new HashMap<>() {
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
          put(
              "businessAttribute",
              ImmutableList.of("businessAttributeRef", "businessAttributeRef.urn"));
          put("origin", ImmutableList.of("origin", "env"));
          put("env", ImmutableList.of("env", "origin"));
        }
      };

  /*
   * Refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html for list of reserved
   * characters in an Elasticsearch regular expression.
   */
  private static final String ELASTICSEARCH_REGEXP_RESERVED_CHARACTERS = "?+*|{}[]()#@&<>~";

  private ESUtils() {}

  /**
   * Builds a map of field names to their types based on entity registry. This method extracts field
   * types from searchable annotations with fallback to ES mappings for all entities in the
   * registry.
   *
   * @param mappingsBuilder mappings builder instance to use for extracting field types
   * @param entityRegistry entity registry to extract field types from
   * @return map of field names to their searchable field types
   */
  public static Map<String, Set<SearchableAnnotation.FieldType>> buildSearchableFieldTypes(
      @Nonnull EntityRegistry entityRegistry, @Nonnull MappingsBuilder mappingsBuilder) {
    List<EntitySpec> entitySpecs =
        entityRegistry.getEntitySpecs().values().stream().collect(Collectors.toList());
    return buildSearchableFieldTypes(mappingsBuilder, entityRegistry, entitySpecs);
  }

  /**
   * Builds a map of field names to their types based on entity specs. This method extracts field
   * types from searchable annotations with fallback to ES mappings.
   *
   * @param mappingsBuilder mappings builder instance to use for extracting field types
   * @param entityRegistry entity registry for looking up mappings
   * @param entitySpecs list of entity specs to extract field types from
   * @return map of field names to their searchable field types
   */
  public static Map<String, Set<SearchableAnnotation.FieldType>> buildSearchableFieldTypes(
      @Nonnull MappingsBuilder mappingsBuilder,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull List<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .flatMap(
            (EntitySpec entitySpec) -> {
              Map<String, Set<SearchableAnnotation.FieldType>> annotationFieldTypes =
                  entitySpec.getSearchableFieldTypes();

              // fallback to mappings
              @SuppressWarnings("unchecked")
              Map<String, Map<String, Object>> rawMappingTypes =
                  ((Map<String, Object>)
                          mappingsBuilder
                              .getIndexMappings(entityRegistry, entitySpec)
                              .getOrDefault("properties", Map.<String, Object>of()))
                      .entrySet().stream()
                          .filter(
                              entry ->
                                  !annotationFieldTypes.containsKey(entry.getKey())
                                      && ((Map<String, Object>) entry.getValue()).containsKey(TYPE))
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey, e -> (Map<String, Object>) e.getValue()));

              Map<String, Set<SearchableAnnotation.FieldType>> mappingFieldTypes =
                  rawMappingTypes.entrySet().stream()
                      .map(
                          entry -> Map.entry(entry.getKey(), entry.getValue().get(TYPE).toString()))
                      .map(
                          entry ->
                              Map.entry(
                                  entry.getKey(),
                                  fallbackMappingToAnnotation(entry.getValue()).stream()
                                      .collect(Collectors.toSet())))
                      .filter(entry -> !entry.getValue().isEmpty())
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

              // aliases - pull from annotations
              Map<String, Set<SearchableAnnotation.FieldType>> aliasFieldTypes =
                  rawMappingTypes.entrySet().stream()
                      .filter(
                          entry -> ALIAS_FIELD_TYPE.equals(entry.getValue().get(TYPE).toString()))
                      .map(
                          entry ->
                              Map.entry(
                                  entry.getKey(),
                                  annotationFieldTypes.getOrDefault(
                                      entry.getValue().get(PATH).toString(),
                                      Collections.emptySet())))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

              List<SearchableFieldSpec> objectFieldSpec =
                  entitySpec.getSearchableFieldSpecs().stream()
                      .filter(
                          searchableFieldSpec ->
                              searchableFieldSpec.getSearchableAnnotation().getFieldType()
                                  == SearchableAnnotation.FieldType.OBJECT)
                      .collect(Collectors.toList());

              Map<String, Set<SearchableAnnotation.FieldType>> objectFieldTypes = new HashMap<>();

              objectFieldSpec.forEach(
                  fieldSpec -> {
                    String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
                    DataSchema.Type dataType =
                        ((MapDataSchema) fieldSpec.getPegasusSchema()).getValues().getType();

                    Set<SearchableAnnotation.FieldType> fieldType;

                    switch (dataType) {
                      case BOOLEAN:
                        fieldType = Set.of(SearchableAnnotation.FieldType.BOOLEAN);
                        break;
                      case INT:
                        fieldType = Set.of(SearchableAnnotation.FieldType.COUNT);
                        break;
                      case DOUBLE:
                      case LONG:
                      case FLOAT:
                        fieldType = Set.of(SearchableAnnotation.FieldType.DOUBLE);
                        break;
                      default:
                        fieldType = Set.of(SearchableAnnotation.FieldType.TEXT);
                        break;
                    }
                    objectFieldTypes.put(fieldName, fieldType);
                    annotationFieldTypes.remove(fieldName);
                  });

              return Stream.<Map.Entry<String, Set<SearchableAnnotation.FieldType>>>concat(
                  Stream.concat(
                      objectFieldTypes.entrySet().stream(),
                      annotationFieldTypes.entrySet().stream()),
                  Stream.concat(
                      mappingFieldTypes.entrySet().stream(), aliasFieldTypes.entrySet().stream()));
            })
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (set1, set2) -> {
                  Set<SearchableAnnotation.FieldType> merged = new HashSet<>(set1);
                  merged.addAll(set2);
                  return merged;
                }));
  }

  /**
   * Builds a map of PathSpec to field paths based on entity registry. This method aggregates field
   * paths from all entity specs using the existing EntitySpec.getSearchableFieldPathMap() method.
   *
   * @param entityRegistry entity registry to extract field paths from
   * @return map of PathSpec to their field paths
   */
  public static Map<PathSpec, String> buildSearchableFieldPaths(
      @Nonnull EntityRegistry entityRegistry) {
    Map<PathSpec, String> searchableFieldPaths = new HashMap<>();

    // Use the existing EntitySpec.getSearchableFieldPathMap() method for each entity
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs().values()) {
      searchableFieldPaths.putAll(entitySpec.getSearchableFieldPathMap());
    }

    return searchableFieldPaths;
  }

  private static Set<SearchableAnnotation.FieldType> fallbackMappingToAnnotation(
      @Nonnull String mappingType) {
    switch (mappingType) {
      case KEYWORD_FIELD_TYPE:
        return Set.of(SearchableAnnotation.FieldType.KEYWORD);
      case DATE_FIELD_TYPE:
        return Set.of(SearchableAnnotation.FieldType.DATETIME);
      case OBJECT_FIELD_TYPE:
        return Set.of(SearchableAnnotation.FieldType.OBJECT);
    }
    return Collections.emptySet();
  }

  /**
   * Builds a filter query as a Map suitable for low-level REST clients.
   *
   * <p>This method takes a Filter object and transforms it into an optimized OpenSearch query,
   * including rewrites and optimizations, while returning a Map for use with the Low-Level client.
   *
   * <p>Output shape is ensured to have a top-level {@code bool} object. If the optimized query is
   * not a bool, it will be wrapped as {"bool": {"filter": [ <query> ]}}.
   *
   * <p><b>Important:</b> Field types must be properly specified in searchableFieldTypes for numeric
   * fields to ensure correct value serialization. Without field type specification, numeric values
   * in range queries will remain as strings, leading to incorrect comparisons (e.g., "9" > "15").
   *
   * <p>Note: minimumShouldMatch is always serialized as a string by OpenSearch/Elasticsearch (e.g.,
   * "1" instead of 1) because it supports both absolute numbers and percentages.
   */
  @Nonnull
  public static Map<String, Object> buildFilterMap(
      @Nullable Filter filter,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    QueryBuilder qb =
        ESUtils.buildFilterQuery(
            filter, isTimeseries, searchableFieldTypes, opContext, queryFilterRewriteChain);

    // Optimize and preserve filtering semantics (considerScore=false for filters)
    qb = ESUtils.queryOptimize(qb, false);

    boolean wrapAsBool = !(qb instanceof BoolQueryBuilder);

    try {
      // If not a bool, wrap under a bool.filter using a QueryBuilder to avoid manual JSON mistakes
      QueryBuilder topQuery = wrapAsBool ? QueryBuilders.boolQuery().filter(qb) : qb;

      XContentBuilder builder = XContentFactory.jsonBuilder();
      topQuery.toXContent(builder, ToXContent.EMPTY_PARAMS);

      Map<String, Object> top =
          XContentHelper.convertToMap(JsonXContent.jsonXContent, builder.toString(), true);

      // If the serialized content already has top-level bool, return as-is, else wrap
      return top.containsKey("bool") ? top : Map.of("bool", top);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize filter query to Map", e);
    }
  }

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
  public static BoolQueryBuilder buildFilterQuery(
      @Nullable Filter filter,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    BoolQueryBuilder finalQueryBuilder = QueryBuilders.boolQuery();
    if (filter == null) {
      return finalQueryBuilder;
    }

    StructuredPropertyUtils.validateFilter(filter, opContext.getAspectRetriever());

    if (filter.getOr() != null) {
      // If caller is using the new Filters API, build boolean query from that.
      filter
          .getOr()
          .forEach(
              or ->
                  finalQueryBuilder.should(
                      ESUtils.buildConjunctiveFilterQuery(
                          or,
                          isTimeseries,
                          searchableFieldTypes,
                          opContext,
                          queryFilterRewriteChain)));
    } else if (filter.getCriteria() != null) {
      // Otherwise, build boolean query from the deprecated "criteria" field.
      log.warn("Received query Filter with a deprecated field 'criteria'. Use 'or' instead.");
      final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
      filter
          .getCriteria()
          .forEach(
              criterion -> {
                if (criterion.hasValues() || criterion.getCondition() == Condition.IS_NULL) {
                  andQueryBuilder.must(
                      getQueryBuilderFromCriterion(
                          criterion,
                          isTimeseries,
                          searchableFieldTypes,
                          opContext,
                          queryFilterRewriteChain));
                }
              });
      finalQueryBuilder.should(andQueryBuilder);
    }
    if (Boolean.TRUE.equals(
        opContext.getSearchContext().getSearchFlags().isFilterNonLatestVersions())) {
      BoolQueryBuilder filterNonLatestVersions =
          ESUtils.buildFilterNonLatestEntities(
              opContext, queryFilterRewriteChain, searchableFieldTypes);
      finalQueryBuilder.must(filterNonLatestVersions);
    }
    if (!finalQueryBuilder.should().isEmpty()) {
      finalQueryBuilder.minimumShouldMatch(1);
    }
    return finalQueryBuilder;
  }

  @Nonnull
  public static BoolQueryBuilder buildConjunctiveFilterQuery(
      @Nonnull ConjunctiveCriterion conjunctiveCriterion,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
    conjunctiveCriterion
        .getAnd()
        .forEach(
            criterion -> {
              if (Set.of(Condition.EXISTS, Condition.IS_NULL).contains(criterion.getCondition())
                  || criterion.hasValues()) {
                if (!criterion.isNegated()) {
                  // `filter` instead of `must` (enables caching and bypasses scoring)
                  andQueryBuilder.filter(
                      getQueryBuilderFromCriterion(
                          criterion,
                          isTimeseries,
                          searchableFieldTypes,
                          opContext,
                          queryFilterRewriteChain));
                } else {
                  andQueryBuilder.mustNot(
                      getQueryBuilderFromCriterion(
                          criterion,
                          isTimeseries,
                          searchableFieldTypes,
                          opContext,
                          queryFilterRewriteChain));
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
      @Nonnull final Criterion criterion,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    final String fieldName = toParentField(criterion.getField(), opContext.getAspectRetriever());

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
          maybeFieldToExpand.get(),
          criterion,
          isTimeseries,
          searchableFieldTypes,
          opContext,
          queryFilterRewriteChain);
    }

    return getQueryBuilderFromCriterionForSingleField(
        criterion,
        isTimeseries,
        searchableFieldTypes,
        criterion.getField(),
        opContext,
        queryFilterRewriteChain);
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
    } else if (OBJECT_FIELD_TYPES.contains(fieldType)) {
      return OBJECT_FIELD_TYPE;
    } else if (fieldType == SearchableAnnotation.FieldType.DOUBLE) {
      return DOUBLE_FIELD_TYPE;
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
   * @param sortCriteria list of {@link SortCriterion} to be applied to the search results
   */
  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      List<SortCriterion> sortCriteria,
      List<EntitySpec> entitySpecs) {
    buildSortOrder(
        searchSourceBuilder, sortCriteria == null ? List.of() : sortCriteria, entitySpecs, true);
  }

  /**
   * Allow disabling default sort, used when you know uniqueness is present without urn field. For
   * example, edge indices where the unique constraint is determined by multiple fields (src urn,
   * dst urn, relation type).
   *
   * @param enableDefaultSort enable/disable default sorting logic
   */
  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable List<SortCriterion> sortCriteria,
      List<EntitySpec> entitySpecs,
      boolean enableDefaultSort) {
    if (sortCriteria.isEmpty() && enableDefaultSort) {
      searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
    } else {
      sortCriteria = sortCriteria != null ? sortCriteria : Collections.emptyList();
      for (SortCriterion sortCriterion : sortCriteria) {
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
        if (fieldTypeForDefault.isEmpty() && !entitySpecs.isEmpty()) {
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

        // How missing value in documents should be treated in sorting.
        Object missingValue = sortCriterion.data().get("missingValue");
        if ("_last".equals(missingValue) || "_first".equals(missingValue)) {
          sortBuilder.missing(missingValue);
        }
        searchSourceBuilder.sort(sortBuilder);
      }
    }
    if (enableDefaultSort
        && (sortCriteria.isEmpty()
            || sortCriteria.stream()
                .noneMatch(c -> c.getField().equals(DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD)))) {
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

  /**
   * Resolve structured property field, or normal field, and strip subfields
   *
   * @param filterField name of the field used in the filter request
   * @param aspectRetriever aspect retriever, used if structured property
   * @return normalized field name without subfields
   */
  @Nonnull
  public static String toParentField(
      @Nonnull final String filterField, @Nullable final AspectRetriever aspectRetriever) {
    String fieldName =
        StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(filterField, aspectRetriever)
            .map(
                urnDefinition ->
                    STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                        + StructuredPropertyUtils.toElasticsearchFieldName(
                            urnDefinition.getFirst(), urnDefinition.getSecond()))
            .orElse(filterField);

    return replaceSuffix(fieldName);
  }

  /**
   * Strip subfields from filter field
   *
   * @param fieldName name of the field
   * @return normalized field name without subfields
   */
  @Nonnull
  public static String replaceSuffix(@Nonnull final String fieldName) {
    for (String subfield : SUBFIELDS) {
      String SUFFIX = "." + subfield;
      if (fieldName.endsWith(SUFFIX)) {
        return fieldName.replace(SUFFIX, "");
      }
    }

    return fieldName;
  }

  /**
   * Return resolved structured property field, normal field, or subfield which is of type `keyword`
   *
   * @param filterField the field name used in the filter
   * @param skipKeywordSuffix prevent use of `keyword` subfield, useful when parent field is known
   *     or always `keyword`
   * @param aspectRetriever aspect retriever, used if structured property field
   * @return the preferred field to use for `keyword` queries
   */
  @Nonnull
  public static String toKeywordField(
      @Nonnull final String filterField,
      final boolean skipKeywordSuffix,
      @Nullable final AspectRetriever aspectRetriever) {
    String fieldName =
        StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(filterField, aspectRetriever)
            .map(
                urnDefinition ->
                    STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                        + StructuredPropertyUtils.toElasticsearchFieldName(
                            urnDefinition.getFirst(), urnDefinition.getSecond()))
            .orElse(filterField);

    return skipKeywordSuffix
            || KEYWORD_FIELDS.contains(fieldName)
            || KEYWORD_FIELDS.stream()
                .anyMatch(nestedField -> fieldName.endsWith("." + nestedField))
            || PATH_HIERARCHY_FIELDS.contains(fieldName)
            || SUBFIELDS.stream().anyMatch(subfield -> fieldName.endsWith("." + subfield))
        ? fieldName
        : fieldName + ESUtils.KEYWORD_SUFFIX;
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

  public static void setSliceOptions(
      SearchSourceBuilder searchSourceBuilder, @Nullable SliceOptions sliceOptions) {
    if (sliceOptions != null) {
      searchSourceBuilder.slice(new SliceBuilder(sliceOptions.getId(), sliceOptions.getMax()));
    }
  }

  @Nonnull
  private static QueryBuilder getQueryBuilderFromCriterionForFieldToExpand(
      @Nonnull final List<String> fields,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    final BoolQueryBuilder orQueryBuilder = new BoolQueryBuilder().minimumShouldMatch(1);
    for (String field : fields) {
      orQueryBuilder.should(
          getQueryBuilderFromCriterionForSingleField(
                  buildCriterion(
                      toKeywordField(field, isTimeseries, opContext.getAspectRetriever()),
                      criterion.getCondition(),
                      criterion.isNegated(),
                      criterion.getValues()),
                  isTimeseries,
                  searchableFieldTypes,
                  null,
                  opContext,
                  queryFilterRewriteChain)
              .queryName(field));
    }
    return orQueryBuilder;
  }

  private static boolean isCaseInsensitiveSearchEnabled(Condition condition) {
    return condition == Condition.IEQUAL;
  }

  @Nonnull
  private static QueryBuilder getQueryBuilderFromCriterionForSingleField(
      @Nonnull Criterion criterion,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nullable String queryName,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    final Condition condition = criterion.getCondition();
    final AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    final String fieldName = toParentField(criterion.getField(), aspectRetriever);

    boolean enableCaseInsensitiveSearch;

    if (condition == Condition.IS_NULL) {
      return QueryBuilders.boolQuery()
          .mustNot(QueryBuilders.existsQuery(fieldName))
          .queryName(queryName != null ? queryName : fieldName);
    } else if (condition == Condition.EXISTS) {
      return QueryBuilders.boolQuery()
          .must(QueryBuilders.existsQuery(fieldName))
          .queryName(queryName != null ? queryName : fieldName);
    } else if (criterion.hasValues()) {
      if (condition == Condition.EQUAL || condition == Condition.IEQUAL) {
        enableCaseInsensitiveSearch = isCaseInsensitiveSearchEnabled(condition);
        return buildEqualsConditionFromCriterion(
                fieldName,
                criterion,
                isTimeseries,
                searchableFieldTypes,
                aspectRetriever,
                enableCaseInsensitiveSearch)
            .queryName(queryName != null ? queryName : fieldName);
      } else if (RANGE_QUERY_CONDITIONS.contains(condition)) {
        return buildRangeQueryFromCriterion(
                criterion,
                fieldName,
                searchableFieldTypes,
                condition,
                isTimeseries,
                aspectRetriever)
            .queryName(queryName != null ? queryName : fieldName);
      } else if (condition == Condition.CONTAIN) {
        return buildContainsConditionFromCriterion(
            fieldName, criterion, queryName, isTimeseries, aspectRetriever);
      } else if (condition == Condition.START_WITH) {
        return buildStartsWithConditionFromCriterion(
            fieldName, criterion, queryName, isTimeseries, aspectRetriever);
      } else if (condition == Condition.END_WITH) {
        return buildEndsWithConditionFromCriterion(
            fieldName, criterion, queryName, isTimeseries, aspectRetriever);
      } else if (Set.of(ANCESTORS_INCL, DESCENDANTS_INCL, RELATED_INCL).contains(condition)) {
        enableCaseInsensitiveSearch = isCaseInsensitiveSearchEnabled(condition);
        return QueryFilterRewriterContext.builder()
            .queryFilterRewriteChain(queryFilterRewriteChain)
            .condition(condition)
            .searchFlags(opContext.getSearchContext().getSearchFlags())
            .build(isTimeseries)
            .rewrite(
                opContext,
                buildEqualsConditionFromCriterion(
                    fieldName,
                    criterion,
                    isTimeseries,
                    searchableFieldTypes,
                    aspectRetriever,
                    enableCaseInsensitiveSearch))
            .queryName(queryName != null ? queryName : fieldName);
      }
    }
    throw new UnsupportedOperationException("Unsupported condition: " + condition);
  }

  private static QueryBuilder buildWildcardQueryWithMultipleValues(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries,
      @Nullable String queryName,
      @Nonnull AspectRetriever aspectRetriever,
      String wildcardPattern) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

    for (String value : criterion.getValues()) {
      boolQuery.should(
          QueryBuilders.wildcardQuery(
                  toKeywordField(criterion.getField(), isTimeseries, aspectRetriever),
                  String.format(wildcardPattern, ESUtils.escapeReservedCharacters(value.trim())))
              .queryName(queryName != null ? queryName : fieldName)
              .caseInsensitive(true));
    }
    return boolQuery;
  }

  private static QueryBuilder buildContainsConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable String queryName,
      final boolean isTimeseries,
      @Nonnull AspectRetriever aspectRetriever) {
    return buildWildcardQueryWithMultipleValues(
        fieldName, criterion, isTimeseries, queryName, aspectRetriever, "*%s*");
  }

  private static QueryBuilder buildStartsWithConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable String queryName,
      final boolean isTimeseries,
      @Nonnull AspectRetriever aspectRetriever) {
    return buildWildcardQueryWithMultipleValues(
        fieldName, criterion, isTimeseries, queryName, aspectRetriever, "%s*");
  }

  private static QueryBuilder buildEndsWithConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable String queryName,
      final boolean isTimeseries,
      @Nonnull AspectRetriever aspectRetriever) {
    return buildWildcardQueryWithMultipleValues(
        fieldName, criterion, isTimeseries, queryName, aspectRetriever, "*%s");
  }

  private static QueryBuilder buildEqualsConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull AspectRetriever aspectRetriever,
      boolean enableCaseInsensitiveSearch) {
    return buildEqualsConditionFromCriterionWithValues(
        fieldName,
        criterion,
        isTimeseries,
        searchableFieldTypes,
        aspectRetriever,
        enableCaseInsensitiveSearch);
  }

  /**
   * Builds an instance of {@link QueryBuilder} representing an EQUALS condition which was created
   * using the new multi-match 'values' field of Criterion.pdl model.
   */
  private static QueryBuilder buildEqualsConditionFromCriterionWithValues(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull AspectRetriever aspectRetriever,
      boolean enableCaseInsensitiveSearch) {
    Set<String> fieldTypes =
        getFieldTypes(searchableFieldTypes, fieldName, criterion, aspectRetriever);
    if (fieldTypes.size() > 1) {
      log.warn(
          "Multiple field types for field name {}, determining best fit for set: {}",
          fieldName,
          fieldTypes);
    }
    if (fieldTypes.contains(BOOLEAN_FIELD_TYPE) && criterion.getValues().size() == 1) {
      return QueryBuilders.termQuery(fieldName, Boolean.parseBoolean(criterion.getValues().get(0)))
          .queryName(fieldName);
    } else if (fieldTypes.contains(LONG_FIELD_TYPE) || fieldTypes.contains(DATE_FIELD_TYPE)) {
      List<Long> longValues =
          criterion.getValues().stream().map(Long::parseLong).collect(Collectors.toList());
      return QueryBuilders.termsQuery(fieldName, longValues).queryName(fieldName);
    } else if (fieldTypes.contains(DOUBLE_FIELD_TYPE)) {
      List<Double> doubleValues =
          criterion.getValues().stream().map(Double::parseDouble).collect(Collectors.toList());
      return QueryBuilders.termsQuery(fieldName, doubleValues).queryName(fieldName);
    }

    if (enableCaseInsensitiveSearch) {
      BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
      criterion
          .getValues()
          .forEach(
              value ->
                  boolQuery.should(
                      QueryBuilders.termQuery(
                              toKeywordField(criterion.getField(), isTimeseries, aspectRetriever),
                              value.trim())
                          .caseInsensitive(true)));
      if (!boolQuery.should().isEmpty()) {
        boolQuery.minimumShouldMatch(1);
      }
      return boolQuery;
    }

    return QueryBuilders.termsQuery(
            toKeywordField(criterion.getField(), isTimeseries, aspectRetriever),
            criterion.getValues())
        .queryName(fieldName);
  }

  private static Set<String> getFieldTypes(
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFields,
      String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable AspectRetriever aspectRetriever) {
    final Set<String> finalFieldTypes;
    if (fieldName.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
      // use criterion field here for structured props since fieldName has dots replaced with
      // underscores
      finalFieldTypes =
          StructuredPropertyUtils.toElasticsearchFieldType(
              replaceSuffix(criterion.getField()), aspectRetriever);
    } else {
      Set<SearchableAnnotation.FieldType> fieldTypes =
          searchableFields.getOrDefault(fieldName.split("\\.")[0], Collections.emptySet());
      finalFieldTypes =
          fieldTypes.stream().map(ESUtils::getElasticTypeForFieldType).collect(Collectors.toSet());
    }

    if (finalFieldTypes.size() > 1) {
      String fieldType =
          fieldName.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)
              ? "structured property"
              : "regular field";
      log.warn(
          "Multiple field types for {} '{}' (criterion: {}, values: {}), determining best fit for set: {}",
          fieldType,
          fieldName,
          criterion.getField(),
          criterion.getValues(),
          finalFieldTypes);
    }
    return finalFieldTypes;
  }

  private static RangeQueryBuilder buildRangeQueryFromCriterion(
      Criterion criterion,
      String fieldName,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Condition condition,
      boolean isTimeseries,
      AspectRetriever aspectRetriever) {
    Set<String> fieldTypes =
        getFieldTypes(searchableFieldTypes, fieldName, criterion, aspectRetriever);

    // Determine criterion value, range query only accepts single value so take first value in
    // values if multiple
    String criterionValueString = criterion.getValues().get(0).trim();

    Object criterionValue;
    String documentFieldName;
    if (fieldTypes.contains(BOOLEAN_FIELD_TYPE)) {
      criterionValue = Boolean.parseBoolean(criterionValueString);
      documentFieldName = fieldName;
    } else if (fieldTypes.contains(LONG_FIELD_TYPE) || fieldTypes.contains(DATE_FIELD_TYPE)) {
      criterionValue = Long.parseLong(criterionValueString);
      documentFieldName = fieldName;
    } else if (fieldTypes.contains(DOUBLE_FIELD_TYPE)) {
      criterionValue = Double.parseDouble(criterionValueString);
      documentFieldName = fieldName;
    } else {
      criterionValue = criterionValueString;
      documentFieldName = toKeywordField(fieldName, isTimeseries, aspectRetriever);
    }

    // Set up QueryBuilder based on condition
    if (condition == Condition.GREATER_THAN) {
      return QueryBuilders.rangeQuery(documentFieldName).gt(criterionValue).queryName(fieldName);
    } else if (condition == Condition.GREATER_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(documentFieldName).gte(criterionValue).queryName(fieldName);
    } else if (condition == Condition.LESS_THAN) {
      return QueryBuilders.rangeQuery(documentFieldName).lt(criterionValue).queryName(fieldName);
    } else /*if (condition == Condition.LESS_THAN_OR_EQUAL_TO)*/ {
      return QueryBuilders.rangeQuery(documentFieldName).lte(criterionValue).queryName(fieldName);
    }
  }

  @Nonnull
  public static BoolQueryBuilder applyDefaultSearchFilters(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery) {
    // filter soft deleted entities by default
    filterSoftDeletedByDefault(filter, filterQuery, opContext.getSearchContext().getSearchFlags());
    return filterQuery;
  }

  /**
   * Applies a default filter to remove entities that are soft deleted only if there isn't a filter
   * for the REMOVED field already and soft delete entities are not being requested via search flags
   */
  private static void filterSoftDeletedByDefault(
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery,
      @Nonnull SearchFlags searchFlags) {
    if (Boolean.FALSE.equals(searchFlags.isIncludeSoftDeleted())) {
      boolean removedInOrFilter = false;
      if (filter != null) {
        removedInOrFilter =
            filter.getOr().stream()
                .anyMatch(
                    or ->
                        or.getAnd().stream()
                            .anyMatch(
                                criterion ->
                                    criterion.getField().equals(REMOVED)
                                        || criterion.getField().equals(REMOVED + KEYWORD_SUFFIX)));
      }
      if (!removedInOrFilter) {
        filterQuery.mustNot(QueryBuilders.termQuery(REMOVED, true));
      }
    }
  }

  public static BoolQueryBuilder buildFilterNonLatestEntities(
      OperationContext opContext,
      QueryFilterRewriteChain queryFilterRewriteChain,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes) {
    ConjunctiveCriterion isLatestCriterion = new ConjunctiveCriterion();
    CriterionArray isLatestCriterionArray = new CriterionArray();
    isLatestCriterionArray.add(
        CriterionUtils.buildCriterion(IS_LATEST_FIELD_NAME, Condition.EQUAL, "true"));
    isLatestCriterion.setAnd(isLatestCriterionArray);
    BoolQueryBuilder isLatest =
        ESUtils.buildConjunctiveFilterQuery(
            isLatestCriterion, false, searchableFieldTypes, opContext, queryFilterRewriteChain);
    ConjunctiveCriterion isNotVersionedCriterion = new ConjunctiveCriterion();
    CriterionArray isNotVersionedCriterionArray = new CriterionArray();
    isNotVersionedCriterionArray.add(
        CriterionUtils.buildCriterion(IS_LATEST_FIELD_NAME, Condition.EXISTS, true));
    isNotVersionedCriterion.setAnd(isNotVersionedCriterionArray);
    BoolQueryBuilder isNotVersioned =
        ESUtils.buildConjunctiveFilterQuery(
            isNotVersionedCriterion,
            false,
            searchableFieldTypes,
            opContext,
            queryFilterRewriteChain);
    return QueryBuilders.boolQuery().should(isLatest).should(isNotVersioned).minimumShouldMatch(1);
  }

  public static Optional<String> getSystemModifiedAtFieldName(
      @Nonnull SearchableFieldSpec searchableFieldSpec) {
    final String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
    return searchableFieldSpec.getSearchableAnnotation().isIncludeSystemModifiedAt()
        ? searchableFieldSpec
            .getSearchableAnnotation()
            .getSystemModifiedAtFieldName()
            .or(() -> Optional.of(String.format("%sSystemModifiedAt", fieldName)))
        : Optional.empty();
  }

  /**
   * Performs a single optimization pass on the query tree.
   *
   * @param query The QueryBuilder to optimize
   * @param considerScore If true, moves to must() for scoring; if false, moves to filter()
   * @return true if any optimization was performed, false otherwise
   */
  static boolean optimizePass(QueryBuilder query, boolean considerScore) {
    if (!(query instanceof BoolQueryBuilder)) {
      return false;
    }

    BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
    boolean changed = false;
    boolean localChanged;

    // Keep optimizing this level until no more local changes
    // This ensures that when we move clauses around, they get properly optimized
    do {
      localChanged = false;

      // First, recursively optimize all nested queries
      // Must clauses
      for (int i = 0; i < boolQuery.must().size(); i++) {
        localChanged |= optimizePass(boolQuery.must().get(i), considerScore);
      }

      // Filter clauses - always use considerScore=false for filters
      for (int i = 0; i < boolQuery.filter().size(); i++) {
        localChanged |= optimizePass(boolQuery.filter().get(i), false);
      }

      // Should clauses
      for (int i = 0; i < boolQuery.should().size(); i++) {
        localChanged |= optimizePass(boolQuery.should().get(i), considerScore);
      }

      // MustNot clauses - always use considerScore=false
      for (int i = 0; i < boolQuery.mustNot().size(); i++) {
        localChanged |= optimizePass(boolQuery.mustNot().get(i), false);
      }

      // After optimizing children, check if this query itself can be optimized

      // Optimization 1: Convert single should clause with minimumShouldMatch=1
      if (isOptimizableShould(boolQuery)) {
        // Get the should clause
        QueryBuilder shouldClause = boolQuery.should().get(0);

        // Remove the should clause
        boolQuery.should().clear();

        // Reset minimumShouldMatch since there are no more should clauses
        boolQuery.minimumShouldMatch(null);

        // Add to appropriate clause type
        if (considerScore) {
          boolQuery.must(shouldClause);
        } else {
          boolQuery.filter(shouldClause);
        }

        localChanged = true;
      }

      // Optimization 2: Flatten nested bool queries with only filter clauses
      if (canFlattenFilters(boolQuery)) {
        localChanged |= flattenFilters(boolQuery);
      }

      changed |= localChanged;
    } while (localChanged);

    // Note: We don't handle unwrapping here because it requires replacing the query object
    // which can't be done in-place. Unwrapping is handled at the top level in queryOptimize.

    return changed;
  }

  /**
   * Checks if the BoolQueryBuilder has a single should clause with minimumShouldMatch=1
   *
   * @param query The BoolQueryBuilder to check
   * @return true if the query can be optimized, false otherwise
   */
  static boolean isOptimizableShould(BoolQueryBuilder query) {
    // Check if there's exactly one should clause
    if (query.should().size() != 1) {
      return false;
    }

    // Check if minimumShouldMatch is set to "1"
    String minShouldMatch = query.minimumShouldMatch();
    if (minShouldMatch == null) {
      return false;
    }

    // Handle different formats of minimumShouldMatch
    // It could be "1", "1%", or other formats
    if (minShouldMatch.equals("1") || minShouldMatch.equals("100%")) {
      return true;
    }

    // Check if it's a percentage format like "100%"
    if (minShouldMatch.endsWith("%")) {
      try {
        int percentage = Integer.parseInt(minShouldMatch.substring(0, minShouldMatch.length() - 1));
        // With only 1 should clause, 100% means that 1 clause must match
        return percentage == 100;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    return false;
  }

  /**
   * Checks if a bool query can have its filter clauses flattened. This is possible when it has
   * filter clauses containing other bool queries that only have filter clauses.
   *
   * @param query The BoolQueryBuilder to check
   * @return true if filters can be flattened
   */
  static boolean canFlattenFilters(BoolQueryBuilder query) {
    // Check each filter clause
    for (QueryBuilder filter : query.filter()) {
      if (filter instanceof BoolQueryBuilder) {
        BoolQueryBuilder nestedBool = (BoolQueryBuilder) filter;
        // If the nested bool query has only filter clauses (no must, should, mustNot),
        // then we can flatten it
        if (nestedBool.must().isEmpty()
            && nestedBool.should().isEmpty()
            && nestedBool.mustNot().isEmpty()
            && !nestedBool.filter().isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Flattens nested filter clauses in a bool query. Extracts filter clauses from nested bool
   * queries that only contain filters and adds them directly to the parent.
   *
   * @param query The BoolQueryBuilder to flatten
   * @return true if any flattening occurred
   */
  private static boolean flattenFilters(BoolQueryBuilder query) {
    boolean changed = false;
    List<QueryBuilder> filtersToAdd = new ArrayList<>();
    List<QueryBuilder> filtersToRemove = new ArrayList<>();

    // Identify filters that can be flattened
    for (QueryBuilder filter : query.filter()) {
      if (filter instanceof BoolQueryBuilder) {
        BoolQueryBuilder nestedBool = (BoolQueryBuilder) filter;
        // If the nested bool query has only filter clauses, extract them
        if (nestedBool.must().isEmpty()
            && nestedBool.should().isEmpty()
            && nestedBool.mustNot().isEmpty()
            && !nestedBool.filter().isEmpty()) {
          // Mark for removal
          filtersToRemove.add(filter);

          // Extract all filters from the nested bool query
          filtersToAdd.addAll(nestedBool.filter());

          changed = true;
        }
      }
    }

    // Apply the changes
    if (changed) {
      // Remove the nested bool queries
      for (QueryBuilder toRemove : filtersToRemove) {
        query.filter().remove(toRemove);
      }

      // Add the extracted filters directly
      for (QueryBuilder toAdd : filtersToAdd) {
        query.filter(toAdd);
      }
    }

    return changed;
  }

  /**
   * Checks if a bool query can be unwrapped (replaced by its single clause). This is possible when
   * the bool query has exactly one clause total.
   *
   * @param query The BoolQueryBuilder to check
   * @return true if the query can be unwrapped
   */
  static boolean canUnwrap(BoolQueryBuilder query) {
    int totalClauses =
        query.must().size()
            + query.filter().size()
            + query.should().size()
            + query.mustNot().size();

    // Can unwrap if there's exactly one clause and no minimumShouldMatch constraint
    return totalClauses == 1 && query.minimumShouldMatch() == null;
  }

  /**
   * Fully optimizes a query by running optimization passes until no more changes occur.
   *
   * @param query The QueryBuilder to optimize
   * @param considerScore If true, preserves scoring; if false, optimizes for filtering
   * @return The optimized query (may be a different instance if unwrapping occurred)
   */
  public static QueryBuilder queryOptimize(QueryBuilder query, boolean considerScore) {
    if (query == null) {
      return null;
    }

    // For non-bool queries, return as-is
    if (!(query instanceof BoolQueryBuilder)) {
      return query;
    }

    BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;

    // Keep optimizing until no more changes
    boolean changed;
    int iterations = 0;
    int maxIterations = 100; // Safety limit to prevent infinite loops

    do {
      changed = optimizePass(boolQuery, considerScore);
      iterations++;

      if (iterations >= maxIterations) {
        // Log warning or throw exception in production code
        break;
      }
    } while (changed);

    // After all optimization passes, check if we can unwrap at the top level
    if (canUnwrap(boolQuery)) {
      // Return the single clause directly
      if (!boolQuery.must().isEmpty()) {
        return queryOptimize(boolQuery.must().get(0), considerScore);
      } else if (!boolQuery.filter().isEmpty()) {
        return queryOptimize(boolQuery.filter().get(0), false);
      } else if (!boolQuery.should().isEmpty()) {
        return queryOptimize(boolQuery.should().get(0), considerScore);
      } else if (!boolQuery.mustNot().isEmpty()) {
        // mustNot can't stand alone, keep it wrapped
        return boolQuery;
      }
    }

    return boolQuery;
  }

  public static @Nonnull String computePointInTime(
      String scrollId, String keepAlive, SearchClientShim<?> client, String... indexArray) {
    if (scrollId != null) {
      SearchAfterWrapper searchAfterWrapper = SearchAfterWrapper.fromScrollId(scrollId);
      if (System.currentTimeMillis() + 10000 <= searchAfterWrapper.getExpirationTime()) {
        return searchAfterWrapper.getPitId();
      }
    }
    switch (client.getEngineType()) {
      case ELASTICSEARCH_7:
        return createPointInTimeElasticSearch(client, indexArray, keepAlive);
      case ELASTICSEARCH_8:
      case OPENSEARCH_2:
      case ELASTICSEARCH_9:
        return createPointInTimeOpenSearch(client, indexArray, keepAlive);
      default:
        log.warn("Unsupported elasticsearch implementation: {}", client.getEngineType());
        throw new IllegalStateException("Unsupported elasticsearch implementation.");
    }
  }

  private static @Nonnull String createPointInTimeElasticSearch(
      SearchClientShim<?> client, String[] indexArray, String keepAlive) {
    String endPoint = String.join(",", indexArray) + "/_pit";
    Request request = new Request("POST", endPoint);
    request.addParameter("keep_alive", keepAlive);
    try {
      RawResponse response = client.performLowLevelRequest(request);
      Map<String, Object> mappedResponse =
          OBJECT_MAPPER.readValue(response.getEntity().getContent(), new TypeReference<>() {});
      return (String) mappedResponse.get("id");
    } catch (IOException e) {
      log.warn("Failed to generate PointInTime Identifier:", e);
      throw new IllegalStateException("Failed to generate PointInTime Identifier.", e);
    }
  }

  private static @Nonnull String createPointInTimeOpenSearch(
      SearchClientShim<?> client, String[] indexArray, String keepAlive) {
    try {
      CreatePitRequest request =
          new CreatePitRequest(TimeValue.parseTimeValue(keepAlive, "keepAlive"), false, indexArray);
      CreatePitResponse response = client.createPit(request, RequestOptions.DEFAULT);
      return response.getId();
    } catch (OpenSearchStatusException ose) {
      if (TOO_MANY_REQUESTS.equals(ose.status())) {
        APIThrottleException throttleException =
            new APIThrottleException(
                TimeValue.parseTimeValue(keepAlive, "keepAlive").millis(),
                "Too many point in times created, retry after keep alive has expired.");
        try {
          throttleException.initCause(ose);
        } catch (IllegalStateException | IllegalArgumentException e) {
          // Do nothing, can't fill in cause
        }
        throw throttleException;
      }
      throw ose;
    } catch (IOException e) {
      log.warn("Failed to generate PointInTime Identifier:", e);
      throw new IllegalStateException("Failed to generate PointInTime Identifier.", e);
    }
  }

  /**
   * Clean up a Point-in-Time (PIT) to prevent hitting the PIT context limit. This method should be
   * called in finally blocks after PIT usage.
   *
   * @param client The OpenSearch client
   * @param pitId The PIT ID to clean up
   * @param context Optional context for logging (e.g., "slice 0", "search request")
   */
  public static void cleanupPointInTime(SearchClientShim<?> client, String pitId, String context) {
    if (pitId == null) {
      return;
    }

    try {
      switch (client.getEngineType()) {
        case OPENSEARCH_2:
        case ELASTICSEARCH_8:
        case ELASTICSEARCH_9:
          {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            DeletePitResponse deletePitResponse =
                client.deletePit(deletePitRequest, RequestOptions.DEFAULT);
            // DeletePitResponse doesn't have isAcknowledged(), but if we get here without
            // exception, it
            // succeeded
            log.debug("Successfully cleaned up PIT {} for {}", pitId, context);
            break;
          }
        case ELASTICSEARCH_7:
          {
            // For Elasticsearch, use the low-level client to delete PIT
            String endPoint = "/_pit";
            Request request = new Request("DELETE", endPoint);
            request.setJsonEntity("{\"id\":\"" + pitId + "\"}");
            RawResponse response = client.performLowLevelRequest(request);
            if (response.getStatusLine().getStatusCode() == 200) {
              log.debug("Successfully cleaned up PIT {} for {}", pitId, context);
            } else {
              log.warn(
                  "Failed to clean up PIT {} for {}: HTTP {}",
                  pitId,
                  context,
                  response.getStatusLine().getStatusCode());
            }
          }
      }
    } catch (Exception e) {
      log.warn("Error cleaning up PIT {} for {}: {}", pitId, context, e.getMessage());
    }
  }
}

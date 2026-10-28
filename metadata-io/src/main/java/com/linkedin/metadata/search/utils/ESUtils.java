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

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.EntityFilter;
import com.linkedin.metadata.service.LifecycleStageTypeService;
import com.linkedin.metadata.throttle.ThrottleMechanismType;
import com.linkedin.metadata.throttle.ThrottleResponseSource;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
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

  /**
   * Holds the LifecycleStageTypeService singleton for use in default search filters. Set once at
   * startup via setLifecycleStageTypeService(). Volatile for visibility across threads.
   */
  @Nullable private static volatile LifecycleStageTypeService lifecycleStageTypeService;

  /** Wires the service into the search filter path. Called once from the Spring factory. */
  public static void setLifecycleStageTypeService(@Nullable LifecycleStageTypeService service) {
    ESUtils.lifecycleStageTypeService = service;
  }

  /** Returns the currently wired service, or null if not yet set. */
  @Nullable
  public static LifecycleStageTypeService getLifecycleStageTypeService() {
    return lifecycleStageTypeService;
  }

  private static final String DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD = "urn";
  public static final String KEYWORD_ANALYZER = "keyword";
  public static final String KEYWORD_SUFFIX = ".keyword";
  public static final String OPAQUE_ID_HEADER = "X-Opaque-Id";
  public static final String HEADER_VALUE_DELIMITER = "|";
  public static final String REMOVED = "removed";
  public static final String LIFECYCLE_STAGE = "lifecycleStage";
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

  /**
   * {@code ignore_above} for TEXT-derived keyword fields. Lucene rejects indexed terms longer than
   * 32766 bytes; matching that limit skips oversized values instead of failing the document. See
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html and
   * https://docs.opensearch.org/latest/field-types/supported-field-types/string/ Tokenized text
   * sub-fields are unaffected.
   *
   * <p>Structured-property write validation is configured separately via {@code
   * structuredProperties.keywordMaxLength} / {@code STRUCTURED_PROPERTIES_KEYWORD_MAX_LENGTH}.
   */
  public static final int KEYWORD_MAXLENGTH = 32766;

  /**
   * True when {@code value}'s UTF-8 encoding exceeds the configured keyword max length (Lucene term
   * limit). Empty or null values never exceed.
   */
  public static boolean exceedsKeywordMaxBytes(@Nullable String value, int keywordMaxBytes) {
    if (value == null || value.isEmpty()) {
      return false;
    }
    int maxBytes = keywordMaxBytes > 0 ? keywordMaxBytes : KEYWORD_MAXLENGTH;
    return value.getBytes(StandardCharsets.UTF_8).length > maxBytes;
  }

  /** Mapping parameter name for the keyword length guard described above. */
  public static final String IGNORE_ABOVE = "ignore_above";

  /**
   * Byte-safe {@code ignore_above} (characters) for a configured keyword max length in bytes.
   *
   * <p>{@code ignore_above} is measured in <b>characters</b> by Elasticsearch/OpenSearch, but
   * Lucene's per-term limit is in <b>bytes</b>. A UTF-8 character is at most 4 bytes, so dividing
   * by 4 yields the largest character threshold that can never exceed the byte limit.
   */
  public static int keywordIgnoreAboveForMaxBytes(int keywordMaxBytes) {
    int maxBytes = keywordMaxBytes > 0 ? keywordMaxBytes : KEYWORD_MAXLENGTH;
    return maxBytes / 4;
  }

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
    return buildFilterMap(
        filter, isTimeseries, searchableFieldTypes, opContext, queryFilterRewriteChain, null);
  }

  @Nonnull
  public static Map<String, Object> buildFilterMap(
      @Nullable Filter filter,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nullable SearchFieldResolver v3FieldResolver) {
    QueryBuilder qb =
        ESUtils.buildFilterQuery(
            filter,
            isTimeseries,
            searchableFieldTypes,
            opContext,
            queryFilterRewriteChain,
            v3FieldResolver);

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
    return buildFilterQuery(
        filter, isTimeseries, searchableFieldTypes, opContext, queryFilterRewriteChain, null);
  }

  @Nonnull
  public static BoolQueryBuilder buildFilterQuery(
      @Nullable Filter filter,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nullable SearchFieldResolver v3FieldResolver) {
    BoolQueryBuilder finalQueryBuilder = QueryBuilders.boolQuery();
    // No early return on a null filter: unfiltered searches still need the latest-version clause.
    if (filter != null) {
      StructuredPropertyUtils.validateFilter(opContext, filter, opContext.getAspectRetriever());

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
                            queryFilterRewriteChain,
                            v3FieldResolver)));
      } else if (filter.getCriteria() != null) {
        // Otherwise, build boolean query from the deprecated "criteria" field.
        log.warn("Received query Filter with a deprecated field 'criteria'. Use 'or' instead.");
        final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
        boolean omitted = false;
        for (Criterion criterion : filter.getCriteria()) {
          if (criterion.hasValues() || criterion.getCondition() == Condition.IS_NULL) {
            QueryBuilder query =
                getQueryBuilderFromCriterion(
                    criterion,
                    isTimeseries,
                    searchableFieldTypes,
                    opContext,
                    queryFilterRewriteChain,
                    v3FieldResolver);
            if (query == null) {
              omitted = true;
            } else {
              andQueryBuilder.must(query);
            }
          }
        }
        finalQueryBuilder.should(omittedGroup(andQueryBuilder, omitted));
      }
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
    return buildConjunctiveFilterQuery(
        conjunctiveCriterion,
        isTimeseries,
        searchableFieldTypes,
        opContext,
        queryFilterRewriteChain,
        null);
  }

  @Nonnull
  public static BoolQueryBuilder buildConjunctiveFilterQuery(
      @Nonnull ConjunctiveCriterion conjunctiveCriterion,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nullable SearchFieldResolver v3FieldResolver) {
    final BoolQueryBuilder andQueryBuilder = new BoolQueryBuilder();
    boolean omitted = false;
    for (Criterion criterion : conjunctiveCriterion.getAnd()) {
      if (Set.of(Condition.EXISTS, Condition.IS_NULL).contains(criterion.getCondition())
          || criterion.hasValues()) {
        QueryBuilder query =
            getQueryBuilderFromCriterion(
                criterion,
                isTimeseries,
                searchableFieldTypes,
                opContext,
                queryFilterRewriteChain,
                v3FieldResolver);
        if (query == null) {
          omitted = true;
          continue;
        }
        if (!criterion.isNegated()) {
          // `filter` instead of `must` (enables caching and bypasses scoring)
          andQueryBuilder.filter(query);
        } else {
          andQueryBuilder.mustNot(query);
        }
      }
    }
    return omittedGroup(andQueryBuilder, omitted);
  }

  /**
   * An empty bool matches every document. A group that omitted every clause must not, or a sole
   * unindexed filter and an OR branch of one would match the catalog.
   */
  @Nonnull
  private static BoolQueryBuilder omittedGroup(@Nonnull BoolQueryBuilder group, boolean omitted) {
    if (omitted && !hasClauses(group)) {
      return QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
    }
    return group;
  }

  private static boolean hasClauses(@Nonnull BoolQueryBuilder query) {
    return !query.filter().isEmpty()
        || !query.must().isEmpty()
        || !query.should().isEmpty()
        || !query.mustNot().isEmpty();
  }

  /**
   * Builds search query given a {@link Criterion}, containing field, values and
   * association/condition between them.
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
   * @param criterion {@link Criterion} single criterion which contains field, values and a
   *     comparison operator
   */
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterion(
      @Nonnull final Criterion criterion,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    return getQueryBuilderFromCriterion(
        criterion, isTimeseries, searchableFieldTypes, opContext, queryFilterRewriteChain, null);
  }

  /**
   * Returns null when a registered Search V3 field cannot answer the condition. An unindexed label
   * such as {@code description} answers {@code EXISTS} and {@code IS_NULL} through its hasValues
   * field and is left out for every other condition except {@code CONTAIN}.
   */
  @Nullable
  public static QueryBuilder getQueryBuilderFromCriterion(
      @Nonnull final Criterion criterion,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nullable SearchFieldResolver v3FieldResolver) {
    if (v3FieldResolver != null
        && !criterion.getField().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
      Optional<String> unlabeledSource =
          v3FieldResolver.unlabeledHasValuesSource(criterion.getField());
      if (unlabeledSource.isPresent()) {
        return unlabeledHasValuesQuery(v3FieldResolver, criterion, unlabeledSource.get());
      }
    }
    final boolean resolvedForV3 =
        v3FieldResolver != null
            && !criterion.getField().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)
            && v3FieldResolver.contains(criterion.getField());
    final Criterion effectiveCriterion;
    if (resolvedForV3 && criterion.getCondition() == Condition.CONTAIN) {
      SearchFieldResolver.Field containField =
          v3FieldResolver.resolve(criterion.getField(), Intent.CONTAIN);
      if (!containField.isIndexed()) {
        return buildAnalyzedContainsQuery(
            criterion.getField(), v3FieldResolver.analyzedPaths(criterion.getField()), criterion);
      }
      effectiveCriterion =
          buildCriterion(
              lowercaseKeywordPath(
                  v3FieldResolver,
                  criterion.getField(),
                  v3FieldResolver.exactPath(criterion.getField(), Intent.CONTAIN)),
              criterion.getCondition(),
              criterion.isNegated(),
              criterion.hasValues() ? criterion.getValues() : null);
    } else if (resolvedForV3) {
      Intent intent =
          RANGE_QUERY_CONDITIONS.contains(criterion.getCondition()) ? Intent.RANGE : Intent.FILTER;
      if (v3FieldResolver.supports(criterion.getField(), intent)) {
        String path = v3FieldResolver.exactPath(criterion.getField(), intent);
        if (criterion.getCondition() == Condition.IEQUAL) {
          final String lowercase = v3FieldResolver.lowercaseFullUrnPath(criterion.getField());
          if (!lowercase.isEmpty()) {
            path = lowercase;
          } else {
            path = lowercaseKeywordPath(v3FieldResolver, criterion.getField(), path);
          }
        }
        if (criterion.getCondition() == Condition.START_WITH
            || criterion.getCondition() == Condition.END_WITH) {
          path = lowercaseKeywordPath(v3FieldResolver, criterion.getField(), path);
        }
        effectiveCriterion =
            buildCriterion(
                path,
                criterion.getCondition(),
                criterion.isNegated(),
                criterion.hasValues() ? criterion.getValues() : null);
      } else {
        List<String> hasValuesFields = v3FieldResolver.hasValuesFieldNames(criterion.getField());
        if ((criterion.getCondition() == Condition.EXISTS
                || criterion.getCondition() == Condition.IS_NULL)
            && hasValuesFields.size() > 1) {
          return buildHasValuesExistenceQuery(
              criterion.getField(), hasValuesFields, criterion.getCondition() == Condition.IS_NULL);
        }
        Criterion rewritten = unindexedV3Criterion(v3FieldResolver, criterion);
        if (rewritten == null) {
          return null;
        }
        effectiveCriterion = rewritten;
      }
    } else {
      effectiveCriterion = criterion;
    }
    final String fieldName =
        resolvedForV3
            ? effectiveCriterion.getField()
            : toParentField(opContext, criterion.getField(), opContext.getAspectRetriever());

    /*
     * Check the field-name for a "sibling" field, or one which should ALWAYS
     * be matched in disjunction with the targeted field (OR).
     *
     * This essentially equates to filter expansion based on a particular field.
     * First we handle this expansion, if required, otherwise we build the filter as usual
     * without expansion.
     */
    final Optional<List<String>> maybeFieldToExpand =
        resolvedForV3
            ? Optional.empty()
            : Optional.ofNullable(FIELDS_TO_EXPANDED_FIELDS_LIST.get(fieldName));

    if (maybeFieldToExpand.isPresent()) {
      return getQueryBuilderFromCriterionForFieldToExpand(
          maybeFieldToExpand.get(),
          effectiveCriterion,
          isTimeseries,
          searchableFieldTypes,
          opContext,
          queryFilterRewriteChain,
          v3FieldResolver != null);
    }

    if (resolvedForV3 && isSharedHasValuesFalse(v3FieldResolver, criterion)) {
      return QueryBuilders.boolQuery()
          .mustNot(QueryBuilders.existsQuery(effectiveCriterion.getField()))
          .queryName(criterion.getField());
    }

    return getQueryBuilderFromCriterionForSingleField(
        effectiveCriterion,
        isTimeseries,
        searchableFieldTypes,
        criterion.getField(),
        opContext,
        queryFilterRewriteChain,
        resolvedForV3,
        v3FieldResolver != null);
  }

  /**
   * An unindexed label is searchable text. Existence uses the label's hasValues boolean. Every
   * other condition has no indexed value to match, so the criterion is left out. A group with no
   * clause left matches nothing.
   */
  @Nullable
  private static Criterion unindexedV3Criterion(
      @Nonnull SearchFieldResolver resolver, @Nonnull Criterion criterion) {
    if (criterion.getCondition() == Condition.EXISTS
        || criterion.getCondition() == Condition.IS_NULL) {
      Optional<String> hasValues = resolver.hasValuesFieldName(criterion.getField());
      if (hasValues.isPresent()) {
        return buildCriterion(
            hasValues.get(),
            criterion.getCondition(),
            criterion.isNegated(),
            criterion.hasValues() ? criterion.getValues() : null);
      }
    }
    log.debug(
        "Omitting Search V3 {} filter on {}: the field is not indexed for that condition",
        criterion.getCondition(),
        criterion.getField());
    return null;
  }

  /**
   * A {@code has*} name with no {@code searchLabel} is not mapped. True and {@code EXISTS} match
   * documents where the source field exists. False and {@code IS_NULL} match documents where it is
   * missing. A contains, range, or any other condition is not a boolean check and is left out.
   */
  @Nullable
  private static QueryBuilder unlabeledHasValuesQuery(
      @Nonnull SearchFieldResolver resolver,
      @Nonnull Criterion criterion,
      @Nonnull String sourceField) {
    Optional<Boolean> missing = unlabeledHasValuesMissing(criterion);
    if (missing.isEmpty()) {
      log.debug(
          "Omitting Search V3 {} filter on {}: a removed has-field only answers presence",
          criterion.getCondition(),
          criterion.getField());
      return null;
    }
    String path = resolver.exactPath(sourceField, Intent.FILTER);
    boolean isMissing = missing.get();
    if (criterion.isNegated()) {
      isMissing = !isMissing;
    }
    QueryBuilder exists = QueryBuilders.existsQuery(path);
    if (isMissing) {
      return QueryBuilders.boolQuery().mustNot(exists).queryName(criterion.getField());
    }
    return QueryBuilders.boolQuery().must(exists).queryName(criterion.getField());
  }

  /** Empty when the condition is not a true/false presence check. */
  private static Optional<Boolean> unlabeledHasValuesMissing(@Nonnull Criterion criterion) {
    Condition condition = criterion.getCondition();
    if (condition == Condition.IS_NULL) {
      return Optional.of(true);
    }
    if (condition == Condition.EXISTS) {
      return Optional.of(false);
    }
    if ((condition == Condition.EQUAL || condition == Condition.IEQUAL)
        && criterion.hasValues()
        && criterion.getValues().size() == 1) {
      String value = criterion.getValues().get(0);
      if ("true".equalsIgnoreCase(value)) {
        return Optional.of(false);
      }
      if ("false".equalsIgnoreCase(value)) {
        return Optional.of(true);
      }
    }
    return Optional.empty();
  }

  /** Opted-in keyword. Contains, prefix, and suffix read the folded subfield. */
  @Nonnull
  private static String lowercaseKeywordPath(
      @Nonnull SearchFieldResolver resolver, @Nonnull String requestedField, @Nonnull String path) {
    if (resolver.hasLowercaseSubfield(requestedField)
        && !path.endsWith("." + V3SearchFields.LOWERCASE)) {
      return path + "." + V3SearchFields.LOWERCASE;
    }
    return path;
  }

  @Nonnull
  private static QueryBuilder buildHasValuesExistenceQuery(
      @Nonnull String queryName, @Nonnull List<String> fields, boolean missing) {
    BoolQueryBuilder anyPresent = QueryBuilders.boolQuery().minimumShouldMatch(1);
    fields.forEach(field -> anyPresent.should(QueryBuilders.existsQuery(field)));
    if (missing) {
      return QueryBuilders.boolQuery().mustNot(anyPresent).queryName(queryName);
    }
    return anyPresent.queryName(queryName);
  }

  /**
   * Shared-label hasValues fields are never indexed as false, so false means the field is absent.
   */
  private static boolean isSharedHasValuesFalse(
      @Nullable SearchFieldResolver v3FieldResolver, @Nonnull Criterion criterion) {
    return v3FieldResolver != null
        && v3FieldResolver.falseIsMissing(criterion.getField())
        && (criterion.getCondition() == Condition.EQUAL
            || criterion.getCondition() == Condition.IEQUAL)
        && criterion.hasValues()
        && criterion.getValues().size() == 1
        && !Boolean.parseBoolean(criterion.getValues().get(0));
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
        searchSourceBuilder,
        sortCriteria == null ? List.of() : sortCriteria,
        entitySpecs,
        true,
        null);
  }

  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      List<SortCriterion> sortCriteria,
      List<EntitySpec> entitySpecs,
      @Nullable SearchFieldResolver v3FieldResolver) {
    buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs, v3FieldResolver, null, null);
  }

  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      List<SortCriterion> sortCriteria,
      List<EntitySpec> entitySpecs,
      @Nullable SearchFieldResolver v3FieldResolver,
      @Nullable OperationFingerprint opContext,
      @Nullable AspectRetriever aspectRetriever) {
    buildSortOrder(
        searchSourceBuilder,
        sortCriteria == null ? List.of() : sortCriteria,
        entitySpecs,
        true,
        v3FieldResolver,
        opContext,
        aspectRetriever);
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
    buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs, enableDefaultSort, null);
  }

  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable List<SortCriterion> sortCriteria,
      List<EntitySpec> entitySpecs,
      boolean enableDefaultSort,
      @Nullable SearchFieldResolver v3FieldResolver) {
    buildSortOrder(
        searchSourceBuilder,
        sortCriteria,
        entitySpecs,
        enableDefaultSort,
        v3FieldResolver,
        null,
        null);
  }

  public static void buildSortOrder(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable List<SortCriterion> sortCriteria,
      List<EntitySpec> entitySpecs,
      boolean enableDefaultSort,
      @Nullable SearchFieldResolver v3FieldResolver,
      @Nullable OperationFingerprint opContext,
      @Nullable AspectRetriever aspectRetriever) {
    if (sortCriteria.isEmpty() && enableDefaultSort) {
      searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
    } else {
      sortCriteria = sortCriteria != null ? sortCriteria : Collections.emptyList();
      for (SortCriterion sortCriterion : sortCriteria) {
        final String requestedField =
            v3FieldResolver == null
                ? sortCriterion.getField()
                : v3FieldResolver
                    .unlabeledHasValuesSource(sortCriterion.getField())
                    .orElse(sortCriterion.getField());
        // urn is a synthetic keyword root on every entity index, not an annotated entity field.
        Optional<SearchableAnnotation.FieldType> fieldTypeForDefault =
            DEFAULT_SEARCH_RESULTS_SORT_BY_FIELD.equals(requestedField)
                ? Optional.of(SearchableAnnotation.FieldType.KEYWORD)
                : Optional.empty();
        for (EntitySpec entitySpec : entitySpecs) {
          if (fieldTypeForDefault.isPresent()) {
            break;
          }
          List<SearchableFieldSpec> fieldSpecs = entitySpec.getSearchableFieldSpecs();
          for (SearchableFieldSpec fieldSpec : fieldSpecs) {
            SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
            if (annotation.getFieldName().equals(requestedField)
                || annotation.getFieldNameAliases().contains(requestedField)) {
              fieldTypeForDefault = Optional.of(fieldSpec.getSearchableAnnotation().getFieldType());
              break;
            }
          }
          if (fieldTypeForDefault.isPresent()) {
            break;
          }
        }
        if (fieldTypeForDefault.isEmpty()
            && !entitySpecs.isEmpty()
            && !StructuredPropertyUtils.isStructuredPropertyField(requestedField)) {
          log.warn(
              "Sort criterion field "
                  + requestedField
                  + " was not found in any entity spec to be searched");
        }
        final SortOrder esSortOrder =
            (sortCriterion.getOrder() == com.linkedin.metadata.query.filter.SortOrder.ASCENDING)
                ? SortOrder.ASC
                : SortOrder.DESC;
        FieldSortBuilder sortBuilder =
            new FieldSortBuilder(
                    sortField(opContext, requestedField, aspectRetriever, v3FieldResolver))
                .order(esSortOrder);
        if (fieldTypeForDefault.isPresent()) {
          String esFieldtype = getElasticTypeForFieldType(fieldTypeForDefault.get());
          if (esFieldtype != null) {
            sortBuilder.unmappedType(esFieldtype);
          }
        } else {
          // A structured property is not an entity-spec field, so the loop above does not
          // learn a type. An index whose entity does not declare the property has no mapping.
          // unmappedType has to match the mapped type or the cross-index merge rejects the sort.
          structuredPropertySortType(opContext, aspectRetriever, sortCriterion.getField())
              .ifPresent(sortBuilder::unmappedType);
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
   * Field a sort reads. An entity field on Search V3 goes through {@link SearchFieldResolver}. A
   * structured property is not an entity field, so it uses {@link #toV3KeywordField}, the same
   * resolver filters and facets use: the keyword root the index maps, with a caller-supplied {@code
   * .keyword} removed. V2 keeps the field the caller named.
   */
  @Nonnull
  private static String sortField(
      @Nullable OperationFingerprint opContext,
      @Nonnull String field,
      @Nullable AspectRetriever aspectRetriever,
      @Nullable SearchFieldResolver v3FieldResolver) {
    if (v3FieldResolver != null && v3FieldResolver.contains(field)) {
      return v3FieldResolver.exactPath(field, Intent.SORT);
    }
    if (v3FieldResolver != null && StructuredPropertyUtils.isStructuredPropertyField(field)) {
      return toV3KeywordField(opContext, field, aspectRetriever);
    }
    return field;
  }

  /**
   * Elasticsearch type of a structured property, used as {@code unmapped_type} when another index
   * in the same search does not map the field. Empty when the definition is not loaded, so a
   * missing property still fails the query instead of sorting as the wrong type.
   */
  private static Optional<String> structuredPropertySortType(
      @Nullable OperationFingerprint opContext,
      @Nullable AspectRetriever aspectRetriever,
      @Nonnull String field) {
    if (aspectRetriever == null || !StructuredPropertyUtils.isStructuredPropertyField(field)) {
      return Optional.empty();
    }
    if (StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(
            opContext, field, aspectRetriever)
        .isEmpty()) {
      return Optional.empty();
    }
    Set<String> types =
        StructuredPropertyUtils.toElasticsearchFieldType(opContext, field, aspectRetriever);
    if (types.size() != 1) {
      return Optional.empty();
    }
    return Optional.of(types.iterator().next());
  }

  /**
   * Populates source field of search query with the suggestions query so that we get search
   * suggestions back. Right now we are only supporting suggestions based on the virtual _entityName
   * field alias.
   */
  public static void buildNameSuggestions(
      @Nonnull SearchSourceBuilder searchSourceBuilder, @Nullable String textInput) {
    buildNameSuggestions(searchSourceBuilder, textInput, ENTITY_NAME_FIELD);
  }

  public static void buildNameSuggestions(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable String textInput,
      @Nonnull String field) {
    SuggestionBuilder<TermSuggestionBuilder> builder =
        SuggestBuilders.termSuggestion(field).text(textInput);
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
   * @param opContext operation context for tenant-aware aspect retrieval
   * @param filterField name of the field used in the filter request
   * @param aspectRetriever aspect retriever, used if structured property
   * @return normalized field name without subfields
   */
  @Nonnull
  public static String toParentField(
      @Nullable final Object opContext,
      @Nonnull final String filterField,
      @Nullable final AspectRetriever aspectRetriever) {
    // Only structured-property filters need definition lookup to sanitize/version the ES field
    // name. Non-SP fields pass through unchanged aside from stripping known subfield suffixes.
    String fieldName =
        StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(
                (OperationFingerprint) opContext, filterField, aspectRetriever)
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
   * @param opContext operation context for tenant-aware aspect retrieval
   * @param filterField the field name used in the filter
   * @param skipKeywordSuffix prevent use of `keyword` subfield, useful when parent field is known
   *     or always `keyword`
   * @param aspectRetriever aspect retriever, used if structured property field
   * @return the preferred field to use for `keyword` queries
   */
  @Nonnull
  public static String toKeywordField(
      @Nullable final Object opContext,
      @Nonnull final String filterField,
      final boolean skipKeywordSuffix,
      @Nullable final AspectRetriever aspectRetriever) {
    // Structured properties: single source of truth for parent vs .keyword is
    // StructuredPropertyUtils.toStructuredPropertyFacetName (DATE/NUMBER/URN skip .keyword).
    Optional<String> structuredPropertyField =
        StructuredPropertyUtils.toStructuredPropertyFacetName(
            (OperationFingerprint) opContext, filterField, aspectRetriever);
    if (structuredPropertyField.isPresent()) {
      return skipKeywordSuffix
          ? replaceSuffix(structuredPropertyField.get())
          : structuredPropertyField.get();
    }

    // Definition lookup failed. If the caller already passed a versioned ES path, infer type from
    // the trailing segment so STRING still gets .keyword and URN/DATE/NUMBER do not. Unversioned
    // FQN misses prefer the parent (strip any caller-supplied .keyword / other subfields) so
    // typed parents are not queried on a missing multi-field.
    if (filterField.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
      Optional<LogicalValueType> inferredType =
          StructuredPropertyUtils.getLogicalValueTypeFromFieldName(filterField);
      if (inferredType.isPresent()) {
        String parent = replaceSuffix(filterField);
        if (skipKeywordSuffix || !StructuredPropertyUtils.usesKeywordSubfield(inferredType.get())) {
          return parent;
        }
        return parent + ESUtils.KEYWORD_SUFFIX;
      }
      return replaceSuffix(filterField);
    }

    return skipKeywordSuffix
            || KEYWORD_FIELDS.contains(filterField)
            || KEYWORD_FIELDS.stream()
                .anyMatch(nestedField -> filterField.endsWith("." + nestedField))
            || PATH_HIERARCHY_FIELDS.contains(filterField)
            || SUBFIELDS.stream().anyMatch(subfield -> filterField.endsWith("." + subfield))
        ? filterField
        : filterField + ESUtils.KEYWORD_SUFFIX;
  }

  /**
   * {@link #toKeywordField} for a Search V3 index. A V3 keyword root is the exact field, so this
   * never appends {@code .keyword} and drops one the caller wrote for V2.
   */
  @Nonnull
  public static String toV3KeywordField(
      @Nullable final Object opContext,
      @Nonnull final String filterField,
      @Nullable final AspectRetriever aspectRetriever) {
    return V3SearchFields.rootPath(toKeywordField(opContext, filterField, true, aspectRetriever));
  }

  /** {@link #toV3KeywordField} on a V3 index, otherwise {@link #toKeywordField}. */
  @Nonnull
  public static String toKeywordField(
      @Nullable final Object opContext,
      @Nonnull final String filterField,
      final boolean skipKeywordSuffix,
      @Nullable final AspectRetriever aspectRetriever,
      final boolean v3Index) {
    return v3Index
        ? toV3KeywordField(opContext, filterField, aspectRetriever)
        : toKeywordField(opContext, filterField, skipKeywordSuffix, aspectRetriever);
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
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      boolean v3Index) {
    final BoolQueryBuilder orQueryBuilder = new BoolQueryBuilder().minimumShouldMatch(1);
    for (String field : fields) {
      orQueryBuilder.should(
          getQueryBuilderFromCriterionForSingleField(
                  buildCriterion(
                      toKeywordField(
                          opContext, field, isTimeseries, opContext.getAspectRetriever(), v3Index),
                      criterion.getCondition(),
                      criterion.isNegated(),
                      criterion.getValues()),
                  isTimeseries,
                  searchableFieldTypes,
                  null,
                  opContext,
                  queryFilterRewriteChain,
                  false,
                  v3Index)
              .queryName(field));
    }
    return orQueryBuilder;
  }

  private static boolean isCaseInsensitiveSearchEnabled(Condition condition) {
    return condition == Condition.IEQUAL;
  }

  /**
   * A URN structured property, or its parsed {@code __urn_name} sibling. On a Search V3 index,
   * case-insensitive match reads {@code .lowercase}. Search V2 has no such subfield.
   */
  private static boolean isStructuredPropertyUrnKeyword(
      @Nullable final Object opContext,
      @Nonnull final String criterionField,
      @Nullable final AspectRetriever aspectRetriever,
      @Nonnull final String keywordField) {
    if (!keywordField.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
      return false;
    }
    if (keywordField.endsWith(StructuredPropertyUrnIdentity.FIELD_SUFFIX)
        || replaceSuffix(keywordField).endsWith(StructuredPropertyUrnIdentity.FIELD_SUFFIX)) {
      return true;
    }
    Optional<LogicalValueType> fromPath =
        StructuredPropertyUtils.getLogicalValueTypeFromFieldName(keywordField);
    if (fromPath.isPresent()) {
      return fromPath.get() == LogicalValueType.URN;
    }
    if (aspectRetriever == null) {
      return false;
    }
    try {
      return StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(
              opContext instanceof OperationFingerprint fingerprint ? fingerprint : null,
              criterionField,
              aspectRetriever)
          .map(
              definition ->
                  StructuredPropertyUtils.getLogicalValueType(definition.getSecond())
                      == LogicalValueType.URN)
          .orElse(false);
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  /**
   * Same token {@code keyword_normalizer} stores: lowercase, then ASCII folding. A {@code term}
   * query does not run the normalizer, so the query value has to be folded first.
   */
  @Nonnull
  private static BoolQueryBuilder lowercaseTerms(
      @Nonnull final String field, @Nonnull final Criterion criterion) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    criterion
        .getValues()
        .forEach(
            value ->
                boolQuery.should(QueryBuilders.termQuery(field, keywordNormalize(value.trim()))));
    if (!boolQuery.should().isEmpty()) {
      boolQuery.minimumShouldMatch(1);
    }
    return boolQuery;
  }

  @Nonnull
  public static String keywordNormalize(@Nonnull final String value) {
    final String lower = value.toLowerCase(Locale.ROOT);
    final char[] chars = lower.toCharArray();
    final char[] folded = new char[chars.length * 4];
    final int length = ASCIIFoldingFilter.foldToASCII(chars, 0, folded, 0, chars.length);
    return new String(folded, 0, length);
  }

  @Nonnull
  private static QueryBuilder getQueryBuilderFromCriterionForSingleField(
      @Nonnull Criterion criterion,
      boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nullable String queryName,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      boolean resolvedForV3,
      boolean v3Index) {
    final Condition condition = criterion.getCondition();
    final AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    final String fieldName =
        resolvedForV3
            ? criterion.getField()
            : toParentField(opContext, criterion.getField(), aspectRetriever);

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
                opContext,
                aspectRetriever,
                enableCaseInsensitiveSearch,
                resolvedForV3,
                v3Index)
            .queryName(queryName != null ? queryName : fieldName);
      } else if (RANGE_QUERY_CONDITIONS.contains(condition)) {
        return buildRangeQueryFromCriterion(
                criterion,
                fieldName,
                searchableFieldTypes,
                condition,
                isTimeseries,
                opContext,
                aspectRetriever,
                resolvedForV3,
                v3Index)
            .queryName(queryName != null ? queryName : fieldName);
      } else if (condition == Condition.CONTAIN) {
        return buildContainsConditionFromCriterion(
            fieldName,
            criterion,
            queryName,
            isTimeseries,
            opContext,
            aspectRetriever,
            resolvedForV3,
            v3Index);
      } else if (condition == Condition.START_WITH) {
        return buildStartsWithConditionFromCriterion(
            fieldName,
            criterion,
            queryName,
            isTimeseries,
            opContext,
            aspectRetriever,
            resolvedForV3,
            v3Index);
      } else if (condition == Condition.END_WITH) {
        return buildEndsWithConditionFromCriterion(
            fieldName,
            criterion,
            queryName,
            isTimeseries,
            opContext,
            aspectRetriever,
            resolvedForV3,
            v3Index);
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
                    opContext,
                    aspectRetriever,
                    enableCaseInsensitiveSearch,
                    resolvedForV3,
                    v3Index))
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
      @Nullable final Object opContext,
      @Nonnull AspectRetriever aspectRetriever,
      String wildcardPattern,
      boolean resolvedForV3,
      boolean v3Index) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
    String name = queryName != null ? queryName : fieldName;

    for (String value : criterion.getValues()) {
      String field =
          resolvedForV3
              ? fieldName
              : toKeywordField(
                  opContext, criterion.getField(), isTimeseries, aspectRetriever, v3Index);
      String raw = value.trim();
      // v3.lowercase stores the keyword_normalizer token. A wildcard does not run it.
      // case_insensitive wildcards fail the shard on OpenSearch 3, so a keyword without that
      // subfield matches the written pattern and its lowercase form.
      if (field.endsWith("." + V3SearchFields.LOWERCASE)) {
        raw = keywordNormalize(raw);
      }
      String pattern = String.format(wildcardPattern, ESUtils.escapeReservedCharacters(raw));
      if (v3Index) {
        boolQuery.should(wildcard(field, pattern, name));
        if (!field.endsWith("." + V3SearchFields.LOWERCASE)) {
          String folded = pattern.toLowerCase(Locale.ROOT);
          if (!folded.equals(pattern)) {
            boolQuery.should(wildcard(field, folded, name));
          }
        }
      } else {
        boolQuery.should(wildcard(field, pattern, name).caseInsensitive(true));
      }
    }
    return boolQuery;
  }

  @Nonnull
  private static WildcardQueryBuilder wildcard(
      @Nonnull String field, @Nonnull String pattern, @Nonnull String name) {
    return QueryBuilders.wildcardQuery(field, pattern).queryName(name);
  }

  /**
   * A contains filter on a text-only label matches each analyzed subfield. Every token must match,
   * and any subfield is enough. Multiple values stay in disjunction, as a keyword wildcard does.
   */
  @Nonnull
  private static QueryBuilder buildAnalyzedContainsQuery(
      @Nonnull String queryName, @Nonnull List<String> paths, @Nonnull Criterion criterion) {
    BoolQueryBuilder values = QueryBuilders.boolQuery().minimumShouldMatch(1);
    for (String value : criterion.getValues()) {
      BoolQueryBuilder fields = QueryBuilders.boolQuery().minimumShouldMatch(1);
      for (String path : paths) {
        fields.should(QueryBuilders.matchQuery(path, value.trim()).operator(Operator.AND));
      }
      values.should(fields);
    }
    return values.queryName(queryName);
  }

  private static QueryBuilder buildContainsConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable String queryName,
      final boolean isTimeseries,
      @Nullable final Object opContext,
      @Nonnull AspectRetriever aspectRetriever,
      boolean resolvedForV3,
      boolean v3Index) {
    return buildWildcardQueryWithMultipleValues(
        fieldName,
        criterion,
        isTimeseries,
        queryName,
        opContext,
        aspectRetriever,
        "*%s*",
        resolvedForV3,
        v3Index);
  }

  private static QueryBuilder buildStartsWithConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable String queryName,
      final boolean isTimeseries,
      @Nullable final Object opContext,
      @Nonnull AspectRetriever aspectRetriever,
      boolean resolvedForV3,
      boolean v3Index) {
    return buildWildcardQueryWithMultipleValues(
        fieldName,
        criterion,
        isTimeseries,
        queryName,
        opContext,
        aspectRetriever,
        "%s*",
        resolvedForV3,
        v3Index);
  }

  private static QueryBuilder buildEndsWithConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable String queryName,
      final boolean isTimeseries,
      @Nullable final Object opContext,
      @Nonnull AspectRetriever aspectRetriever,
      boolean resolvedForV3,
      boolean v3Index) {
    return buildWildcardQueryWithMultipleValues(
        fieldName,
        criterion,
        isTimeseries,
        queryName,
        opContext,
        aspectRetriever,
        "*%s",
        resolvedForV3,
        v3Index);
  }

  private static QueryBuilder buildEqualsConditionFromCriterion(
      @Nonnull final String fieldName,
      @Nonnull final Criterion criterion,
      final boolean isTimeseries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nullable final Object opContext,
      @Nonnull AspectRetriever aspectRetriever,
      boolean enableCaseInsensitiveSearch,
      boolean resolvedForV3,
      boolean v3Index) {
    return buildEqualsConditionFromCriterionWithValues(
        fieldName,
        criterion,
        isTimeseries,
        searchableFieldTypes,
        opContext,
        aspectRetriever,
        enableCaseInsensitiveSearch,
        resolvedForV3,
        v3Index);
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
      @Nullable final Object opContext,
      @Nonnull AspectRetriever aspectRetriever,
      boolean enableCaseInsensitiveSearch,
      boolean resolvedForV3,
      boolean v3Index) {
    Set<String> fieldTypes =
        getFieldTypes(searchableFieldTypes, fieldName, criterion, opContext, aspectRetriever);
    if (fieldTypes.size() > 1) {
      log.debug(
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
      String keywordField =
          resolvedForV3
              ? fieldName
              : toKeywordField(
                  opContext, criterion.getField(), isTimeseries, aspectRetriever, v3Index);
      if (v3Index
          && !resolvedForV3
          && isStructuredPropertyUrnKeyword(
              opContext, criterion.getField(), aspectRetriever, keywordField)) {
        return lowercaseTerms(keywordField + "." + V3SearchFields.LOWERCASE, criterion);
      }
      // A resolved URN IEQUAL already names <field>_urn.lowercase. Term queries do not run
      // keyword_normalizer, and caseInsensitive does not ASCII-fold.
      if (resolvedForV3 && keywordField.endsWith("." + V3SearchFields.LOWERCASE)) {
        return lowercaseTerms(keywordField, criterion);
      }
      BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
      criterion
          .getValues()
          .forEach(
              value ->
                  boolQuery.should(
                      QueryBuilders.termQuery(
                              resolvedForV3
                                  ? fieldName
                                  : toKeywordField(
                                      opContext,
                                      criterion.getField(),
                                      isTimeseries,
                                      aspectRetriever,
                                      v3Index),
                              value.trim())
                          .caseInsensitive(true)));
      if (!boolQuery.should().isEmpty()) {
        boolQuery.minimumShouldMatch(1);
      }
      return boolQuery;
    }

    return QueryBuilders.termsQuery(
            resolvedForV3
                ? fieldName
                : toKeywordField(
                    opContext, criterion.getField(), isTimeseries, aspectRetriever, v3Index),
            criterion.getValues())
        .queryName(fieldName);
  }

  private static Set<String> getFieldTypes(
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFields,
      String fieldName,
      @Nonnull final Criterion criterion,
      @Nullable final Object opContext,
      @Nullable AspectRetriever aspectRetriever) {
    final Set<String> finalFieldTypes;
    if (fieldName.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
      // use criterion field here for structured props since fieldName has dots replaced with
      // underscores
      finalFieldTypes =
          StructuredPropertyUtils.toElasticsearchFieldType(
              (OperationFingerprint) opContext,
              replaceSuffix(criterion.getField()),
              aspectRetriever);
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
      log.debug(
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
      @Nullable final Object opContext,
      AspectRetriever aspectRetriever,
      boolean resolvedForV3,
      boolean v3Index) {
    Set<String> fieldTypes =
        getFieldTypes(searchableFieldTypes, fieldName, criterion, opContext, aspectRetriever);

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
      documentFieldName =
          resolvedForV3
              ? fieldName
              : toKeywordField(opContext, fieldName, isTimeseries, aspectRetriever, v3Index);
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

  /**
   * Resolves the set of hidden lifecycle stage URNs for the given entity types, or empty if the
   * service is not wired yet or the search targets lifecycle stage types themselves.
   */
  @Nonnull
  public static Set<String> resolveHiddenStageUrns(@Nonnull List<String> entityNames) {
    return (lifecycleStageTypeService != null
            && !entityNames.contains(LifecycleStageTypeService.LIFECYCLE_STAGE_TYPE_ENTITY_NAME))
        ? lifecycleStageTypeService.getHiddenStageUrns(new HashSet<>(entityNames))
        : Collections.emptySet();
  }

  @Nonnull
  public static BoolQueryBuilder applyDefaultSearchFilters(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery) {
    return applyDefaultSearchFilters(
        opContext, entityNames, filter, filterQuery, resolveHiddenStageUrns(entityNames), null);
  }

  @Nonnull
  public static BoolQueryBuilder applyDefaultSearchFilters(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery,
      @Nullable EntityIndexConfiguration entityIndexConfiguration) {
    return applyDefaultSearchFilters(
        opContext,
        entityNames,
        filter,
        filterQuery,
        resolveHiddenStageUrns(entityNames),
        entityIndexConfiguration);
  }

  @Nonnull
  public static BoolQueryBuilder applyDefaultSearchFilters(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery,
      @Nonnull Set<String> hiddenLifecycleStageUrns) {
    return applyDefaultSearchFilters(
        opContext, entityNames, filter, filterQuery, hiddenLifecycleStageUrns, null);
  }

  @Nonnull
  public static BoolQueryBuilder applyDefaultSearchFilters(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery,
      @Nonnull Set<String> hiddenLifecycleStageUrns,
      @Nullable EntityIndexConfiguration entityIndexConfiguration) {
    filterSoftDeletedAndHiddenStages(
        filter,
        filterQuery,
        opContext.getSearchContext().getSearchFlags(),
        hiddenLifecycleStageUrns,
        entityIndexConfiguration);
    EntityFilter.applyEntityTypeFilter(filterQuery, entityNames, entityIndexConfiguration);
    return filterQuery;
  }

  /**
   * Applies default filters to exclude soft-deleted entities and entities in hidden lifecycle
   * stages unless the caller has explicitly opted in or already filtered on those fields.
   *
   * <p>Soft-delete exclusion (removed=true): controlled by SearchFlags.includeSoftDeleted.
   *
   * <p>Lifecycle stage exclusion: driven by {@code hiddenLifecycleStageUrns} — the set of lifecycle
   * stage type URNs whose settings specify {@code hideInSearch=true}. Controlled by
   * SearchFlags.includeHiddenLifecycleStages. When the caller already filters on the {@code
   * lifecycleStage} field, the default exclusion is bypassed.
   */
  private static void filterSoftDeletedAndHiddenStages(
      @Nullable Filter filter,
      @Nonnull BoolQueryBuilder filterQuery,
      @Nonnull SearchFlags searchFlags,
      @Nonnull Set<String> hiddenLifecycleStageUrns,
      @Nullable EntityIndexConfiguration entityIndexConfiguration) {
    boolean removedInOrFilter = false;
    boolean lifecycleStageInOrFilter = false;
    if (filter != null) {
      for (var or : filter.getOr()) {
        for (var criterion : or.getAnd()) {
          String field = criterion.getField();
          if (field.equals(REMOVED) || field.equals(REMOVED + KEYWORD_SUFFIX)) {
            removedInOrFilter = true;
          }
          if (field.equals(LIFECYCLE_STAGE) || field.equals(LIFECYCLE_STAGE + KEYWORD_SUFFIX)) {
            lifecycleStageInOrFilter = true;
          }
        }
      }
    }

    if (Boolean.FALSE.equals(searchFlags.isIncludeSoftDeleted()) && !removedInOrFilter) {
      filterQuery.mustNot(QueryBuilders.termQuery(REMOVED, true));
    }

    // Exclude entities in hidden lifecycle stages unless the caller opted in or already filters.
    if (!Boolean.TRUE.equals(searchFlags.isIncludeHiddenLifecycleStages())
        && !lifecycleStageInOrFilter
        && !hiddenLifecycleStageUrns.isEmpty()) {
      // V3 stores the raw stage URN on the keyword root. V2 stores it on .keyword and normalizes
      // the root, so a term on the root would not match the URN.
      String stageField =
          EntitySearchIndexResolver.shouldReadV3(entityIndexConfiguration)
              ? LIFECYCLE_STAGE
              : LIFECYCLE_STAGE + KEYWORD_SUFFIX;
      for (String stageUrn : hiddenLifecycleStageUrns) {
        filterQuery.mustNot(QueryBuilders.termQuery(stageField, stageUrn));
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
      @Nonnull OperationContext opContext,
      String scrollId,
      String keepAlive,
      SearchClientShim<?> client,
      String... indexArray) {
    if (scrollId != null) {
      SearchAfterWrapper searchAfterWrapper = SearchAfterWrapper.fromScrollId(scrollId);
      if (System.currentTimeMillis() + 10000 <= searchAfterWrapper.getExpirationTime()) {
        return searchAfterWrapper.getPitId();
      }
    }
    switch (client.getEngineType()) {
      case ELASTICSEARCH_8:
      case OPENSEARCH_2:
      case OPENSEARCH_3:
      case ELASTICSEARCH_9:
        return createPointInTimeOpenSearch(opContext, client, indexArray, keepAlive);
      default:
        log.warn("Unsupported elasticsearch implementation: {}", client.getEngineType());
        throw new IllegalStateException("Unsupported elasticsearch implementation.");
    }
  }

  private static @Nonnull String createPointInTimeOpenSearch(
      @Nonnull OperationContext opContext,
      SearchClientShim<?> client,
      String[] indexArray,
      String keepAlive) {
    try {
      CreatePitRequest request =
          new CreatePitRequest(TimeValue.parseTimeValue(keepAlive, "keepAlive"), false, indexArray);
      CreatePitResponse response = client.createPit(opContext, request, RequestOptions.DEFAULT);
      return response.getId();
    } catch (OpenSearchStatusException ose) {
      if (TOO_MANY_REQUESTS.equals(ose.status())) {
        APIThrottleException throttleException =
            new APIThrottleException(
                TimeValue.parseTimeValue(keepAlive, "keepAlive").millis(),
                "Too many point in times created, retry after keep alive has expired.",
                "opensearch-pit",
                ThrottleMechanismType.SEARCH,
                ThrottleResponseSource.OPENSEARCH);
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
  public static void cleanupPointInTime(
      @Nonnull OperationContext opContext,
      SearchClientShim<?> client,
      String pitId,
      String context) {
    if (pitId == null) {
      return;
    }

    try {
      switch (client.getEngineType()) {
        case OPENSEARCH_2:
        case OPENSEARCH_3:
        case ELASTICSEARCH_8:
        case ELASTICSEARCH_9:
          {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            client.deletePit(opContext, deletePitRequest, RequestOptions.DEFAULT);
            log.debug("Successfully cleaned up PIT {} for {}", pitId, context);
            break;
          }
        default:
          log.warn(
              "Skipping PIT cleanup for unsupported engine type {} ({})",
              client.getEngineType(),
              context);
      }
    } catch (Exception e) {
      log.warn("Error cleaning up PIT {} for {}: {}", pitId, context, e.getMessage());
    }
  }
}

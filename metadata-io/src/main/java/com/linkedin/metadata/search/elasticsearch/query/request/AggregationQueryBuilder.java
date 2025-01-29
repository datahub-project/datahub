package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toStructuredPropertyFacetName;
import static com.linkedin.metadata.search.utils.ESUtils.toParentField;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.missing.ParsedMissing;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;

@Slf4j
public class AggregationQueryBuilder {
  private static final String URN_FILTER = "urn";
  private static final String STRUCTURED_PROPERTIES_PREFIX = "structuredProperties.";
  private final SearchConfiguration configs;
  private final Set<String> defaultFacetFields;
  private final Set<String> allFacetFields;
  private final Map<EntitySpec, List<SearchableAnnotation>> entitySearchAnnotations;

  private Map<String, String> filtersToDisplayName;

  public AggregationQueryBuilder(
      @Nonnull final SearchConfiguration configs,
      @Nonnull Map<EntitySpec, List<SearchableAnnotation>> entitySearchAnnotations) {
    this.configs = Objects.requireNonNull(configs, "configs must not be null");
    this.entitySearchAnnotations = entitySearchAnnotations;

    List<SearchableAnnotation> annotations =
        this.entitySearchAnnotations.values().stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    this.defaultFacetFields = getDefaultFacetFields(annotations);
    this.allFacetFields = getAllFacetFields(annotations);
  }

  /** Get the set of default aggregations, across all facets. */
  public List<AggregationBuilder> getAggregations(@Nonnull OperationContext opContext) {
    return getAggregations(opContext, null);
  }

  /**
   * Get aggregations for a search request for the given facets provided, and if none are provided,
   * then get aggregations for all.
   */
  public List<AggregationBuilder> getAggregations(
      @Nonnull OperationContext opContext, @Nullable List<String> facets) {
    final Set<String> facetsToAggregate = new HashSet<>();
    if (Boolean.TRUE.equals(
        opContext.getSearchContext().getSearchFlags().isIncludeDefaultFacets())) {
      facetsToAggregate.addAll(defaultFacetFields);
    }
    if (facets != null) {
      facets.stream().filter(this::isValidAggregate).forEach(facetsToAggregate::add);
    }
    return facetsToAggregate.stream()
        .map(f -> facetToAggregationBuilder(opContext, f))
        .collect(Collectors.toList());
  }

  private Set<String> getDefaultFacetFields(final List<SearchableAnnotation> annotations) {
    Set<String> facets =
        annotations.stream()
            .flatMap(annotation -> getDefaultFacetFieldsFromAnnotation(annotation).stream())
            .collect(Collectors.toSet());
    facets.add(INDEX_VIRTUAL_FIELD);
    // Entity Type > Sub-Type aggregations.
    // TODO: Ideally we can avoid the index-virtual-fields aggregation.
    facets.add(
        String.format("%s%s%s", INDEX_VIRTUAL_FIELD, AGGREGATION_SEPARATOR_CHAR, "typeNames"));
    return facets;
  }

  private Set<String> getAllFacetFields(final List<SearchableAnnotation> annotations) {
    Set<String> facets =
        annotations.stream()
            .flatMap(annotation -> getAllFacetFieldsFromAnnotation(annotation).stream())
            .collect(Collectors.toSet());
    facets.add(INDEX_VIRTUAL_FIELD);
    return facets;
  }

  private boolean isValidAggregate(final String inputFacet) {
    List<String> facets = List.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    boolean isValid =
        !facets.isEmpty()
            && ((facets.size() == 1
                    && facets.get(0).startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + "."))
                || allFacetFields.containsAll(facets));
    if (!isValid) {
      log.warn(
          String.format(
              "Requested facet for search filter aggregations that isn't part of the filters. "
                  + "Provided: %s; Available: %s",
              inputFacet, allFacetFields));
    }
    return isValid;
  }

  private AggregationBuilder facetToAggregationBuilder(
      @Nonnull OperationContext opContext, final String inputFacet) {
    List<String> facets = List.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    AggregationBuilder lastAggBuilder = null;
    int maxTermBuckets =
        Math.min(
            opContext.getSearchContext().getSearchFlags().getMaxAggValues(),
            configs.getMaxTermBucketSize());
    for (int i = facets.size() - 1; i >= 0; i--) {
      String facet = facets.get(i);

      AggregationBuilder aggBuilder;
      if (facet.contains(AGGREGATION_SPECIAL_TYPE_DELIMITER)) {
        List<String> specialTypeFields = List.of(facet.split(AGGREGATION_SPECIAL_TYPE_DELIMITER));
        switch (specialTypeFields.get(0)) {
          case MISSING_SPECIAL_TYPE:
            aggBuilder =
                INDEX_VIRTUAL_FIELD.equalsIgnoreCase(specialTypeFields.get(1))
                    ? AggregationBuilders.missing(inputFacet)
                        .field(getAggregationField(ES_INDEX_FIELD, opContext.getAspectRetriever()))
                    : AggregationBuilders.missing(inputFacet)
                        .field(
                            getAggregationField(
                                specialTypeFields.get(1), opContext.getAspectRetriever()));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown special type: " + specialTypeFields.get(0));
        }
      } else {
        aggBuilder =
            facet.equalsIgnoreCase(INDEX_VIRTUAL_FIELD)
                ? AggregationBuilders.terms(inputFacet)
                    .field(getAggregationField(ES_INDEX_FIELD, opContext.getAspectRetriever()))
                    .size(maxTermBuckets)
                    .minDocCount(0)
                : AggregationBuilders.terms(inputFacet)
                    .field(getAggregationField(facet, opContext.getAspectRetriever()))
                    .size(maxTermBuckets);
      }
      if (lastAggBuilder != null) {
        aggBuilder = aggBuilder.subAggregation(lastAggBuilder);
      }
      lastAggBuilder = aggBuilder;
    }
    // Logic to build nested facets
    return lastAggBuilder;
  }

  private String getAggregationField(
      final String facet, @Nullable AspectRetriever aspectRetriever) {
    if (facet.startsWith("has")) {
      // Boolean hasX field, not a keyword field. Return the name of the original facet.
      return facet;
    }
    // intercept structured property if it exists
    return toStructuredPropertyFacetName(facet, aspectRetriever)
        // Otherwise assume that this field is of keyword type.
        .orElse(ESUtils.toKeywordField(facet, false, aspectRetriever));
  }

  List<String> getDefaultFacetFieldsFromAnnotation(final SearchableAnnotation annotation) {
    final List<String> facetsFromAnnotation = new ArrayList<>();
    if (annotation.isAddToFilters()) {
      facetsFromAnnotation.add(annotation.getFieldName());
    }
    if (annotation.isAddHasValuesToFilters() && annotation.getHasValuesFieldName().isPresent()) {
      facetsFromAnnotation.add(annotation.getHasValuesFieldName().get());
    }
    if (annotation.isIncludeQueryEmptyAggregation()) {
      facetsFromAnnotation.add(
          MISSING_SPECIAL_TYPE + AGGREGATION_SPECIAL_TYPE_DELIMITER + annotation.getFieldName());
    }
    return facetsFromAnnotation;
  }

  List<String> getAllFacetFieldsFromAnnotation(final SearchableAnnotation annotation) {
    final List<String> facetsFromAnnotation = new ArrayList<>();
    facetsFromAnnotation.add(annotation.getFieldName());
    if (annotation.getHasValuesFieldName().isPresent()) {
      facetsFromAnnotation.add(annotation.getHasValuesFieldName().get());
    }
    if (annotation.isIncludeQueryEmptyAggregation()) {
      facetsFromAnnotation.add(
          MISSING_SPECIAL_TYPE + AGGREGATION_SPECIAL_TYPE_DELIMITER + annotation.getFieldName());
    }
    return facetsFromAnnotation;
  }

  private String computeDisplayName(String name) {
    if (getFacetToDisplayNames().containsKey(name)) {
      return getFacetToDisplayNames().get(name);
    } else if (name.contains(AGGREGATION_SEPARATOR_CHAR)) {
      return Arrays.stream(name.split(AGGREGATION_SEPARATOR_CHAR))
          .map(i -> getFacetToDisplayNames().get(i))
          .collect(Collectors.joining(AGGREGATION_SEPARATOR_CHAR));
    }
    return name;
  }

  List<AggregationMetadata> extractAggregationMetadata(
      @Nonnull SearchResponse searchResponse,
      @Nullable Filter filter,
      @Nullable AspectRetriever aspectRetriever) {
    final List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();
    if (searchResponse.getAggregations() == null) {
      return addFiltersToAggregationMetadata(aggregationMetadataList, filter, aspectRetriever);
    }
    for (Map.Entry<String, Aggregation> entry :
        searchResponse.getAggregations().getAsMap().entrySet()) {
      if (entry.getValue() instanceof ParsedTerms) {
        processTermAggregations(entry, aggregationMetadataList);
      }
      if (entry.getValue() instanceof ParsedMissing) {
        processMissingAggregations(entry, aggregationMetadataList);
      }
    }
    return addFiltersToAggregationMetadata(aggregationMetadataList, filter, aspectRetriever);
  }

  public void processTermAggregations(
      final Map.Entry<String, Aggregation> entry,
      final List<AggregationMetadata> aggregationMetadataList) {
    final Map<String, Long> oneTermAggResult =
        extractTermAggregations(
            (ParsedTerms) entry.getValue(), entry.getKey().equals(INDEX_VIRTUAL_FIELD));
    if (oneTermAggResult.isEmpty()) {
      return;
    }
    final AggregationMetadata aggregationMetadata =
        new AggregationMetadata()
            .setName(entry.getKey())
            .setDisplayName(computeDisplayName(entry.getKey()))
            .setAggregations(new LongMap(oneTermAggResult))
            .setFilterValues(
                new FilterValueArray(
                    SearchUtil.convertToFilters(oneTermAggResult, Collections.emptySet())));
    updateAggregationEntity(aggregationMetadata);
    aggregationMetadataList.add(aggregationMetadata);
  }

  /**
   * Adds nested sub-aggregation values to the aggregated results
   *
   * @param aggs The aggregations to traverse. Could be null (base case)
   * @return A map from names to aggregation count values
   */
  @Nonnull
  private static Map<String, Long> recursivelyAddNestedSubAggs(@Nullable Aggregations aggs) {
    final Map<String, Long> aggResult = new HashMap<>();

    if (aggs != null) {
      for (Map.Entry<String, Aggregation> entry : aggs.getAsMap().entrySet()) {
        if (entry.getValue() instanceof ParsedTerms) {
          recurseTermsAgg((ParsedTerms) entry.getValue(), aggResult, false);
        } else if (entry.getValue() instanceof ParsedMissing) {
          recurseMissingAgg((ParsedMissing) entry.getValue(), aggResult);
        } else {
          throw new UnsupportedOperationException(
              "Unsupported aggregation type: " + entry.getValue().getClass().getName());
        }
      }
    }
    return aggResult;
  }

  private static void recurseTermsAgg(
      ParsedTerms terms, Map<String, Long> aggResult, boolean includeZeroes) {
    List<? extends Terms.Bucket> bucketList = terms.getBuckets();
    bucketList.forEach(bucket -> processTermBucket(bucket, aggResult, includeZeroes));
  }

  private static void processTermBucket(
      Terms.Bucket bucket, Map<String, Long> aggResult, boolean includeZeroes) {
    final String key = bucket.getKeyAsString();
    String finalKey = key;
    try {
      // if the value is a date string, convert to milliseconds since epoch
      OffsetDateTime time = OffsetDateTime.parse(key);
      finalKey = String.valueOf(time.toEpochSecond() * 1000);
    } catch (DateTimeParseException e) {
      // do nothing, this is expected if the value is not a date
    }
    // Gets filtered sub aggregation doc count if exist
    Map<String, Long> subAggs = recursivelyAddNestedSubAggs(bucket.getAggregations());
    subAggs.forEach(
        (entryKey, entryValue) ->
            aggResult.put(
                String.format("%s%s%s", key, AGGREGATION_SEPARATOR_CHAR, entryKey), entryValue));
    long docCount = bucket.getDocCount();
    if (includeZeroes || docCount > 0) {
      aggResult.put(finalKey, docCount);
    }
  }

  private static void recurseMissingAgg(ParsedMissing missing, Map<String, Long> aggResult) {
    Map<String, Long> subAggs = recursivelyAddNestedSubAggs(missing.getAggregations());
    subAggs.forEach(
        (key, value) ->
            aggResult.put(
                String.format("%s%s%s", missing.getName(), AGGREGATION_SEPARATOR_CHAR, key),
                value));
    long docCount = missing.getDocCount();
    if (docCount > 0) {
      aggResult.put(missing.getName(), docCount);
    }
  }

  /**
   * Extracts term aggregations give a parsed term.
   *
   * @param terms an abstract parse term, input can be either ParsedStringTerms ParsedLongTerms
   * @return a map with aggregation key and corresponding doc counts
   */
  @Nonnull
  private static Map<String, Long> extractTermAggregations(
      @Nonnull ParsedTerms terms, boolean includeZeroes) {

    final Map<String, Long> aggResult = new HashMap<>();
    recurseTermsAgg(terms, aggResult, includeZeroes);

    return aggResult;
  }

  /** Injects the missing conjunctive filters into the aggregations list. */
  public List<AggregationMetadata> addFiltersToAggregationMetadata(
      @Nonnull final List<AggregationMetadata> originalMetadata,
      @Nullable final Filter filter,
      @Nullable AspectRetriever aspectRetriever) {
    if (filter == null) {
      return originalMetadata;
    }
    if (filter.getOr() != null) {
      addOrFiltersToAggregationMetadata(filter.getOr(), originalMetadata, aspectRetriever);
    } else if (filter.getCriteria() != null) {
      addCriteriaFiltersToAggregationMetadata(
          filter.getCriteria(), originalMetadata, aspectRetriever);
    }
    return originalMetadata;
  }

  void addOrFiltersToAggregationMetadata(
      @Nonnull final ConjunctiveCriterionArray or,
      @Nonnull final List<AggregationMetadata> originalMetadata,
      @Nullable AspectRetriever aspectRetriever) {
    for (ConjunctiveCriterion conjunction : or) {
      // For each item in the conjunction, inject an empty aggregation if necessary
      addCriteriaFiltersToAggregationMetadata(
          conjunction.getAnd(), originalMetadata, aspectRetriever);
    }
  }

  private void addCriteriaFiltersToAggregationMetadata(
      @Nonnull final CriterionArray criteria,
      @Nonnull final List<AggregationMetadata> originalMetadata,
      @Nullable AspectRetriever aspectRetriever) {
    for (Criterion criterion : criteria) {
      addCriterionFiltersToAggregationMetadata(criterion, originalMetadata, aspectRetriever);
    }
  }

  public void addCriterionFiltersToAggregationMetadata(
      @Nonnull final Criterion criterion,
      @Nonnull final List<AggregationMetadata> aggregationMetadata,
      @Nullable AspectRetriever aspectRetriever) {

    // We should never see duplicate aggregation for the same field in aggregation metadata list.
    final Map<String, AggregationMetadata> aggregationMetadataMap =
        aggregationMetadata.stream()
            .collect(Collectors.toMap(AggregationMetadata::getName, agg -> agg));

    // Map a filter criterion to a facet field (e.g. domains.keyword -> domains)
    final String finalFacetField = toParentField(criterion.getField(), aspectRetriever);

    if (finalFacetField == null) {
      log.warn(
          String.format(
              "Found invalid filter field for entity search. Invalid or unrecognized facet %s",
              criterion.getField()));
      return;
    }

    // We don't want to add urn filters to the aggregations we return as a sidecar to search
    // results.
    // They are automatically added by searchAcrossLineage and we dont need them to show up in the
    // filter panel.
    if (finalFacetField.equals(URN_FILTER)) {
      return;
    }

    if (aggregationMetadataMap.containsKey(finalFacetField)) {
      /*
       * If we already have aggregations for the facet field, simply inject any missing values counts into the set.
       * If there are no results for a particular facet value, it will NOT be in the original aggregation set returned by
       * Elasticsearch.
       */
      AggregationMetadata originalAggMetadata = aggregationMetadataMap.get(finalFacetField);
      if (criterion.hasValues()) {
        criterion
            .getValues()
            .forEach(
                value ->
                    addMissingAggregationValueToAggregationMetadata(value, originalAggMetadata));
      }
    } else if (aggregationMetadataMap.containsKey(criterion.getField())) {
      /*
       * If we already have aggregations for the facet field (original field name), simply inject any missing values counts into the set.
       * If there are no results for a particular facet value, it will NOT be in the original aggregation set returned by
       * Elasticsearch.
       */
      AggregationMetadata originalAggMetadata = aggregationMetadataMap.get(criterion.getField());
      criterion
          .getValues()
          .forEach(
              value -> addMissingAggregationValueToAggregationMetadata(value, originalAggMetadata));
    } else {
      /*
       * If we do not have ANY aggregation for the facet field, then inject a new aggregation metadata object for the
       * facet field.
       * If there are no results for a particular facet, it will NOT be in the original aggregation set returned by
       * Elasticsearch.
       */
      // Simply replace suffix from original field when there are no aggregations for it. Prevents
      // bug where ES mappings for field are different from how we map the field back to UI
      // (ie. Structured Properties with dots in them)
      String facetField = ESUtils.replaceSuffix(criterion.getField());
      aggregationMetadata.add(
          buildAggregationMetadata(
              facetField,
              getFacetToDisplayNames().getOrDefault(facetField, facetField),
              new LongMap(
                  criterion.getValues().stream().collect(Collectors.toMap(i -> i, i -> 0L))),
              new FilterValueArray(
                  criterion.getValues().stream()
                      .map(value -> createFilterValue(value, 0L, true))
                      .collect(Collectors.toList()))));
    }
  }

  private void addMissingAggregationValueToAggregationMetadata(
      @Nonnull final String value, @Nonnull final AggregationMetadata originalMetadata) {
    if (originalMetadata.getAggregations().entrySet().stream()
            .noneMatch(entry -> value.equals(entry.getKey()))
        || originalMetadata.getFilterValues().stream()
            .noneMatch(entry -> entry.getValue().equals(value))) {
      // No aggregation found for filtered value -- inject one!
      originalMetadata.getAggregations().put(value, 0L);
      originalMetadata.getFilterValues().add(createFilterValue(value, 0L, true));
    }
  }

  private AggregationMetadata buildAggregationMetadata(
      @Nonnull final String facetField,
      @Nonnull final String displayName,
      @Nonnull final LongMap aggValues,
      @Nonnull final FilterValueArray filterValues) {
    AggregationMetadata aggregationMetadata =
        new AggregationMetadata()
            .setName(facetField)
            .setDisplayName(displayName)
            .setAggregations(aggValues)
            .setFilterValues(filterValues);
    updateAggregationEntity(aggregationMetadata);
    return aggregationMetadata;
  }

  public void updateAggregationEntity(@Nonnull final AggregationMetadata aggregationMetadata) {
    if (aggregationMetadata.getName().startsWith(STRUCTURED_PROPERTIES_PREFIX)) {
      aggregationMetadata.setEntity(
          UrnUtils.getUrn(
              String.format(
                  "urn:li:structuredProperty:%s",
                  aggregationMetadata.getName().replaceFirst(STRUCTURED_PROPERTIES_PREFIX, ""))));
    }
  }

  private List<Pair<String, Pair<String, String>>> getFacetFieldDisplayNameFromAnnotation(
      @Nonnull EntitySpec entitySpec, @Nonnull final SearchableAnnotation annotation) {
    final List<Pair<String, Pair<String, String>>> facetsFromAnnotation = new ArrayList<>();
    // Case 1: Default Keyword field
    if (annotation.isAddToFilters()) {
      facetsFromAnnotation.add(
          Pair.of(
              annotation.getFieldName(),
              Pair.of(entitySpec.getName(), annotation.getFilterName())));
    }
    // Case 2: HasX boolean field
    if (annotation.isAddHasValuesToFilters() && annotation.getHasValuesFieldName().isPresent()) {
      facetsFromAnnotation.add(
          Pair.of(
              annotation.getHasValuesFieldName().get(),
              Pair.of(entitySpec.getName(), annotation.getHasValuesFilterName())));
    }
    return facetsFromAnnotation;
  }

  @WithSpan
  public static Map<String, Long> extractAggregationsFromResponse(
      @Nonnull SearchResponse searchResponse, @Nonnull String aggregationName) {
    if (searchResponse.getAggregations() == null) {
      return Collections.emptyMap();
    }

    Aggregation aggregation = searchResponse.getAggregations().get(aggregationName);
    if (aggregation == null) {
      return Collections.emptyMap();
    }
    if (aggregation instanceof ParsedTerms) {
      return extractTermAggregations(
          (ParsedTerms) aggregation, aggregationName.equals("_entityType"));
    } else if (aggregation instanceof ParsedMissing) {
      return Collections.singletonMap(
          aggregation.getName(), ((ParsedMissing) aggregation).getDocCount());
    }
    throw new UnsupportedOperationException(
        "Unsupported aggregation type: " + aggregation.getClass().getName());
  }

  /**
   * Only used in aggregation queries, lazy load
   *
   * @return map of field name to facet display names
   */
  private Map<String, String> getFacetToDisplayNames() {
    if (filtersToDisplayName == null) {
      // Validate field names
      Map<String, Set<Pair<String, Pair<String, String>>>> validateFieldMap =
          entitySearchAnnotations.entrySet().stream()
              .flatMap(
                  entry ->
                      entry.getValue().stream()
                          .flatMap(
                              annotation ->
                                  getFacetFieldDisplayNameFromAnnotation(entry.getKey(), annotation)
                                      .stream()))
              .collect(Collectors.groupingBy(Pair::getFirst, Collectors.toSet()));
      for (Map.Entry<String, Set<Pair<String, Pair<String, String>>>> entry :
          validateFieldMap.entrySet()) {
        if (entry.getValue().stream().map(i -> i.getSecond().getSecond()).distinct().count() > 1) {
          Map<String, Set<Pair<String, String>>> displayNameEntityMap =
              entry.getValue().stream()
                  .map(Pair::getSecond)
                  .collect(Collectors.groupingBy(Pair::getSecond, Collectors.toSet()));
          throw new IllegalStateException(
              String.format(
                  "Facet field collision on field `%s`. Incompatible Display Name across entities. Multiple Display Names detected: %s",
                  entry.getKey(), displayNameEntityMap));
        }
      }

      filtersToDisplayName =
          entitySearchAnnotations.entrySet().stream()
              .flatMap(
                  entry ->
                      entry.getValue().stream()
                          .flatMap(
                              annotation ->
                                  getFacetFieldDisplayNameFromAnnotation(entry.getKey(), annotation)
                                      .stream()))
              .collect(
                  Collectors.toMap(Pair::getFirst, p -> p.getSecond().getSecond(), mapMerger()));
      filtersToDisplayName.put(INDEX_VIRTUAL_FIELD, "Type");
    }

    return filtersToDisplayName;
  }

  private void processMissingAggregations(
      final Map.Entry<String, Aggregation> entry,
      final List<AggregationMetadata> aggregationMetadataList) {
    ParsedMissing parsedMissing = (ParsedMissing) entry.getValue();
    Long docCount = parsedMissing.getDocCount();
    LongMap longMap = new LongMap();
    longMap.put(entry.getKey(), docCount);
    final AggregationMetadata aggregationMetadata =
        new AggregationMetadata()
            .setName(entry.getKey())
            .setDisplayName(computeDisplayName(entry.getKey()))
            .setAggregations(longMap)
            .setFilterValues(
                new FilterValueArray(SearchUtil.convertToFilters(longMap, Collections.emptySet())));
    aggregationMetadataList.add(aggregationMetadata);
  }

  // If values are not equal, throw error
  private BinaryOperator<String> mapMerger() {
    return (s1, s2) -> {
      if (!StringUtils.equals(s1, s2)) {
        throw new IllegalStateException(String.format("Unable to merge values %s and %s", s1, s2));
      }
      return s1;
    };
  }
}

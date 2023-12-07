package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.utils.SearchUtil.*;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;

@Slf4j
public class AggregationQueryBuilder {

  private final SearchConfiguration _configs;
  private final Set<String> _defaultFacetFields;
  private final Set<String> _allFacetFields;

  public AggregationQueryBuilder(
      @Nonnull final SearchConfiguration configs,
      @Nonnull final List<SearchableAnnotation> annotations) {
    this._configs = Objects.requireNonNull(configs, "configs must not be null");
    this._defaultFacetFields = getDefaultFacetFields(annotations);
    this._allFacetFields = getAllFacetFields(annotations);
  }

  /** Get the set of default aggregations, across all facets. */
  public List<AggregationBuilder> getAggregations() {
    return getAggregations(null);
  }

  /**
   * Get aggregations for a search request for the given facets provided, and if none are provided,
   * then get aggregations for all.
   */
  public List<AggregationBuilder> getAggregations(@Nullable List<String> facets) {
    final Set<String> facetsToAggregate;
    if (facets != null) {
      facets.stream()
          .filter(f -> !isValidAggregate(f))
          .forEach(
              facet -> {
                log.warn(
                    String.format(
                        "Requested facet for search filter aggregations that isn't part of the default filters. Provided: %s; Available: %s",
                        facet, _defaultFacetFields));
              });
      facetsToAggregate =
          facets.stream().filter(this::isValidAggregate).collect(Collectors.toSet());
    } else {
      facetsToAggregate = _defaultFacetFields;
    }
    return facetsToAggregate.stream()
        .map(this::facetToAggregationBuilder)
        .collect(Collectors.toList());
  }

  private Set<String> getDefaultFacetFields(final List<SearchableAnnotation> annotations) {
    Set<String> facets =
        annotations.stream()
            .flatMap(annotation -> getDefaultFacetFieldsFromAnnotation(annotation).stream())
            .collect(Collectors.toSet());
    facets.add(INDEX_VIRTUAL_FIELD);
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
    Set<String> facets = Set.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    return facets.size() > 0 && _allFacetFields.containsAll(facets);
  }

  private AggregationBuilder facetToAggregationBuilder(final String inputFacet) {
    List<String> facets = List.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    AggregationBuilder lastAggBuilder = null;
    for (int i = facets.size() - 1; i >= 0; i--) {
      String facet = facets.get(i);
      AggregationBuilder aggBuilder =
          facet.equalsIgnoreCase(INDEX_VIRTUAL_FIELD)
              ? AggregationBuilders.terms(inputFacet)
                  .field(getAggregationField("_index"))
                  .size(_configs.getMaxTermBucketSize())
                  .minDocCount(0)
              : AggregationBuilders.terms(inputFacet)
                  .field(getAggregationField(facet))
                  .size(_configs.getMaxTermBucketSize());
      if (lastAggBuilder != null) {
        aggBuilder = aggBuilder.subAggregation(lastAggBuilder);
      }
      lastAggBuilder = aggBuilder;
    }
    // Logic to build nested facets
    return lastAggBuilder;
  }

  private String getAggregationField(final String facet) {
    if (facet.startsWith("has")) {
      // Boolean hasX field, not a keyword field. Return the name of the original facet.
      return facet;
    }
    // Otherwise assume that this field is of keyword type.
    return ESUtils.toKeywordField(facet, false);
  }

  List<String> getDefaultFacetFieldsFromAnnotation(final SearchableAnnotation annotation) {
    final List<String> facetsFromAnnotation = new ArrayList<>();
    if (annotation.isAddToFilters()) {
      facetsFromAnnotation.add(annotation.getFieldName());
    }
    if (annotation.isAddHasValuesToFilters() && annotation.getHasValuesFieldName().isPresent()) {
      facetsFromAnnotation.add(annotation.getHasValuesFieldName().get());
    }
    return facetsFromAnnotation;
  }

  List<String> getAllFacetFieldsFromAnnotation(final SearchableAnnotation annotation) {
    final List<String> facetsFromAnnotation = new ArrayList<>();
    facetsFromAnnotation.add(annotation.getFieldName());
    if (annotation.getHasValuesFieldName().isPresent()) {
      facetsFromAnnotation.add(annotation.getHasValuesFieldName().get());
    }
    return facetsFromAnnotation;
  }
}

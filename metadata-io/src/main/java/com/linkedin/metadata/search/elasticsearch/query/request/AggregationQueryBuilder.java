package com.linkedin.metadata.search.elasticsearch.query.request;

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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import static com.linkedin.metadata.utils.SearchUtil.*;


@Slf4j
public class AggregationQueryBuilder {

  private final SearchConfiguration _configs;
  private final Set<String> _facetFields;

  public AggregationQueryBuilder(
      @Nonnull final SearchConfiguration configs,
      @Nonnull final List<SearchableAnnotation> annotations) {
    this._configs = Objects.requireNonNull(configs, "configs must not be null");
    this._facetFields = getFacetFields(annotations);
  }

  /**
   * Get the set of default aggregations, across all facets.
   */
  public List<AggregationBuilder> getAggregations() {
    return getAggregations(null);
  }

  /**
   * Get aggregations for a search request for the given facets provided, and if none are provided, then get aggregations for all.
   */
  public List<AggregationBuilder> getAggregations(@Nullable List<String> facets) {
    final Set<String> facetsToAggregate;
    if (facets != null) {
      facets.stream().filter(f -> !isValidAggregate(f)).forEach(facet -> {
        log.warn(String.format("Provided facet for search filter aggregations that doesn't exist. Provided: %s; Available: %s", facet, _facetFields));
      });
      facetsToAggregate = facets.stream().filter(this::isValidAggregate).collect(Collectors.toSet());
    } else {
      facetsToAggregate = _facetFields;
    }
    return facetsToAggregate.stream().map(this::facetToAggregationBuilder).collect(Collectors.toList());
  }


  private Set<String> getFacetFields(final List<SearchableAnnotation> annotations) {
    Set<String> facets = annotations.stream()
        .flatMap(annotation -> getFacetFieldsFromAnnotation(annotation).stream())
        .collect(Collectors.toSet());
    facets.add(INDEX_VIRTUAL_FIELD);
    return facets;
  }

  private boolean isValidAggregate(final String inputFacet) {
    Set<String> facets = Set.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    return facets.size() > 0 && _facetFields.containsAll(facets);
  }

  private AggregationBuilder facetToAggregationBuilder(final String inputFacet) {
    List<String> facets = List.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    AggregationBuilder lastAggBuilder = null;
    for (int i = facets.size() - 1; i >= 0; i--) {
      String facet = facets.get(i);
      if (facet.equalsIgnoreCase(INDEX_VIRTUAL_FIELD)) {
        facet = "_index";
      }
      AggregationBuilder aggBuilder =
          AggregationBuilders.terms(inputFacet)
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

  List<String> getFacetFieldsFromAnnotation(final SearchableAnnotation annotation) {
    final List<String> facetsFromAnnotation = new ArrayList<>();
    if (annotation.isAddToFilters()) {
      facetsFromAnnotation.add(annotation.getFieldName());
    }
    if (annotation.isAddHasValuesToFilters() && annotation.getHasValuesFieldName().isPresent()) {
      facetsFromAnnotation.add(annotation.getHasValuesFieldName().get());
    }
    return facetsFromAnnotation;
  }
}

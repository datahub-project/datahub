package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import static com.linkedin.metadata.utils.SearchUtil.*;


@Slf4j
public class AggregationQueryBuilder {
  private final SearchConfiguration _configs;
  private final List<SearchableAnnotation> _annotations;
  private final Set<String> _facetFields;
  public AggregationQueryBuilder(SearchConfiguration configs, List<SearchableAnnotation> annotations) {
    this._configs = configs;
    this._annotations = annotations;
    this._facetFields = getFacetFields(annotations);
  }

  private Set<String> getFacetFields(List<SearchableAnnotation> annotations) {
    Set<String> facets = annotations.stream()
        .filter(SearchableAnnotation::isAddToFilters)
        .map(SearchableAnnotation::getFieldName)
        .collect(Collectors.toSet());
    facets.add(INDEX_VIRTUAL_FIELD);
    return facets;
  }

  public List<AggregationBuilder> getAggregations() {
    return getAggregations(null);
  }

  private boolean isValidAggregate(String inputFacet) {
    Set<String> facets = Set.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    return facets.size() > 0 && _facetFields.containsAll(facets);
  }

  private AggregationBuilder facetToAggregationBuilder(String inputFacet) {
    List<String> facets = List.of(inputFacet.split(AGGREGATION_SEPARATOR_CHAR));
    AggregationBuilder lastAggBuilder = null;
    for (int i = facets.size() - 1; i >= 0; i--) {
      String facet = facets.get(i);
      if (facet.equalsIgnoreCase(INDEX_VIRTUAL_FIELD)) {
        facet = "_index";
      }
      AggregationBuilder aggBuilder =
          AggregationBuilders.terms(inputFacet).field(ESUtils.toKeywordField(facet, false)).size(_configs.getMaxTermBucketSize());
      if (lastAggBuilder != null) {
        aggBuilder = aggBuilder.subAggregation(lastAggBuilder);
      }
      lastAggBuilder = aggBuilder;
    }
    // Logic to build nested facets
    return lastAggBuilder;
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
}

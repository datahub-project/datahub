package com.linkedin.metadata.search.query.builder;

import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;


public class FacetsAggregationBuilder {

  private final EntitySpec _entitySpec;
  private final Set<String> _facetFields;
  private final int _maxTermBucketSize = 100;

  public FacetsAggregationBuilder(@Nonnull EntitySpec entitySpec) {
    _entitySpec = entitySpec;
    _facetFields = getFacetFields();
  }

  private static final Map<String, FacetsAggregationBuilder> FACETS_AGGREGATION_BUILDER_BY_ENTITY_NAME =
      new HashMap<>();

  public static FacetsAggregationBuilder getBuilder(@Nonnull String entityName) {
    EntitySpec entitySpec = SnapshotEntityRegistry.getInstance().getEntitySpec(entityName);
    return FACETS_AGGREGATION_BUILDER_BY_ENTITY_NAME.computeIfAbsent(entityName,
        k -> new FacetsAggregationBuilder(entitySpec));
  }

  public List<AggregationBuilder> getAggregations(@Nullable Filter filter) {
    List<AggregationBuilder> aggregationBuilders = new ArrayList<>();
    for (String facet : _facetFields) {
      AggregationBuilder aggBuilder = AggregationBuilders.terms(facet).field(facet).size(_maxTermBucketSize);
      Optional.ofNullable(filter).map(Filter::getCriteria).ifPresent(criteria -> {
        for (Criterion criterion : criteria) {
          if (!_facetFields.contains(criterion.getField()) || criterion.getField().equals(facet)) {
            continue;
          }
          QueryBuilder filterQueryBuilder = ESUtils.getQueryBuilderFromCriterionForSearch(criterion);
          aggBuilder.subAggregation(AggregationBuilders.filter(criterion.getField(), filterQueryBuilder));
        }
      });
      aggregationBuilders.add(aggBuilder);
    }
    return aggregationBuilders;
  }

  public Set<String> getFacetFields() {
    return _entitySpec.getSearchableFieldSpecs()
        .stream()
        .filter(SearchableFieldSpec::addToFilters)
        .map(SearchableFieldSpec::getFieldName)
        .collect(Collectors.toSet());
  }

  public List<AggregationMetadata> extractAggregation(@Nonnull SearchResponse searchResponse) {
    final List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();

    if (searchResponse.getAggregations() == null) {
      return aggregationMetadataList;
    }

    for (Map.Entry<String, Aggregation> entry : searchResponse.getAggregations().getAsMap().entrySet()) {
      final Map<String, Long> oneTermAggResult = extractTermAggregations((ParsedTerms) entry.getValue());
      final AggregationMetadata aggregationMetadata =
          new AggregationMetadata().setName(entry.getKey()).setAggregations(new LongMap(oneTermAggResult));
      aggregationMetadataList.add(aggregationMetadata);
    }

    return aggregationMetadataList;
  }

  /**
   * Extracts term aggregations give a parsed term.
   *
   * @param terms an abstract parse term, input can be either ParsedStringTerms ParsedLongTerms
   * @return a map with aggregation key and corresponding doc counts
   */
  @Nonnull
  private Map<String, Long> extractTermAggregations(@Nonnull ParsedTerms terms) {

    final Map<String, Long> aggResult = new HashMap<>();
    List<? extends Terms.Bucket> bucketList = terms.getBuckets();

    for (Terms.Bucket bucket : bucketList) {
      String key = bucket.getKeyAsString();
      ParsedFilter parsedFilter = extractBucketAggregations(bucket);
      // Gets filtered sub aggregation doc count if exist
      Long docCount = parsedFilter != null ? parsedFilter.getDocCount() : bucket.getDocCount();
      if (docCount > 0) {
        aggResult.put(key, docCount);
      }
    }

    return aggResult;
  }

  /**
   * Extracts sub aggregations from one term bucket.
   *
   * @param bucket a term bucket
   * @return a parsed filter if exist
   */
  @Nullable
  private ParsedFilter extractBucketAggregations(@Nonnull Terms.Bucket bucket) {

    ParsedFilter parsedFilter = null;
    Map<String, Aggregation> bucketAggregations = bucket.getAggregations().getAsMap();
    for (Map.Entry<String, Aggregation> entry : bucketAggregations.entrySet()) {
      parsedFilter = (ParsedFilter) entry.getValue();
      // TODO: implement and test multi parsed filters
    }

    return parsedFilter;
  }
}

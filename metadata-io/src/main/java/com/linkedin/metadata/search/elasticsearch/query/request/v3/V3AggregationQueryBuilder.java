package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

/**
 * Search V3 facets. A v3 index holds several entity types, so the entity-type aggregation reads the
 * stored {@code _entityType} field and reports only the requested types.
 */
public class V3AggregationQueryBuilder extends AggregationQueryBuilder {

  public V3AggregationQueryBuilder(
      @Nonnull final SearchConfiguration configs,
      @Nonnull Map<EntitySpec, List<SearchableAnnotation>> entitySearchAnnotations) {
    this(configs, entitySearchAnnotations, null);
  }

  public V3AggregationQueryBuilder(
      @Nonnull final SearchConfiguration configs,
      @Nonnull Map<EntitySpec, List<SearchableAnnotation>> entitySearchAnnotations,
      @Nullable SearchFieldResolver v3FieldResolver) {
    super(configs, entitySearchAnnotations, v3FieldResolver);
  }

  @Override
  protected String entityTypeField() {
    return INDEX_VIRTUAL_FIELD;
  }

  @Override
  protected TermsAggregationBuilder entityTypeAggregation(
      @Nonnull OperationContext opContext, @Nonnull String name, int maxTermBuckets) {
    // Report only the requested types, including those without hits, like the per-index buckets
    // on V2.
    return super.entityTypeAggregation(opContext, name, maxTermBuckets)
        .includeExclude(
            new IncludeExclude(
                entitySearchAnnotations.keySet().stream()
                    .map(EntitySpec::getName)
                    .toArray(String[]::new),
                null));
  }
}

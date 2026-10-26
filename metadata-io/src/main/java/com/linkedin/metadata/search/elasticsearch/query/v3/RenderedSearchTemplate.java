package com.linkedin.metadata.search.elasticsearch.query.v3;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilder;

/** Immutable output of rendering one Search V3 template. */
public record RenderedSearchTemplate(
    @Nonnull QueryBuilder query,
    @Nullable QueryRescorerBuilder rescore,
    @Nullable List<AggregationBuilder> aggregations,
    @Nullable List<SortBuilder<?>> sorts,
    @Nonnull List<String> highlights,
    @Nonnull List<String> queriedFields,
    @Nonnull String originalQuery,
    @Nonnull Map<String, NamedClause> namedClauses,
    int estimatedClauses) {

  public RenderedSearchTemplate {
    highlights = List.copyOf(highlights);
    queriedFields = List.copyOf(queriedFields);
    namedClauses = Map.copyOf(namedClauses);
    aggregations = aggregations == null ? null : List.copyOf(aggregations);
    sorts = sorts == null ? null : List.copyOf(sorts);
  }

  public record NamedClause(
      @Nonnull String urn, @Nonnull String path, @Nonnull String subfield, boolean analyzed) {}
}

package com.linkedin.metadata.search.elasticsearch.query.v3;

import com.linkedin.search.template.SearchTemplateSearchType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilder;

/**
 * Immutable output of rendering one Search V3 template. {@code searchType} is the resolved template
 * override. Null means the template request uses query-then-fetch.
 */
public record RenderedSearchTemplate(
    @Nonnull QueryBuilder query,
    @Nullable QueryRescorerBuilder rescore,
    @Nullable List<AggregationBuilder> aggregations,
    @Nullable List<SortBuilder<?>> sorts,
    @Nonnull List<String> highlights,
    boolean matchedFields,
    @Nonnull List<String> queriedFields,
    @Nonnull String originalQuery,
    @Nonnull Map<String, NamedClause> namedClauses,
    int estimatedClauses,
    @Nonnull Map<String, List<String>> queriedFieldSources,
    @Nullable SearchTemplateSearchType searchType) {

  public RenderedSearchTemplate {
    highlights = List.copyOf(highlights);
    queriedFields = List.copyOf(queriedFields);
    namedClauses = Map.copyOf(namedClauses);
    Map<String, List<String>> sources = new LinkedHashMap<>();
    queriedFieldSources.forEach((label, paths) -> sources.put(label, List.copyOf(paths)));
    queriedFieldSources = Map.copyOf(sources);
    aggregations = aggregations == null ? null : List.copyOf(aggregations);
    sorts = sorts == null ? null : List.copyOf(sorts);
  }

  public record NamedClause(
      @Nonnull String urn, @Nonnull String path, @Nonnull String subfield, boolean analyzed) {}
}

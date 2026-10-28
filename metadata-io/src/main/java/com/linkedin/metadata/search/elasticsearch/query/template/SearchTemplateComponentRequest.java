package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.search.template.SearchTemplateComponentMode;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * One component invocation. {@code hits} is empty when {@code plan} runs, before the engine call.
 */
@Value
@Builder
public class SearchTemplateComponentRequest {
  @Nullable OperationContext opContext;
  @Nonnull String query;
  @Nullable String renderedQuery;
  @Nullable Filter filter;
  @Nullable List<SortCriterion> sort;
  int from;
  @Nullable Integer size;
  boolean scroll;

  @Builder.Default @Nonnull List<SearchEntity> hits = List.of();

  long total;
  @Nullable SearchResultMetadata metadata;

  @Builder.Default @Nonnull List<SearchEntity> parallelHits = List.of();

  @Nonnull Map<String, String> config;
  @Nonnull SearchTemplateComponentMode mode;
}

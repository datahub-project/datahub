package com.linkedin.metadata.search.elasticsearch.query;

import com.linkedin.metadata.search.elasticsearch.query.TemplateSearchExecutor.PreparedRequestComponents;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCandidates;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Companion query merged into a template rung's candidate window. OSS registers none, so the merge
 * is a no-op. An extension registers one when a second query should fill the same window.
 */
interface TemplateResultSupplement {
  @Nonnull
  SearchTemplateCandidates.SupplementaryMerge offer(
      @Nonnull OperationContext opContext,
      @Nonnull ESSearchDAO dao,
      @Nonnull PreparedRequestComponents prepared,
      @Nullable String normalizedQuery,
      int mergeWindow);

  final class None implements TemplateResultSupplement {
    @Override
    @Nonnull
    public SearchTemplateCandidates.SupplementaryMerge offer(
        @Nonnull OperationContext opContext,
        @Nonnull ESSearchDAO dao,
        @Nonnull PreparedRequestComponents prepared,
        @Nullable String normalizedQuery,
        int mergeWindow) {
      return SearchTemplateCandidates.SupplementaryMerge.NONE;
    }
  }
}

package com.linkedin.metadata.search.elasticsearch.query.template;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchTemplateRuntime;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Reads the template coordinator carried on an operation's search context. */
public final class SearchTemplateRuntimes {
  private SearchTemplateRuntimes() {}

  @Nullable
  public static SearchTemplateCoordinator coordinator(@Nonnull OperationContext opContext) {
    SearchTemplateRuntime runtime = opContext.getSearchContext().getSearchTemplateRuntime();
    return runtime instanceof SearchTemplateCoordinator coordinator ? coordinator : null;
  }

  /** Binds the template for this query. A missing runtime leaves the context unchanged. */
  @Nonnull
  public static OperationContext bind(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean scroll,
      @Nullable List<String> entityNames) {
    SearchTemplateCoordinator coordinator = coordinator(opContext);
    if (coordinator == null) {
      return opContext;
    }
    return coordinator.bind(opContext, query, scroll, entityNames);
  }
}

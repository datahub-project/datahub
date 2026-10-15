package com.linkedin.metadata.search.elasticsearch.query.template;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Process-wide template coordinator. Absent means templates are off. */
public final class SearchTemplates {
  @Nullable private static volatile SearchTemplateCoordinator current;

  private SearchTemplates() {}

  public static void install(@Nullable SearchTemplateCoordinator coordinator) {
    current = coordinator;
  }

  @Nullable
  public static SearchTemplateCoordinator current() {
    return current;
  }

  @Nonnull
  public static OperationContext bind(@Nonnull OperationContext opContext, @Nonnull String query) {
    return bind(opContext, query, false);
  }

  @Nonnull
  public static OperationContext bind(
      @Nonnull OperationContext opContext, @Nonnull String query, boolean scroll) {
    return bind(opContext, query, scroll, null);
  }

  @Nonnull
  public static OperationContext bind(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean scroll,
      @Nullable List<String> entityNames) {
    SearchTemplateCoordinator coordinator = current;
    if (coordinator == null) {
      return opContext;
    }
    return coordinator.bind(opContext, query, scroll, entityNames);
  }
}

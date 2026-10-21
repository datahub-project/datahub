package com.linkedin.metadata.search.elasticsearch.query.template;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Template coordinator access. Absent means templates are off. */
public final class SearchTemplates {
  @Nullable private static volatile SearchTemplateCoordinator processWide;
  private static final ThreadLocal<SearchTemplateCoordinator> TEST_OVERRIDE = new ThreadLocal<>();

  private SearchTemplates() {}

  /** Installs the application coordinator. Production wiring owns this process-wide lifecycle. */
  public static void installProcessWide(@Nullable SearchTemplateCoordinator coordinator) {
    processWide = coordinator;
  }

  /**
   * Installs a coordinator for the current test thread.
   *
   * <p>Search suites execute classes concurrently. Keeping their overrides thread-scoped prevents
   * one fixture from enabling templates in an unrelated Java-fallback test.
   */
  public static void install(@Nullable SearchTemplateCoordinator coordinator) {
    if (coordinator == null) {
      TEST_OVERRIDE.remove();
    } else {
      TEST_OVERRIDE.set(coordinator);
    }
  }

  @Nullable
  public static SearchTemplateCoordinator current() {
    SearchTemplateCoordinator override = TEST_OVERRIDE.get();
    return override == null ? processWide : override;
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
    SearchTemplateCoordinator coordinator = current();
    if (coordinator == null) {
      return opContext;
    }
    return coordinator.bind(opContext, query, scroll, entityNames);
  }
}

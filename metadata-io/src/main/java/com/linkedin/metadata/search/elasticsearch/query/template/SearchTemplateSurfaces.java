package com.linkedin.metadata.search.elasticsearch.query.template;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Search V3 template surfaces. A surface selects a seed; it is not an API operation name. */
public final class SearchTemplateSurfaces {
  public static final String SEARCH_ENTITIES = "searchEntities";
  public static final String SCROLL_ENTITIES = "scrollEntities";
  public static final String AGGREGATE_ENTITIES = "aggregateEntities";

  /**
   * Semantic search ({@code semanticSearchAcrossEntities} and {@code semanticSearch}) through a
   * template. Only a vector rung may serve it. Its rollout is off unless set: a blank setting does
   * not inherit the global mode.
   */
  public static final String SEMANTIC_SEARCH = "semanticSearch";

  private SearchTemplateSurfaces() {}

  /**
   * Page search and scroll fill a missing surface. Aggregate sets its surface earlier and keeps it.
   * Browse clears an incoming surface so it uses the global seed.
   */
  @Nonnull
  public static OperationContext withDefault(
      @Nonnull OperationContext opContext, @Nonnull String surface) {
    String current = opContext.getSearchContext().getSearchSurface();
    if (current != null && !current.isBlank()) {
      return opContext;
    }
    return opContext.withSearchSurface(surface);
  }

  /** The request runs on the semantic search surface. */
  public static boolean semantic(@Nonnull OperationContext opContext) {
    return SEMANTIC_SEARCH.equals(opContext.getSearchContext().getSearchSurface());
  }
}

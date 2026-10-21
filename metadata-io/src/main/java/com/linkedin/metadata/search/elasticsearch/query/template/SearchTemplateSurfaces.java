package com.linkedin.metadata.search.elasticsearch.query.template;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Search V3 template surfaces. A surface selects a seed; it is not an API operation name. */
public final class SearchTemplateSurfaces {
  public static final String SEARCH_ENTITIES = "searchEntities";
  public static final String SCROLL_ENTITIES = "scrollEntities";
  public static final String AGGREGATE_ENTITIES = "aggregateEntities";
  public static final String BROWSE_ENTITIES = "browseEntities";
  public static final String SEARCH_ENTITIES_LINEAGE = "searchEntitiesLineage";
  public static final String SCROLL_ENTITIES_LINEAGE = "scrollEntitiesLineage";

  private SearchTemplateSurfaces() {}

  /**
   * Page search and scroll fill a missing surface. Browse, aggregate, and lineage set theirs
   * earlier and keep it.
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
}

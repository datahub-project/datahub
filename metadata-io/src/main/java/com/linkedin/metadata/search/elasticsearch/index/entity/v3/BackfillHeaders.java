package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

/**
 * Change-log headers a primary-storage backfill stamps on the events it writes, so an entity index
 * writer can tell a backfill from a live write and learn what the batch it received represents.
 * Live writes carry none of them.
 */
public final class BackfillHeaders {
  /** {@link #MODE_BASELINE} or {@link #MODE_CATCH_UP}; absent on live writes. */
  public static final String MODE_HEADER_NAME = "DataHub-Search-V3-Backfill-Mode";

  /**
   * Index a baseline fill writes into instead of the entity index's alias. Set when a fill rebuilds
   * an index into a next physical index that the alias only points at once the fill completes.
   * Ignored on anything but baseline backfill events.
   */
  public static final String TARGET_INDEX_HEADER_NAME = "DataHub-Search-V3-Backfill-Target-Index";

  /**
   * Set to {@code true} on a baseline fill event whose batch carries every stored aspect of the
   * entity, because the loader read the entity's rows whole, with no aspect or time filter. A
   * writer may then treat the batch as the entity's stored state instead of reading it again from
   * primary storage. Ignored on anything but baseline backfill events.
   */
  public static final String COMPLETE_ENTITY_HEADER_NAME =
      "DataHub-Search-V3-Backfill-Complete-Entity";

  /** An initial population of an index from primary storage. */
  public static final String MODE_BASELINE = "baseline";

  /** A replay of rows changed during a window, written over whatever the index already holds. */
  public static final String MODE_CATCH_UP = "catchup";

  private BackfillHeaders() {}
}

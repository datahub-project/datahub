package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * One logical index of a family on the source cluster, resolved to the indices that hold its
 * documents.
 *
 * @param logicalName the alias, data stream or concrete name the family writes to
 * @param layout how the documents are spread
 * @param backingIndices every index holding documents, oldest first for rollover layouts
 * @param writeIndex the index new documents go to; for a concrete layout the logical name itself
 */
public record SourceIndexSet(
    @Nonnull String logicalName,
    @Nonnull IndexLayout layout,
    @Nonnull List<String> backingIndices,
    @Nonnull String writeIndex) {

  public boolean isRollover() {
    return layout != IndexLayout.CONCRETE;
  }
}

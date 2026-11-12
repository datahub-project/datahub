package com.linkedin.gms.factory.search;

/**
 * Marks an upgrade process that builds search indices and serves no reads.
 *
 * <p>Defined only by system update, restore, and LoadIndices. GMS and the consumers do not have
 * this bean, so a V3 index that cannot be read after the cutover still stops them.
 */
public final class SearchV3IndexRebuild {

  public SearchV3IndexRebuild() {}
}

package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Documents soft-deleted by one {@link DocumentService#deleteDocument} call. {@link #urns()}
 * includes the root, deepest first. {@link #descendantCount()} does not.
 */
public record DocumentDeleteResult(@Nonnull List<Urn> urns, int descendantCount) {

  /** Live descendants besides the root. The next descendant is refused before any write. */
  public static final int MAX_DESCENDANTS = 10_000;

  /**
   * Hops from the root. The root is depth 0. A document at this depth is deleted; its child is not.
   */
  public static final int MAX_DEPTH = 100;

  /** Page size for the descendant and orphan scrolls. */
  public static final int SCROLL_PAGE_SIZE = 1_000;

  /**
   * Point-in-time keep-alive for the descendant scroll. Rest.li {@code scrollAcrossEntities}
   * requires this parameter, so a null value fails the upgrade job's remote client.
   */
  public static final String SCROLL_KEEP_ALIVE = "5m";

  public DocumentDeleteResult {
    urns = Objects.requireNonNull(List.copyOf(urns));
  }
}

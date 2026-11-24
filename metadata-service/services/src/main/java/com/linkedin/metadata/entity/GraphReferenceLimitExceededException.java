package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * A hard delete was refused because more graph edges point at the entity than {@code
 * hardDelete.graph-reference-cleanup-limit} lets one request remove. Nothing was removed. It is an
 * {@link IllegalArgumentException} so the GraphQL exception handler reports its message as a bad
 * request rather than a server error.
 */
public class GraphReferenceLimitExceededException extends IllegalArgumentException {
  public GraphReferenceLimitExceededException(
      @Nonnull final Urn urn, final int referrers, final int limit) {
    super(
        String.format(
            "Cannot delete %s: %d entities reference it, which is over the limit of %d that one"
                + " delete removes. Nothing was deleted. Raise"
                + " DATAHUB_HARD_DELETE_GRAPH_REFERENCE_CLEANUP_LIMIT (hardDelete"
                + ".graph-reference-cleanup-limit), or remove the references first with"
                + " `datahub delete references --urn %s`, then delete again.",
            urn, referrers, limit, urn));
  }
}

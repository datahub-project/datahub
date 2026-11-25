package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * A hard delete was refused because more graph edges point at the entity than {@code
 * hardDelete.graph-reference-cleanup-limit} lets one request remove, and {@code
 * hardDelete.graph-reference-over-limit} is {@code REFUSE}. Nothing was removed. It is an {@link
 * IllegalArgumentException} so the GraphQL exception handler reports its message as a bad request
 * rather than a server error.
 *
 * <p>The message tells the caller to ask an administrator, and says what the administrator can do
 * without naming a mechanism one deployment lacks: raising the limit, or removing the references
 * with one {@code datahub delete references} request, walks every referrer in one request, which is
 * what the limit prevents.
 */
public class GraphReferenceLimitExceededException extends IllegalArgumentException {
  public GraphReferenceLimitExceededException(
      @Nonnull final Urn urn, final int relationships, final int limit) {
    super(
        String.format(
            "Cannot delete %s: %d relationships point at it, which is over the limit of %d that one"
                + " delete removes, and this deployment refuses deletes over the limit. Nothing was"
                + " deleted. Ask your administrator to remove the references in smaller batches or"
                + " to delete it outside this request.",
            urn, relationships, limit));
  }
}

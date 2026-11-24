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
 * <p>The message names no remedy a caller could try: raising the limit, or removing the references
 * with one {@code datahub delete references} request, walks every referrer in one request, which is
 * what the limit prevents.
 */
public class GraphReferenceLimitExceededException extends IllegalArgumentException {
  public GraphReferenceLimitExceededException(
      @Nonnull final Urn urn, final int referrers, final int limit) {
    super(
        String.format(
            "Cannot delete %s: %d entities reference it, which is over the limit of %d that one"
                + " delete removes, and this deployment refuses deletes over the limit. Nothing was"
                + " deleted. Ask your administrator: a delete this large needs the delete"
                + " workflow.",
            urn, referrers, limit));
  }
}

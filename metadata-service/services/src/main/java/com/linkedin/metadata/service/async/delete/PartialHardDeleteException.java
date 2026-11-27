package com.linkedin.metadata.service.async.delete;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * A bounded hard delete removed the aspects captured when the request arrived, but the entity was
 * written to meanwhile and still exists. Retrying inside the same request would re-delete against
 * the same captured versions and fail the same way, so callers surface it and a new delete request
 * (which captures the versions again) finishes the job.
 */
public class PartialHardDeleteException extends IllegalStateException {
  public PartialHardDeleteException(@Nonnull final Urn urn) {
    super(
        String.format(
            "Hard delete of %s did not complete: it was written to while being deleted; try the"
                + " delete again",
            urn));
  }
}

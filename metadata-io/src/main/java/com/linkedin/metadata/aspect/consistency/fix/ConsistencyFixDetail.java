package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/**
 * Details about a single fix operation.
 *
 * <p>Contains the result of applying a fix to an entity, including success/failure status and any
 * error message.
 */
@Data
@Builder
public class ConsistencyFixDetail {

  /** URN of the entity that was fixed (or attempted to fix) */
  @Nonnull private final Urn urn;

  /** Action that was taken (or would be taken in dry-run) */
  @Nonnull private final ConsistencyFixType action;

  /** Whether the fix was successful */
  private final boolean success;

  /** Error message if the fix failed */
  @Nullable private final String errorMessage;

  /** Additional details about the fix */
  @Nullable private final String details;
}

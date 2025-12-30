package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;

/**
 * Represents a request to delete an oversized aspect through EntityService.
 *
 * <p>Used by aspect size validation to collect deletion requests during transaction processing and
 * execute them through proper EntityService flow after transaction commits. This ensures all side
 * effects are handled: database deletion, Elasticsearch index updates, graph edge cleanup, and
 * consumer hook invocation.
 */
@Data
@Builder
public class AspectDeletionRequest {
  /** URN of the entity whose aspect needs deletion */
  @Nonnull private final Urn urn;

  /** Name of the aspect to delete */
  @Nonnull private final String aspectName;

  /** Validation point where oversized aspect was detected */
  @Nonnull private final ValidationPoint validationPoint;

  /** Actual size of the oversized aspect in bytes */
  private final long aspectSize;

  /** Configured size threshold in bytes */
  private final long threshold;
}

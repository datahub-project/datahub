package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * One system-metadata page, after SQL entities have been loaded, for {@link
 * ConsistencyCheck#followUp}.
 */
@Value
@Builder
public class CheckFollowUpRequest {

  @Nonnull OperationContext operationContext;

  @Nonnull String entityType;

  @Builder.Default int batchSize = 100;

  /** Caller-requested URNs. Null or empty means the batch was not URN-filtered. */
  @Nullable Set<Urn> urnFilter;

  /** URNs on this system-metadata page. Empty when the SQL scroll returned no readable URNs. */
  @Nonnull @Builder.Default Set<Urn> pageUrns = Set.of();

  @Nonnull @Builder.Default Map<Urn, EntityResponse> entityResponses = Map.of();

  /**
   * Page URNs that are absent from SQL. The service computes this for every batch. A check decides
   * whether it is an issue.
   */
  @Nonnull @Builder.Default Set<Urn> orphanUrns = Set.of();

  /** True when the system-metadata scroll cannot continue. */
  boolean sqlExhausted;

  @Nullable CheckContext context;
}

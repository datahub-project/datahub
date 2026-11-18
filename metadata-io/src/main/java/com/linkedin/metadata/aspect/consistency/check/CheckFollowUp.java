package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Issues and pagination a check adds after {@link ConsistencyCheck#check}.
 *
 * <p>{@code scrollId} continues a phase that check owns. The service returns it only after the
 * system-metadata scroll is exhausted. A SQL scroll id always wins while that scroll can continue.
 */
@Value
@Builder
public class CheckFollowUp {

  @Nonnull @Builder.Default List<ConsistencyIssue> issues = List.of();

  @Builder.Default int documentsScanned = 0;

  @Nullable String scrollId;

  @Nonnull
  public static CheckFollowUp none() {
    return CheckFollowUp.builder().build();
  }
}

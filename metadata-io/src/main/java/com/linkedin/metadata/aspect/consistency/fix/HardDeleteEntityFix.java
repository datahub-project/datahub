package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Fix implementation that hard-deletes entities (permanently removes from storage).
 *
 * <p>Uses the following fields from Issue:
 *
 * <ul>
 *   <li>hardDeleteUrns - List of URNs to hard delete (if provided)
 *   <li>entityUrn - Falls back to single entity URN if hardDeleteUrns is empty
 * </ul>
 *
 * <p>Use this fix when the entity's associated data has been hard-deleted, so matching the deletion
 * type is appropriate.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HardDeleteEntityFix implements ConsistencyFix {

  private final EntityService<?> entityService;

  @Override
  @Nonnull
  public ConsistencyFixType getType() {
    return ConsistencyFixType.HARD_DELETE;
  }

  @Override
  @Nonnull
  public ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
    Urn primaryUrn = issue.getEntityUrn();

    // Get URNs to delete - use hardDeleteUrns if provided, otherwise fall back to entityUrn
    List<Urn> urnsToDelete =
        (issue.getHardDeleteUrns() != null && !issue.getHardDeleteUrns().isEmpty())
            ? issue.getHardDeleteUrns()
            : List.of(primaryUrn);

    List<String> deletedUrns = new ArrayList<>();
    List<String> failedUrns = new ArrayList<>();

    // TODO: Replace with batch delete interface when available in EntityService
    // Currently EntityService only exposes deleteUrn(opContext, urn) for single URNs
    for (Urn urnToDelete : urnsToDelete) {
      try {
        if (!dryRun) {
          entityService.deleteUrn(opContext, urnToDelete);
        }
        deletedUrns.add(urnToDelete.toString());
        log.info("[{}] HARD_DELETE entity={}", dryRun ? "DRY-RUN" : "APPLY", urnToDelete);
      } catch (Exception e) {
        failedUrns.add(urnToDelete.toString());
        log.error(
            "Failed to hard-delete {}: {}. Continuing with remaining entities.",
            urnToDelete,
            e.getMessage(),
            e);
      }
    }

    // Build result based on success/failure counts
    if (failedUrns.isEmpty()) {
      // Complete success
      String details =
          deletedUrns.size() == 1
              ? "Hard deleted " + deletedUrns.get(0)
              : String.format(
                  "Hard deleted %d entities: %s",
                  deletedUrns.size(), String.join(", ", deletedUrns));
      return ConsistencyFixDetail.builder()
          .urn(primaryUrn)
          .action(ConsistencyFixType.HARD_DELETE)
          .success(true)
          .details(details)
          .build();
    } else if (deletedUrns.isEmpty()) {
      // Complete failure
      return ConsistencyFixDetail.builder()
          .urn(primaryUrn)
          .action(ConsistencyFixType.HARD_DELETE)
          .success(false)
          .errorMessage(
              String.format(
                  "Failed to hard-delete %d entities: %s",
                  failedUrns.size(), String.join(", ", failedUrns)))
          .build();
    } else {
      // Partial success
      return ConsistencyFixDetail.builder()
          .urn(primaryUrn)
          .action(ConsistencyFixType.HARD_DELETE)
          .success(false)
          .details(
              String.format(
                  "Partial success: Hard deleted %d entities: %s",
                  deletedUrns.size(), String.join(", ", deletedUrns)))
          .errorMessage(
              String.format(
                  "Failed to hard-delete %d entities: %s",
                  failedUrns.size(), String.join(", ", failedUrns)))
          .build();
    }
  }
}

package com.linkedin.metadata.aspect.consistency;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/**
 * Represents a consistency issue found during checking.
 *
 * <p>An issue contains:
 *
 * <ul>
 *   <li>Information about the affected entity
 *   <li>The check that found the issue
 *   <li>The recommended fix type
 *   <li>Batch items or URN lists for executing fixes
 * </ul>
 *
 * <p>For MCP-based fixes (CREATE, UPSERT, PATCH, SOFT_DELETE, DELETE_ASPECT), the issue carries
 * BatchItems:
 *
 * <ul>
 *   <li>CREATE/UPSERT: ChangeItemImpl entries
 *   <li>PATCH: PatchItemImpl entries
 *   <li>DELETE_ASPECT: DeleteItemImpl entries
 *   <li>SOFT_DELETE: ChangeItemImpl setting status.removed=true
 * </ul>
 *
 * <p>For HARD_DELETE, the issue carries URNs to delete via entityService.deleteUrn().
 */
@Data
@Builder
public class ConsistencyIssue {

  /** URN of the entity with the issue */
  @Nonnull private final Urn entityUrn;

  /** Entity type (e.g., "assertion", "monitor") */
  @Nonnull private final String entityType;

  /** ID of the check that found this issue */
  @Nonnull private final String checkId;

  /** Type of fix to apply */
  @Nonnull private final ConsistencyFixType fixType;

  /** Human-readable description of the issue */
  @Nonnull private final String description;

  /** Related entity URNs (e.g., missing monitors, referenced entities) */
  @Nullable private final List<Urn> relatedUrns;

  /** Additional details about the issue */
  @Nullable private final String details;

  // Fields for executing fixes

  /**
   * Batch items for MCP-based operations (CREATE/UPSERT/PATCH/SOFT_DELETE/DELETE_ASPECT).
   *
   * <p>Contains:
   *
   * <ul>
   *   <li>ChangeItemImpl for UPSERT/CREATE (soft delete = status.removed=true)
   *   <li>PatchItemImpl for PATCH
   *   <li>DeleteItemImpl for DELETE_ASPECT
   * </ul>
   *
   * <p>The fix executor will build an AspectsBatch from these items at execution time.
   */
  @Nullable private final List<BatchItem> batchItems;

  /**
   * URNs for HARD_DELETE operations.
   *
   * <p>These are processed via entityService.deleteUrn() which is not MCP-based.
   */
  @Nullable private final List<Urn> hardDeleteUrns;

  /**
   * Additional metadata for the fix.
   *
   * <p>Check-specific configuration or context that the fix implementation may need. For example,
   * the AssertionMonitorMissing check may include cron schedule configuration for creating
   * monitors. Values are strings; callers should coerce types as needed.
   */
  @Nullable private final Map<String, String> metadata;

  /**
   * Create an Issue builder pre-populated with common fields.
   *
   * @param entityUrn the entity URN
   * @param entityType the entity type
   * @param checkId the check ID
   * @param fixType the fix type
   * @return issue builder
   */
  public static ConsistencyIssueBuilder forEntity(
      @Nonnull Urn entityUrn,
      @Nonnull String entityType,
      @Nonnull String checkId,
      @Nonnull ConsistencyFixType fixType) {
    return ConsistencyIssue.builder()
        .entityUrn(entityUrn)
        .entityType(entityType)
        .checkId(checkId)
        .fixType(fixType);
  }
}

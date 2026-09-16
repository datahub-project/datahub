package datahub.client.v2.patch;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.exceptions.VersionConflictInfo;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Result of transforming a patch MCP, containing both the transformed MCP and an optional retry
 * function.
 *
 * <p>The retry function is a closure that can re-perform the transformation with fresh data when a
 * version conflict is detected. If null, the MCP cannot be retried (e.g., it's already a full
 * upsert and doesn't use read-modify-write).
 *
 * <p>Example usage:
 *
 * <pre>
 * TransformResult result = new TransformResult(
 *     transformedMcp,
 *     (conflict) -> {
 *         // Re-read with fresh version and re-apply patch
 *         return transformPatchToFullAspect(originalPatch, entityClient);
 *     }
 * );
 * </pre>
 */
@Value
public class TransformResult {
  /** The transformed MetadataChangeProposal. */
  @Nonnull MetadataChangeProposal mcp;

  /**
   * Optional retry function that can re-transform the patch when a version conflict occurs.
   *
   * <p>If null, the MCP cannot be retried and version conflicts will fail immediately.
   */
  @Nullable Function<VersionConflictInfo, MetadataChangeProposal> retryFunction;

  /**
   * Returns true if this result has a retry function (i.e., can be retried on version conflict).
   *
   * @return true if retryable, false otherwise
   */
  public boolean hasRetryFunction() {
    return retryFunction != null;
  }
}

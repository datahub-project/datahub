package com.linkedin.metadata.aspect;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Validator invoked at two checkpoints during aspect processing:
 *
 * <ol>
 *   <li><b>Pre-patch validation:</b> Called when loading existing aspect from database before patch
 *       application
 *   <li><b>Post-patch validation:</b> Called after aspect serialization but before database write
 * </ol>
 *
 * <p><b>Execution Context:</b>
 *
 * <ul>
 *   <li>Pre-patch: Called by DAO when loading aspect for update (EbeanAspectDao,
 *       CassandraAspectDao)
 *   <li>Post-patch: Called in {@code SystemAspect.withVersion()} after RecordTemplate → JSON
 *       serialization
 *   <li>Runs for ALL aspect writes: REST API, GraphQL, and Kafka MCP ingestion
 *   <li>Multiple validators are called in registration order; if any validator throws, the DB write
 *       is prevented
 * </ul>
 *
 * <p><b>Use Cases:</b>
 *
 * <ul>
 *   <li>Size validation: Reject aspects exceeding configured byte limits
 *   <li>Content validation: Enforce schema rules or business constraints
 *   <li>Metrics: Track aspect sizes, write patterns, or performance
 *   <li>Audit logging: Record what aspects are being written
 * </ul>
 *
 * <p><b>Performance:</b> Both validation checkpoints reuse existing serialization:
 *
 * <ul>
 *   <li>Pre-patch: Validates JSON already fetched from database (zero additional cost)
 *   <li>Post-patch: Validates JSON already created for database write (zero additional cost)
 * </ul>
 *
 * <p><b>Naming:</b> "Validator" terminology is used for consistency with existing {@code
 * AspectPayloadValidators} in {@code
 * metadata-io/src/main/java/com/linkedin/metadata/aspect/validation/}. The term "Hook" is reserved
 * for MCL hooks and ingestion consumers in this codebase.
 *
 * <p><b>Implementation Note:</b> Validators are registered as Spring beans and auto-injected via
 * {@code @Autowired List<SystemAspectValidator>} in {@code EntityAspectDaoFactory}.
 */
public interface SystemAspectValidator {
  /**
   * Called when loading existing aspect from database before patch application.
   *
   * <p>Enables validation of pre-existing aspects before they are modified. The raw metadata is the
   * JSON string already fetched from the database - no additional serialization cost.
   *
   * <p>Validators should check if pre-patch validation is enabled in their configuration and return
   * early if disabled. If validation fails, throw a RuntimeException to prevent the write.
   *
   * @param rawMetadata serialized aspect JSON from database (may be null for new aspects)
   * @param urn entity URN
   * @param aspectName aspect name
   * @param operationContext optional operation context (may contain flags like remediation
   *     deletion)
   * @throws RuntimeException to reject the aspect write (prevents DB persistence)
   */
  default void validatePrePatch(
      @Nullable String rawMetadata,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nullable Object operationContext) {
    // Default implementation: no-op (validators can choose to implement pre-patch validation)
  }

  /**
   * Called after aspect serialization but before database write (post-patch validation).
   *
   * <p>Enables validation of the final aspect after all patches have been applied. The serialized
   * aspect is the JSON string already created for database write - no additional serialization
   * cost.
   *
   * @param systemAspect original aspect with context (URN, aspect spec, RecordTemplate)
   * @param serializedAspect serialized EntityAspect containing JSON metadata string (via {@code
   *     getMetadata()})
   * @throws RuntimeException to reject the aspect write (prevents DB persistence)
   */
  void validatePayload(@Nonnull SystemAspect systemAspect, @Nonnull EntityAspect serializedAspect);
}

package com.linkedin.metadata.aspect;

import javax.annotation.Nonnull;

/**
 * Validator invoked after an aspect has been serialized to JSON but before it is written to the
 * database. Enables validation, metrics collection, or other processing without duplicate
 * serialization overhead.
 *
 * <p><b>Execution Context:</b>
 *
 * <ul>
 *   <li>Called in {@code SystemAspect.withVersion()} after RecordTemplate → JSON serialization
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
 * <p><b>Performance:</b> Aspects MUST be serialized to JSON before database writes - this is a
 * required step, not optional. The JSON string in {@code serializedAspect} is this already-created
 * serialization, so validators can inspect it at zero additional cost. Since JSON serialization is
 * one of the most expensive operations in MCP processing, performing a second serialization just
 * for validation would effectively double this cost. The validator pattern makes validation
 * essentially free by reusing the required serialization.
 *
 * <p><b>Naming:</b> "Validator" terminology is used for consistency with existing {@code
 * AspectPayloadValidators} in {@code
 * metadata-io/src/main/java/com/linkedin/metadata/aspect/validation/}. The term "Hook" is reserved
 * for MCL hooks and ingestion consumers in this codebase.
 *
 * <p><b>Implementation Note:</b> Validators are registered as Spring beans and auto-injected via
 * {@code @Autowired List<AspectPayloadValidator>} in {@code EntityAspectDaoFactory}.
 */
public interface AspectPayloadValidator {
  /**
   * Called after aspect serialization but before database write.
   *
   * @param systemAspect original aspect with context (URN, aspect spec, RecordTemplate)
   * @param serializedAspect serialized EntityAspect containing JSON metadata string (via {@code
   *     getMetadata()})
   * @throws RuntimeException to reject the aspect write (prevents DB persistence)
   */
  void validatePayload(@Nonnull SystemAspect systemAspect, @Nonnull EntityAspect serializedAspect);
}

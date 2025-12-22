package com.linkedin.metadata.aspect;

import javax.annotation.Nonnull;

/**
 * Hook invoked after an aspect has been serialized to JSON but before it is written to the
 * database. Enables validation, metrics collection, or other processing without duplicate
 * serialization overhead.
 *
 * <p><b>Execution Context:</b>
 *
 * <ul>
 *   <li>Called in {@code SystemAspect.withVersion()} after RecordTemplate → JSON serialization
 *   <li>Runs for ALL aspect writes: REST API, GraphQL, and Kafka MCP ingestion
 *   <li>Multiple hooks are called in registration order; if any hook throws, the DB write is
 *       prevented
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
 * serialization, so hooks can inspect it at zero additional cost. Since JSON serialization is one
 * of the most expensive operations in MCP processing, performing a second serialization just for
 * validation would effectively double this cost. The hook pattern makes validation essentially free
 * by reusing the required serialization.
 *
 * <p><b>Implementation Note:</b> Hooks are registered as Spring beans and auto-injected via
 * {@code @Autowired List<AspectSerializationHook>} in {@code EntityAspectDaoFactory}.
 */
public interface AspectSerializationHook {
  /**
   * Called after aspect serialization but before database write.
   *
   * @param systemAspect original aspect with context (URN, aspect spec, RecordTemplate)
   * @param serializedAspect serialized EntityAspect containing JSON metadata string (via {@code
   *     getMetadata()})
   * @throws RuntimeException to reject the aspect write (prevents DB persistence)
   */
  void afterSerialization(
      @Nonnull SystemAspect systemAspect, @Nonnull EntityAspect serializedAspect);
}

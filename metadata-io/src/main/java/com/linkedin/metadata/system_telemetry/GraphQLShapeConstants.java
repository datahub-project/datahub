package com.linkedin.metadata.system_telemetry;

/**
 * Central constants for GraphQL query and response shape analysis.
 *
 * <p>Separate depth limits exist for queries vs responses due to different execution contexts:
 *
 * <ul>
 *   <li><strong>Query analysis:</strong> Hot-path (executed on every request) - uses generous
 *       limits
 *   <li><strong>Response analysis:</strong> Tail-case (executed only when thresholds crossed) -
 *       uses tighter limits
 * </ul>
 *
 * <p><strong>Design Rationale:</strong>
 *
 * <ul>
 *   <li>MAX_QUERY_DEPTH (30): Supports complex federated schemas with 5+ service boundaries
 *   <li>MAX_RESPONSE_DEPTH (10): Protects against pathological recursive types in responses
 *   <li>SAMPLE_SIZE (15): Deterministic sampling captures ~90% accuracy on field estimates
 *   <li>INITIAL_SHAPE_BUILDER_CAPACITY (512): Avoids 3-4 reallocations for typical 100-500 char
 *       shapes
 *   <li>BYTES_PER_FIELD_ESTIMATE (50): Order-of-magnitude accuracy acceptable for threshold
 *       decisions
 * </ul>
 */
public final class GraphQLShapeConstants {
  private GraphQLShapeConstants() {}

  // Query shape analysis (hot-path - executed on every request)
  /** Maximum recursion depth for selection-set traversal. Supports complex federated schemas. */
  public static final int MAX_QUERY_DEPTH = 30;

  /** Maximum recursion depth for argument structure traversal. */
  public static final int MAX_QUERY_ARGUMENT_DEPTH = 10;

  /** Maximum length of normalized query shape string before truncation (4096 chars). */
  public static final int MAX_QUERY_SHAPE_LENGTH = 4096;

  // Response shape analysis (tail-case - executed when thresholds crossed)
  /** Maximum recursion depth when walking response tree. Protects against pathological cases. */
  public static final int MAX_RESPONSE_DEPTH = 10;

  /** Maximum length of normalized response shape string before truncation (2048 chars). */
  public static final int MAX_RESPONSE_SHAPE_LENGTH = 2048;

  /**
   * Number of array elements to sample for shape analysis using deterministic seeding. Captures
   * ~90% accuracy on field count estimates without approaching O(n) traversal cost.
   */
  public static final int RESPONSE_SAMPLE_SIZE = 15;

  /**
   * Initial StringBuilder capacity for normalized shape building. Preset at 512 avoids 3-4
   * reallocations for typical 100-500 char shapes, especially important for 10K+ field responses
   * where each reallocation copies all accumulated content.
   */
  public static final int INITIAL_SHAPE_BUILDER_CAPACITY = 512;

  // Heavy field tracking
  /** Maximum number of heavy fields (arrays) to track in priority queue. */
  public static final int TOP_HEAVY_FIELDS_LIMIT = 10;

  /**
   * Minimum array size threshold for a field to be considered "heavy". Single-element arrays don't
   * affect query plans (serialization size is same) and don't contribute significantly to response
   * bytes, so tracking only arrays with size > 1 focuses on genuinely large arrays.
   */
  public static final int RESPONSE_HEAVY_FIELD_MIN_SIZE = 2;

  // Instrumentation
  /**
   * Estimated bytes per field in JSON response for response size calculations. Accounts for:
   *
   * <ul>
   *   <li>Scalar fields: ~30 bytes (key + value + separators)
   *   <li>Object references: ~50 bytes (key + nested structure)
   *   <li>JSON overhead: ~15 bytes per field (commas, spaces, etc.)
   * </ul>
   *
   * Empirical median: 40-60 bytes per field. Order-of-magnitude accuracy is acceptable since
   * response size threshold is one of several checks for anomaly detection.
   */
  public static final long BYTES_PER_FIELD_ESTIMATE = 50;

  /** Sentinel value for unknown/missing top-level fields in query shapes. */
  public static final String UNKNOWN_FIELDS = "unknown";
}

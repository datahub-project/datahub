package com.linkedin.metadata.system_telemetry;

import graphql.ExecutionResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Stateless utility that analyses a GraphQL {@link ExecutionResult} and produces a {@link
 * ResponseShape} describing its structure.
 *
 * <p>Only called when at least one request threshold has already been crossed, so it is acceptable
 * for this analysis to be slightly more expensive than {@link QueryShapeAnalyzer}.
 *
 * <p>All methods are static and thread-safe. Exceptions are caught and handled gracefully — this
 * class never propagates analysis errors, ensuring that shape logging failures do not interrupt
 * request processing.
 */
public final class ResponseShapeAnalyzer {

  /** Path separator for nested field tracking. */
  private static final String PATH_SEPARATOR = ".";

  private ResponseShapeAnalyzer() {}

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Represents a "heavy field" — an array in the response with size > 1. Extracted during AST
   * traversal and sorted by size descending.
   */
  public record HeavyField(@Nonnull String path, int size) {}

  /**
   * Immutable result of a response shape analysis.
   *
   * <ul>
   *   <li>{@code normalizedShape} — string representation of the response data structure, capped at
   *       {@value GraphQLShapeConstants#MAX_RESPONSE_SHAPE_LENGTH} chars.
   *   <li>{@code fieldCount} — total number of scalar (leaf) values in the response.
   *   <li>{@code heavyFields} — list of heavy fields (arrays with size > 1), extracted during AST
   *       traversal. Sorted by size descending, capped at top 10 arrays.
   * </ul>
   */
  @Value
  public static class ResponseShape {
    @Nonnull String normalizedShape;
    int fieldCount;
    @Nonnull List<HeavyField> heavyFields;
  }

  /**
   * Analyse the data portion of the given {@link ExecutionResult}.
   *
   * <p>Returns a shape with empty string and zero counts when {@code result.getData()} is null.
   * Exceptions during analysis are caught and logged, returning best-effort results rather than
   * propagating errors. This ensures that shape logging failures do not interrupt request
   * processing.
   *
   * @param result execution result (non-null)
   * @return {@link ResponseShape} describing the response structure
   */
  @Nonnull
  public static ResponseShape analyze(@Nonnull final ExecutionResult result) {
    Object data = result.getData();
    if (data == null) {
      return new ResponseShape("(null)", 0, List.of());
    }

    try {
      int[] counts = {0}; // [fieldCount]
      // Use PriorityQueue to keep top 10 arrays by size (min-heap pattern)
      // Capacity: TOP_HEAVY_FIELDS_LIMIT + 1 to force eviction of smallest when full
      PriorityQueue<HeavyField> heavyFieldsQueue =
          new PriorityQueue<>(
              GraphQLShapeConstants.TOP_HEAVY_FIELDS_LIMIT + 1,
              (a, b) -> Integer.compare(a.size(), b.size()));

      StringBuilder sb = new StringBuilder(GraphQLShapeConstants.INITIAL_SHAPE_BUILDER_CAPACITY);
      appendValue(sb, data, 0, counts, "", heavyFieldsQueue);

      // Extract top N: drain min-heap via poll() (gives ascending), then reverse for descending
      List<HeavyField> heavyFields = new ArrayList<>(heavyFieldsQueue.size());
      while (!heavyFieldsQueue.isEmpty()) {
        heavyFields.add(heavyFieldsQueue.poll());
      }
      Collections.reverse(heavyFields); // Now in descending order by size

      String shape = sb.toString();
      if (shape.length() > GraphQLShapeConstants.MAX_RESPONSE_SHAPE_LENGTH) {
        shape = shape.substring(0, GraphQLShapeConstants.MAX_RESPONSE_SHAPE_LENGTH) + "...";
      }

      return new ResponseShape(shape, counts[0], heavyFields);
    } catch (Exception e) {
      return new ResponseShape("(error: " + e.getClass().getSimpleName() + ")", 0, List.of());
    }
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  /** Appends '.' before recursing into a Map value (e.g., {@code [64].entity}). */
  private static void appendDotNotationIfMap(
      @Nonnull final StringBuilder sb, @Nullable final Object value) {
    if (value instanceof Map) {
      ShapeFormatter.appendDotNotation(sb);
    }
  }

  /**
   * Recursively appends a normalized representation of {@code value} to {@code sb}.
   *
   * @param sb builder receiving the shape
   * @param value the value to inspect (Map, List, or scalar)
   * @param depth current depth (0 at root data map)
   * @param counts accumulator: {@code [fieldCount]}
   * @param path current path in response tree (e.g., "entities" or "entities.owner")
   * @param heavyFieldsQueue priority queue maintaining top 10 arrays by size
   */
  @SuppressWarnings("unchecked")
  private static void appendValue(
      @Nonnull final StringBuilder sb,
      @Nullable final Object value,
      final int depth,
      @Nonnull final int[] counts,
      @Nonnull final String path,
      @Nonnull final PriorityQueue<HeavyField> heavyFieldsQueue) {

    if (depth >= GraphQLShapeConstants.MAX_RESPONSE_DEPTH) {
      sb.append("...");
      return;
    }

    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      appendMap(sb, map, depth, counts, path, heavyFieldsQueue);

    } else if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      int size = list.size();
      ShapeFormatter.appendArraySize(sb, size);

      // Track heavy fields (arrays with > 1 element) in priority queue
      if (size >= GraphQLShapeConstants.RESPONSE_HEAVY_FIELD_MIN_SIZE && !path.isEmpty()) {
        heavyFieldsQueue.offer(new HeavyField(path, size));
        // Keep only top N arrays by size (min-heap: remove smallest)
        if (heavyFieldsQueue.size() > GraphQLShapeConstants.TOP_HEAVY_FIELDS_LIMIT) {
          heavyFieldsQueue.poll();
        }
      }

      // Sample up to SAMPLE_SIZE deterministic elements to capture shape variants (not just first
      // element).
      // Uses deterministic seeding based on path hash for reproducible shape hashing across runs.
      if (!list.isEmpty()) {
        Set<String> uniqueShapes = new LinkedHashSet<>();
        int sampleSize = Math.min(GraphQLShapeConstants.RESPONSE_SAMPLE_SIZE, list.size());

        // Use deterministic sampling based on path hash for reproducibility.
        // Same array at same path always produces same sample indices and shapes,
        // enabling effective shape deduplication via CRC32 hashing.
        long pathHash = path.isEmpty() ? 0L : path.hashCode() & 0xFFFFFFFFL;
        java.util.Random deterministicRandom = new java.util.Random(pathHash);

        // Record field count before sampling and track field counts across samples.
        // Average field count across all sampled elements for more accurate estimates
        // when array elements may have different structures.
        int fieldCountBefore = counts[0];
        int totalFieldsInSamples = 0;
        int sampledElements = 0;

        // Sample deterministic elements and collect unique shapes + field counts
        for (int i = 0; i < sampleSize; i++) {
          int randomIndex = deterministicRandom.nextInt(list.size());
          Object element = list.get(randomIndex);

          // Single traversal: collect shape and count fields
          StringBuilder elementShape = new StringBuilder();
          int countsBefore = counts[0];
          appendValue(elementShape, element, depth + 1, counts, path, heavyFieldsQueue);
          String shapeStr = elementShape.toString();
          if (!shapeStr.isEmpty()) {
            uniqueShapes.add(shapeStr);
          }

          int fieldsInElement = counts[0] - countsBefore;
          totalFieldsInSamples += fieldsInElement;
          sampledElements++;
        }

        // Append combined shapes: shape1 | shape2 | shape3
        appendDotNotationIfMap(sb, list.get(0));
        if (!uniqueShapes.isEmpty()) {
          sb.append(ShapeFormatter.joinArrayShapeUnion(new ArrayList<>(uniqueShapes)));
        }

        // Estimate total field count based on average across sampled elements
        if (sampledElements > 0) {
          // Average fields per element across samples, then multiply by array size
          long avgFieldsPerElement =
              (totalFieldsInSamples + sampledElements - 1) / sampledElements; // ceiling division
          long estimated = fieldCountBefore + (avgFieldsPerElement * size);
          counts[0] = (int) Math.min(estimated, Integer.MAX_VALUE);
        }
      }

    } else {
      // Scalar leaf
      counts[0]++;
      ShapeFormatter.appendScalarMarker(sb);
    }
  }

  @SuppressWarnings("unchecked")
  private static void appendMap(
      @Nonnull final StringBuilder sb,
      @Nonnull final Map<String, Object> map,
      final int depth,
      @Nonnull final int[] counts,
      @Nonnull final String path,
      @Nonnull final PriorityQueue<HeavyField> heavyFieldsQueue) {

    ShapeFormatter.appendObjectStart(sb);
    boolean first = true;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (!first) {
        ShapeFormatter.appendFieldSeparator(sb);
      }
      first = false;
      String key = entry.getKey();
      sb.append(key);
      appendDotNotationIfMap(sb, entry.getValue());
      String newPath = path.isEmpty() ? key : path + PATH_SEPARATOR + key;
      appendValue(sb, entry.getValue(), depth + 1, counts, newPath, heavyFieldsQueue);
    }
    ShapeFormatter.appendObjectEnd(sb);
  }
}

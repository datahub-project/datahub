package com.linkedin.metadata.system_telemetry;

import static java.util.stream.Collectors.toMap;

import graphql.language.ArrayValue;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.ObjectField;
import graphql.language.ObjectValue;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Stateless utility that analyses a GraphQL {@link Document} and produces a {@link QueryShape}
 * describing its structure.
 *
 * <p>The normalized shape string strips argument values, aliases, and variable references so that
 * semantically identical queries produce the same shape and hash regardless of variable values or
 * client-chosen alias names.
 *
 * <p>All methods are static and thread-safe. Exceptions must be caught by the caller — this class
 * never propagates analysis errors.
 */
public final class QueryShapeAnalyzer {

  private QueryShapeAnalyzer() {}

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Immutable result of a query shape analysis.
   *
   * <ul>
   *   <li>{@code normalizedShape} — human-readable, argument-stripped representation, capped at
   *       {@value GraphQLShapeConstants#MAX_QUERY_SHAPE_LENGTH} chars.
   *   <li>{@code shapeHash} — 8-character lowercase hex CRC32 of the normalized shape.
   *       <p><strong>CRC32 Collision Risk:</strong> 32-bit hash → ~11% collision probability at 1M
   *       distinct shapes (birthday paradox: collisions ≈ sqrt(2^32) ≈ 65K shapes). DataHub
   *       deployments typically see 100–5K distinct query shapes (10% collision rate at 5K), making
   *       CRC32 acceptable for:
   *       <ul>
   *         <li>Metrics aggregation (collisions just reduce cardinality, don't break logic)
   *         <li>Query deduplication (collisions cause minor shape log duplicates, not data loss)
   *         <li>Performance is critical (CRC32 is ~10x faster than SHA-256)
   *       </ul>
   *       If deployment reaches 1M+ distinct shapes, consider upgrading to SHA-256 and versioning
   *       the hash format.
   *   <li>{@code fieldCount} — total number of field selections across the entire query.
   *   <li>{@code maxDepth} — maximum nesting depth of field selections.
   *   <li>{@code operationType} — {@code "query"}, {@code "mutation"}, or {@code "subscription"}.
   *   <li>{@code topLevelFieldNames} — extracted top-level field names from the operation's
   *       selection set (e.g., `["search", "user", "posts"]`).
   * </ul>
   */
  public static class QueryShape {
    @Getter @Nonnull private final String normalizedShape;
    @Getter @Nonnull private final String shapeHash;
    @Getter private final int fieldCount;
    @Getter private final int maxDepth;
    @Getter @Nonnull private final String operationType;
    @Nonnull private final List<String> topLevelFieldNames;
    @Nonnull private final String topLevelFieldsString;

    public QueryShape(
        @Nonnull final String normalizedShape,
        @Nonnull final String shapeHash,
        final int fieldCount,
        final int maxDepth,
        @Nonnull final String operationType,
        @Nonnull final List<String> topLevelFieldNames) {
      this.normalizedShape = normalizedShape;
      this.shapeHash = shapeHash;
      this.fieldCount = fieldCount;
      this.maxDepth = maxDepth;
      this.operationType = operationType;
      this.topLevelFieldNames = topLevelFieldNames;
      // Compute and cache sorted comma-joined string once at construction
      this.topLevelFieldsString = computeTopLevelFieldsString(topLevelFieldNames);
    }

    /** Computes sorted comma-joined string of top-level field names. */
    @Nonnull
    private static String computeTopLevelFieldsString(@Nonnull final List<String> fieldNames) {
      if (fieldNames.isEmpty()) {
        return GraphQLShapeConstants.UNKNOWN_FIELDS;
      }
      // Sort to ensure deterministic tag values — prevents cardinality explosion in metrics
      // when same fields are queried in different order (e.g., "search,user" vs "user,search")
      List<String> sorted = new ArrayList<>(fieldNames);
      sorted.sort(null); // natural order
      return String.join(",", sorted);
    }

    /**
     * Returns a comma-joined string of top-level field names suitable for use as a Micrometer
     * metric tag.
     *
     * <p>Example: {@code "search,user,posts"}
     *
     * <p>This keeps metric tag cardinality bounded (~hundreds of distinct GraphQL root fields) as
     * opposed to using the shape hash which has millions of possible values.
     *
     * @return comma-joined field names, or {@link GraphQLShapeConstants#UNKNOWN_FIELDS} if the
     *     query has no top-level fields
     */
    @Nonnull
    public String getTopLevelFields() {
      return topLevelFieldsString;
    }
  }

  /**
   * Analyse the first {@link OperationDefinition} in {@code document} that matches {@code
   * operationName}. If {@code operationName} is {@code null} or blank the first OperationDefinition
   * is used.
   *
   * @param document parsed GraphQL document (non-null)
   * @param operationName optional operation name hint
   * @return {@link QueryShape} describing the query structure
   */
  @Nonnull
  public static QueryShape analyze(
      @Nonnull final Document document, @Nullable final String operationName) {

    Map<String, FragmentDefinition> fragments =
        document.getDefinitionsOfType(FragmentDefinition.class).stream()
            .collect(toMap(FragmentDefinition::getName, f -> f, (a, b) -> a));

    OperationDefinition operation = findOperation(document, operationName);
    if (operation == null) {
      return new QueryShape("(no operation)", "00000000", 0, 0, "query", List.of());
    }

    String opType = resolveOperationType(operation);
    int[] counts = {0, 0}; // [fieldCount, maxDepth]
    StringBuilder sb = new StringBuilder(GraphQLShapeConstants.INITIAL_SHAPE_BUILDER_CAPACITY);

    appendSelectionSet(sb, operation.getSelectionSet(), fragments, new HashSet<>(), 0, counts);

    String shape = sb.toString();
    if (shape.length() > GraphQLShapeConstants.MAX_QUERY_SHAPE_LENGTH) {
      shape = shape.substring(0, GraphQLShapeConstants.MAX_QUERY_SHAPE_LENGTH) + "...";
    }

    // Extract top-level field names directly from AST (cheap, no regex)
    List<String> topLevelFields = extractTopLevelFields(operation.getSelectionSet());

    return new QueryShape(shape, crc32Hex(shape), counts[0], counts[1], opType, topLevelFields);
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  /**
   * Extracts top-level field names from an operation's selection set using direct AST traversal.
   *
   * <p>This is a cheap, regex-free extraction that walks the SelectionSet and collects Field names.
   * Skips InlineFragment and FragmentSpread selections.
   *
   * @param selectionSet the operation's selection set (may be null)
   * @return list of top-level field names (e.g., {@code ["search", "user", "posts"]}); empty if
   *     selectionSet is null or empty
   */
  @Nonnull
  private static List<String> extractTopLevelFields(@Nullable final SelectionSet selectionSet) {
    if (selectionSet == null || selectionSet.getSelections().isEmpty()) {
      return List.of();
    }

    List<String> fieldNames = new ArrayList<>();
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field) {
        Field field = (Field) selection;
        fieldNames.add(field.getName());
      }
      // Skip InlineFragment, FragmentSpread, etc. — only interested in direct fields
    }
    return fieldNames;
  }

  @Nullable
  private static OperationDefinition findOperation(
      @Nonnull final Document document, @Nullable final String operationName) {
    for (OperationDefinition op : document.getDefinitionsOfType(OperationDefinition.class)) {
      if (operationName == null || operationName.isBlank() || operationName.equals(op.getName())) {
        return op;
      }
    }
    // Fallback: return first operation
    return document.getDefinitionsOfType(OperationDefinition.class).stream()
        .findFirst()
        .orElse(null);
  }

  @Nonnull
  private static String resolveOperationType(@Nonnull final OperationDefinition op) {
    if (op.getOperation() == null) {
      return "query";
    }
    switch (op.getOperation()) {
      case MUTATION:
        return "mutation";
      case SUBSCRIPTION:
        return "subscription";
      default:
        return "query";
    }
  }

  /**
   * Recursively walks a {@link SelectionSet} and appends the normalized shape to {@code sb}.
   *
   * @param sb string builder receiving the shape
   * @param selectionSet may be null (leaf field)
   * @param fragments fragment definitions keyed by name
   * @param visitedFragments guard set to prevent infinite recursion
   * @param depth current nesting depth (0-based at root)
   * @param counts accumulator: {@code counts[0]} = total fields, {@code counts[1]} = max depth
   */
  private static void appendSelectionSet(
      @Nonnull final StringBuilder sb,
      @Nullable final SelectionSet selectionSet,
      @Nonnull final Map<String, FragmentDefinition> fragments,
      @Nonnull final Set<String> visitedFragments,
      final int depth,
      @Nonnull final int[] counts) {

    if (depth > GraphQLShapeConstants.MAX_QUERY_DEPTH) {
      ShapeFormatter.appendDepthTruncation(sb, GraphQLShapeConstants.MAX_QUERY_DEPTH);
      return;
    }

    if (selectionSet == null || selectionSet.getSelections().isEmpty()) {
      return;
    }

    ShapeFormatter.appendObjectStart(sb);
    boolean first = true;
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (!first) {
        ShapeFormatter.appendFieldSeparator(sb);
      }
      first = false;

      if (selection instanceof Field) {
        Field field = (Field) selection;
        counts[0]++;
        int newDepth = depth + 1;
        if (newDepth > counts[1]) {
          counts[1] = newDepth;
        }
        // Use the field name (strip alias)
        sb.append(field.getName());
        // Append argument structure (field names and cardinalities) without values
        if (!field.getArguments().isEmpty()) {
          ShapeFormatter.appendArgumentsStart(sb);
          boolean firstArg = true;
          for (graphql.language.Argument arg : field.getArguments()) {
            if (!firstArg) {
              ShapeFormatter.appendArgumentSeparator(sb);
            }
            firstArg = false;
            ShapeFormatter.appendArgumentNameValue(sb, arg.getName());
            appendArgumentStructure(sb, arg.getValue());
          }
          ShapeFormatter.appendArgumentsEnd(sb);
        }
        appendSelectionSet(
            sb, field.getSelectionSet(), fragments, visitedFragments, newDepth, counts);

      } else if (selection instanceof FragmentSpread) {
        FragmentSpread spread = (FragmentSpread) selection;
        String name = spread.getName();
        if (!visitedFragments.contains(name)) {
          visitedFragments.add(name);
          FragmentDefinition def = fragments.get(name);
          if (def != null) {
            appendSelectionSet(
                sb, def.getSelectionSet(), fragments, visitedFragments, depth + 1, counts);
          }
          // Note: do NOT remove from visitedFragments — keep it to guard cycles
        }

      } else if (selection instanceof InlineFragment) {
        InlineFragment inline = (InlineFragment) selection;
        // Inline fragment type: append just the type name (e.g., Dataset, Dashboard)
        if (inline.getTypeCondition() != null) {
          sb.append(inline.getTypeCondition().getName());
        } else {
          ShapeFormatter.appendAnonymousFragment(sb);
        }
        appendSelectionSet(
            sb, inline.getSelectionSet(), fragments, visitedFragments, depth + 1, counts);
      }
    }
    ShapeFormatter.appendObjectEnd(sb);
  }

  /**
   * Appends the structure of a GraphQL argument value without including actual values.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code ObjectValue {field1: scalar, field2: [...], nested: {...}}} → {@code {field1
   *       field2[N] nested}}
   *   <li>{@code ArrayValue [item1, item2, item3]} → {@code [3]{...first element structure...}}
   *   <li>Scalar value → {@code {}}
   * </ul>
   *
   * @param sb string builder receiving the structure
   * @param value the argument value to inspect (may be null)
   */
  private static void appendArgumentStructure(
      @Nonnull final StringBuilder sb, @Nullable final Object value) {
    appendArgumentStructure(sb, value, 0);
  }

  /**
   * Helper that adds depth tracking to prevent stack overflow on deeply nested arguments.
   *
   * @param sb string builder receiving the structure
   * @param value the argument value to inspect (may be null)
   * @param depth current recursion depth
   */
  private static void appendArgumentStructure(
      @Nonnull final StringBuilder sb, @Nullable final Object value, final int depth) {
    if (depth > GraphQLShapeConstants.MAX_QUERY_ARGUMENT_DEPTH) {
      sb.append("...");
      return;
    }

    if (value == null) {
      ShapeFormatter.appendObjectStart(sb);
      ShapeFormatter.appendObjectEnd(sb);
      return;
    }

    if (value instanceof ObjectValue) {
      ObjectValue obj = (ObjectValue) value;
      ShapeFormatter.appendObjectStart(sb);
      boolean first = true;
      for (ObjectField field : obj.getObjectFields()) {
        if (!first) {
          ShapeFormatter.appendFieldSeparator(sb);
        }
        first = false;
        sb.append(field.getName());
        // Only recurse into complex types (ObjectValue or ArrayValue)
        // For scalars, just show the field name without {}
        Object fieldVal = field.getValue();
        if (fieldVal instanceof ObjectValue || fieldVal instanceof ArrayValue) {
          appendArgumentStructure(sb, fieldVal, depth + 1);
        }
      }
      ShapeFormatter.appendObjectEnd(sb);

    } else if (value instanceof ArrayValue) {
      ArrayValue arr = (ArrayValue) value;
      ShapeFormatter.appendArraySize(sb, arr.getValues().size());
      // Show structure of first array element to understand what's inside
      if (!arr.getValues().isEmpty()) {
        appendArgumentStructure(sb, arr.getValues().get(0), depth + 1);
      } else {
        ShapeFormatter.appendObjectStart(sb);
        ShapeFormatter.appendObjectEnd(sb);
      }

    } else {
      // Scalar value (String, Int, Boolean, etc.) — omit actual value
      ShapeFormatter.appendObjectStart(sb);
      ShapeFormatter.appendObjectEnd(sb);
    }
  }

  /**
   * Computes a CRC32 checksum of {@code input} and returns it as an 8-character lowercase hex
   * string.
   */
  @Nonnull
  private static String crc32Hex(@Nonnull final String input) {
    CRC32 crc = new CRC32();
    crc.update(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return String.format("%08x", crc.getValue());
  }
}

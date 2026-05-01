package com.linkedin.metadata.system_telemetry;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Centralized formatter for GraphQL shape string generation.
 *
 * <p>Extracts all magic string literals and formatting rules for query and response shape analysis
 * into a single, well-documented utility. This eliminates scattered hardcoded strings and makes
 * formatting rules explicit and auditable.
 *
 * <p>Used by {@link QueryShapeAnalyzer} and {@link ResponseShapeAnalyzer} to maintain consistent
 * shape formatting across all traversal contexts.
 */
public final class ShapeFormatter {
  private ShapeFormatter() {}

  // -------------------------------------------------------------------------
  // Object/Map formatting
  // -------------------------------------------------------------------------

  private static final char OBJECT_START = '{';
  private static final char OBJECT_END = '}';
  private static final char FIELD_SEPARATOR = ' ';

  /**
   * Appends the opening brace for an object/map shape representation.
   *
   * @param sb string builder to append to
   */
  public static void appendObjectStart(@Nonnull final StringBuilder sb) {
    sb.append(OBJECT_START);
  }

  /**
   * Appends the closing brace for an object/map shape representation.
   *
   * @param sb string builder to append to
   */
  public static void appendObjectEnd(@Nonnull final StringBuilder sb) {
    sb.append(OBJECT_END);
  }

  /**
   * Appends a field separator (space) between fields in an object shape.
   *
   * @param sb string builder to append to
   */
  public static void appendFieldSeparator(@Nonnull final StringBuilder sb) {
    sb.append(FIELD_SEPARATOR);
  }

  // -------------------------------------------------------------------------
  // Array formatting
  // -------------------------------------------------------------------------

  private static final String ARRAY_SIZE_FORMAT = "[%d]";
  private static final String ARRAY_WILDCARD = "[*]";
  private static final String ARRAY_UNION_SEPARATOR = " | ";

  /**
   * Appends an array size representation (e.g., {@code [42]}).
   *
   * @param sb string builder to append to
   * @param size actual size of the array
   */
  public static void appendArraySize(@Nonnull final StringBuilder sb, final int size) {
    sb.append('[').append(size).append(']');
  }

  /**
   * Appends a wildcard array placeholder (e.g., {@code [*]}) for truncated/sampled arrays.
   *
   * @param sb string builder to append to
   */
  public static void appendArrayWildcard(@Nonnull final StringBuilder sb) {
    sb.append(ARRAY_WILDCARD);
  }

  /**
   * Joins multiple array element shapes with union separator (e.g., {@code shape1 | shape2 |
   * shape3}).
   *
   * @param shapes list of shape strings to union
   * @return joined union string, or empty string if list is empty
   */
  @Nonnull
  public static String joinArrayShapeUnion(@Nonnull final List<String> shapes) {
    return shapes.stream().collect(Collectors.joining(ARRAY_UNION_SEPARATOR));
  }

  // -------------------------------------------------------------------------
  // Scalar and special values
  // -------------------------------------------------------------------------

  private static final char SCALAR_MARKER = '_';
  private static final String NULL_VALUE = "(null)";
  private static final String ERROR_FORMAT = "(error: %s)";
  private static final String ANONYMOUS_FRAGMENT = "*";
  private static final String DOT_NOTATION = ".";

  /**
   * Appends a scalar leaf marker (underscore) to represent a scalar field.
   *
   * @param sb string builder to append to
   */
  public static void appendScalarMarker(@Nonnull final StringBuilder sb) {
    sb.append(SCALAR_MARKER);
  }

  /**
   * Appends a null value marker.
   *
   * @param sb string builder to append to
   */
  public static void appendNullValue(@Nonnull final StringBuilder sb) {
    sb.append(NULL_VALUE);
  }

  /**
   * Appends an error marker with exception class name.
   *
   * @param sb string builder to append to
   * @param exceptionClassName simple name of the exception class
   */
  public static void appendErrorMarker(
      @Nonnull final StringBuilder sb, @Nonnull final String exceptionClassName) {
    sb.append(String.format(ERROR_FORMAT, exceptionClassName));
  }

  /**
   * Appends a marker for an anonymous inline fragment (rare case where type condition is null).
   *
   * @param sb string builder to append to
   */
  public static void appendAnonymousFragment(@Nonnull final StringBuilder sb) {
    sb.append(ANONYMOUS_FRAGMENT);
  }

  /**
   * Appends dot notation before a field name (used for nested objects in response analysis).
   *
   * @param sb string builder to append to
   */
  public static void appendDotNotation(@Nonnull final StringBuilder sb) {
    sb.append(DOT_NOTATION);
  }

  // -------------------------------------------------------------------------
  // Arguments (query shape analysis)
  // -------------------------------------------------------------------------

  private static final char ARGS_START = '(';
  private static final char ARGS_END = ')';
  private static final String ARGS_SEPARATOR = ", ";
  private static final char ARGS_NAME_VALUE_SEP = ':';
  private static final char SPACE = ' ';

  /**
   * Appends an opening parenthesis for argument list.
   *
   * @param sb string builder to append to
   */
  public static void appendArgumentsStart(@Nonnull final StringBuilder sb) {
    sb.append(ARGS_START);
  }

  /**
   * Appends a closing parenthesis for argument list.
   *
   * @param sb string builder to append to
   */
  public static void appendArgumentsEnd(@Nonnull final StringBuilder sb) {
    sb.append(ARGS_END);
  }

  /**
   * Appends a separator between arguments in an argument list.
   *
   * @param sb string builder to append to
   */
  public static void appendArgumentSeparator(@Nonnull final StringBuilder sb) {
    sb.append(ARGS_SEPARATOR);
  }

  /**
   * Appends an argument name:value structure (e.g., {@code limit: }).
   *
   * @param sb string builder to append to
   * @param argName name of the argument
   */
  public static void appendArgumentNameValue(
      @Nonnull final StringBuilder sb, @Nonnull final String argName) {
    sb.append(argName).append(ARGS_NAME_VALUE_SEP).append(SPACE);
  }

  // -------------------------------------------------------------------------
  // Depth truncation (query shape analysis)
  // -------------------------------------------------------------------------

  private static final String TRUNCATION_PREFIX = "...truncated at depth ";

  /**
   * Appends a truncation marker when max query depth is exceeded.
   *
   * @param sb string builder to append to
   * @param maxDepth the maximum depth that was exceeded
   */
  public static void appendDepthTruncation(@Nonnull final StringBuilder sb, final int maxDepth) {
    sb.append(TRUNCATION_PREFIX).append(maxDepth);
  }
}

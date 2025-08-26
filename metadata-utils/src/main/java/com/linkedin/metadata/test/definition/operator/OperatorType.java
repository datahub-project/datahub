package com.linkedin.metadata.test.definition.operator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Tops of operator that the framework supports. */
public enum OperatorType {
  /** Compare two lists to find any matching items */
  ANY_EQUALS("equals"),
  /** A value equals another value exactly. */
  STARTS_WITH("starts_with"),
  /** A string contains a specific substring, or a list contains a specific element */
  CONTAINS_STR("contains_str"),
  /** A list contains any items */
  CONTAINS_ANY("contains_any"),
  /** A string matches a specific regex */
  REGEX_MATCH("regex_match"),
  /** Greater than a value */
  GREATER_THAN("greater_than"),
  /** Less than a value */
  LESS_THAN("less_than"),
  /** Whether an attribute exists at all */
  EXISTS("exists"),
  /** Whether something is true */
  IS_TRUE("is_true"),
  /** Whether something is false */
  IS_FALSE("is_false"),
  /** Or of two things */
  OR("or"),
  /** And of two things */
  AND("and"),
  /** Inverse of an expression */
  NOT("not"),
  /** A query operator */
  QUERY("query"),
  /** Special case operator that validates that ALL schema fields have a description */
  SCHEMA_FIELDS_HAVE_DESCRIPTIONS("schema_fields_have_descriptions");

  private static final Map<String, OperatorType> NAME_TO_OPERATOR =
      Arrays.stream(OperatorType.values())
          .collect(Collectors.toMap(val -> val.commonName.toLowerCase(), val -> val));
  private final String commonName;

  OperatorType(final String commonName) {
    this.commonName = commonName;
  }

  /**
   * Retrieves an {@link OperatorType} by a case-insensitive common name, or throws {@link
   * IllegalArgumentException} if it cannot be found.
   */
  public static OperatorType fromCommonName(@Nonnull final String commonName) {
    String lowerName = commonName.toLowerCase();
    if (NAME_TO_OPERATOR.containsKey(lowerName)) {
      return NAME_TO_OPERATOR.get(lowerName);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported operator type with name %s provided", commonName));
  }
}

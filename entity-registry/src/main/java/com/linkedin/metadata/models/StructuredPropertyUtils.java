package com.linkedin.metadata.models;

import com.linkedin.structured.PrimitivePropertyValue;
import java.sql.Date;
import java.time.format.DateTimeParseException;

public class StructuredPropertyUtils {

  private StructuredPropertyUtils() {}

  static final Date MIN_DATE = Date.valueOf("1000-01-01");
  static final Date MAX_DATE = Date.valueOf("9999-12-31");

  /**
   * Sanitizes fully qualified name for use in an ElasticSearch field name Replaces . and " "
   * characters
   *
   * @param fullyQualifiedName The original fully qualified name of the property
   * @return The sanitized version that can be used as a field name
   */
  public static String sanitizeStructuredPropertyFQN(String fullyQualifiedName) {
    String sanitizedName = fullyQualifiedName.replace('.', '_').replace(' ', '_');
    return sanitizedName;
  }

  public static Date toDate(PrimitivePropertyValue value) throws DateTimeParseException {
    return Date.valueOf(value.getString());
  }

  public static boolean isValidDate(PrimitivePropertyValue value) {
    if (value.getString() == null) {
      return false;
    }
    if (value.getString().length() != 10) {
      return false;
    }
    Date date;
    try {
      date = toDate(value);
    } catch (DateTimeParseException e) {
      return false;
    }
    return date.compareTo(MIN_DATE) >= 0 && date.compareTo(MAX_DATE) <= 0;
  }
}

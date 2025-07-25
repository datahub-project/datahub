package com.datahub.gms.servlet;

import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * Defines a rule for exposing a configuration section via the /config endpoint.
 *
 * <p>This class supports: - Including/excluding entire sections - Allowing specific fields within a
 * section (allowlist approach) - Renaming sections in the output - Secure-by-default behavior (only
 * explicitly allowed fields are exposed)
 */
public class ConfigSectionRule {
  /**
   * -- GETTER --
   *
   * @return The path to the configuration section in the ConfigurationProvider
   */
  @Getter private final String sectionPath;

  /**
   * -- GETTER --
   *
   * @return The path to use in the output JSON (may be different from sectionPath for renaming)
   */
  @Getter private final String outputPath;

  private final Set<String> allowedFields;

  /** -- GETTER -- true if this section should be included in the output */
  @Getter private final boolean includeSection;

  /**
   * Creates a rule to include an entire configuration section.
   *
   * @param sectionPath The path to the configuration section (e.g., "datahub", "authentication")
   */
  public static ConfigSectionRule include(String sectionPath) {
    return new ConfigSectionRule(sectionPath, sectionPath, null, true);
  }

  /**
   * Creates a rule to include a configuration section with only specified fields.
   *
   * @param sectionPath The path to the configuration section
   * @param allowedFields Set of field names to include (null means all fields)
   */
  public static ConfigSectionRule include(String sectionPath, Set<String> allowedFields) {
    return new ConfigSectionRule(sectionPath, sectionPath, allowedFields, true);
  }

  /**
   * Creates a rule to include a configuration section with renaming.
   *
   * @param sectionPath The path to the configuration section
   * @param outputPath The name to use in the output (for renaming)
   * @param allowedFields Set of field names to include (null means all fields)
   */
  public static ConfigSectionRule include(
      String sectionPath, String outputPath, Set<String> allowedFields) {
    return new ConfigSectionRule(sectionPath, outputPath, allowedFields, true);
  }

  /**
   * Creates a rule to exclude a configuration section entirely.
   *
   * @param sectionPath The path to the configuration section to exclude
   */
  public static ConfigSectionRule exclude(String sectionPath) {
    return new ConfigSectionRule(sectionPath, sectionPath, null, false);
  }

  private ConfigSectionRule(
      String sectionPath, String outputPath, Set<String> allowedFields, boolean includeSection) {
    this.sectionPath = sectionPath;
    this.outputPath = outputPath;
    this.allowedFields = allowedFields;
    this.includeSection = includeSection;
  }

  /**
   * @return Set of allowed field names, or null if all fields are allowed
   */
  @Nullable
  public Set<String> getAllowedFields() {
    return allowedFields;
  }

  /**
   * @return true if all fields in the section are allowed (no field-level filtering)
   */
  public boolean isAllFieldsAllowed() {
    return allowedFields == null;
  }

  /**
   * Checks if a specific field is allowed by this rule.
   *
   * @param fieldName The name of the field to check
   * @return true if the field is allowed
   */
  public boolean isFieldAllowed(String fieldName) {
    if (!includeSection) {
      return false;
    }
    if (allowedFields == null) {
      return true; // All fields allowed
    }
    return allowedFields.contains(fieldName);
  }

  /**
   * Gets all allowed field paths that start with the given prefix. This is used for nested path
   * processing.
   *
   * @param pathPrefix The path prefix to match (e.g., "tokenService")
   * @return Set of allowed paths that start with the prefix
   */
  public Set<String> getAllowedPathsWithPrefix(String pathPrefix) {
    if (!includeSection || allowedFields == null) {
      return Set.of();
    }

    String prefix = pathPrefix + ".";
    return allowedFields.stream()
        .filter(path -> path.startsWith(prefix))
        .collect(java.util.stream.Collectors.toSet());
  }

  /**
   * Gets all allowed top-level field names (fields without dots).
   *
   * @return Set of top-level field names
   */
  public Set<String> getTopLevelFields() {
    if (!includeSection || allowedFields == null) {
      return Set.of();
    }

    return allowedFields.stream()
        .filter(path -> !path.contains("."))
        .collect(java.util.stream.Collectors.toSet());
  }

  /**
   * Checks if any nested paths are allowed for the given field name.
   *
   * @param fieldName The top-level field name to check
   * @return true if there are nested paths allowed for this field
   */
  public boolean hasNestedPathsForField(String fieldName) {
    if (!includeSection || allowedFields == null) {
      return false;
    }

    String prefix = fieldName + ".";
    return allowedFields.stream().anyMatch(path -> path.startsWith(prefix));
  }

  @Override
  public String toString() {
    return "ConfigSectionRule{"
        + "sectionPath='"
        + sectionPath
        + '\''
        + ", outputPath='"
        + outputPath
        + '\''
        + ", allowedFields="
        + allowedFields
        + ", includeSection="
        + includeSection
        + '}';
  }
}

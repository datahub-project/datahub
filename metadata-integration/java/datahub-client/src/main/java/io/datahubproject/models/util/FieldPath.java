package io.datahubproject.models.util;

import lombok.NonNull;

public class FieldPath {
  private final String rawFieldPath;
  private final String[] segments;
  private final String version;
  private final String simplePath;

  public FieldPath(@NonNull String rawFieldPath) {
    this.rawFieldPath = rawFieldPath;
    this.segments = rawFieldPath.split("\\.");
    this.version = computeVersion();
    this.simplePath = computeSimplePath();
  }

  public boolean isTopLevel() {
    return depth() == 1;
  }

  /**
   * Returns the logical depth of the field path, ignoring type and version metadata Example:
   * "[version=2.0][type=Foo].address.[type=union].[type=string].street" -> depth = 2
   *
   * @return The logical depth of the field path
   */
  public int depth() {
    String[] segments = simplePath().split("\\.");
    return segments.length;
  }

  public String leafFieldName() {
    return segments[segments.length - 1];
  }

  /**
   * Extracts the version number from the field path. Example: rawFieldPath = "a.b.c" -> version() =
   * "1" rawFieldPath = "[version=2].a.b.c" -> version() = "2" rawFieldPath = "[version=20].a.b.c"
   * -> version() = "20"
   *
   * @return String representing the version number
   */
  private String computeVersion() {
    if (rawFieldPath == null) {
      return "1";
    }

    // Check for version pattern [version=X] where X can be any non-bracket characters
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\[version=([^\\]]+)\\]");
    java.util.regex.Matcher matcher = pattern.matcher(rawFieldPath);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return "1";
  }

  public String version() {
    return version;
  }

  /**
   * Returns the simplified path without version, type, or other metadata Example:
   * "[version=2.0][type=Foo].address.[type=union].[type=string].street" -> "address.street"
   *
   * @return The simplified field path
   */
  private String computeSimplePath() {
    if (rawFieldPath == null) {
      return "";
    }
    // Remove all metadata blocks [xxx=yyy]
    String simplified = rawFieldPath.replaceAll("\\[.*?\\]", "");
    // Replace all "double dots" with a single dot
    simplified = simplified.replaceAll("\\.+", ".");
    // Replace all leading and trailing dots
    simplified = simplified.replaceAll("^\\.|\\.$", "");
    // Remove any trailing metadata blocks without dots
    simplified = simplified.replaceAll("\\[.*?\\]", "");
    return simplified;
  }

  public String simplePath() {
    return simplePath;
  }
}

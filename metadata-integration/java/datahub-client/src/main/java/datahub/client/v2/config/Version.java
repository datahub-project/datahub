package datahub.client.v2.config;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a semantic version with support for 3-part (1.0.0) and 4-part (1.0.0.1) versions.
 *
 * <p>Handles version strings like:
 *
 * <ul>
 *   <li>"v1.0.0" → (1, 0, 0, 0)
 *   <li>"v1.0.0.1" → (1, 0, 0, 1)
 *   <li>"v1.0.0rc3" → (1, 0, 0, 0) - strips rc suffix
 *   <li>"v0.3.11" → (0, 3, 11, 0) - DataHub Cloud version
 * </ul>
 */
public class Version implements Comparable<Version> {
  private static final Pattern VERSION_PATTERN =
      Pattern.compile("^v?(\\d+)\\.(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:rc\\d+|-.*)?$");

  private final int major;
  private final int minor;
  private final int patch;
  private final int build;

  /**
   * Constructs a Version.
   *
   * @param major major version number
   * @param minor minor version number
   * @param patch patch version number
   * @param build build number (0 for 3-part versions)
   */
  public Version(int major, int minor, int patch, int build) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.build = build;
  }

  /**
   * Parses a version string into a Version object.
   *
   * @param versionString version string to parse (e.g., "v1.0.0", "1.0.0.1rc3")
   * @return parsed Version, or Version(0,0,0,0) if null/empty
   * @throws IllegalArgumentException if version string is invalid
   */
  @Nonnull
  public static Version parse(@Nullable String versionString) {
    if (versionString == null || versionString.isEmpty()) {
      return new Version(0, 0, 0, 0);
    }

    Matcher matcher = VERSION_PATTERN.matcher(versionString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid version string: " + versionString);
    }

    int major = Integer.parseInt(matcher.group(1));
    int minor = Integer.parseInt(matcher.group(2));
    int patch = Integer.parseInt(matcher.group(3));
    int build = matcher.group(4) != null ? Integer.parseInt(matcher.group(4)) : 0;

    return new Version(major, minor, patch, build);
  }

  /**
   * Checks if this version is at least the specified version.
   *
   * @param major major version to compare
   * @param minor minor version to compare
   * @param patch patch version to compare
   * @return true if this version >= specified version
   */
  public boolean isAtLeast(int major, int minor, int patch) {
    return compareTo(new Version(major, minor, patch, 0)) >= 0;
  }

  /**
   * Checks if this version is at least the specified version.
   *
   * @param major major version to compare
   * @param minor minor version to compare
   * @param patch patch version to compare
   * @param build build number to compare
   * @return true if this version >= specified version
   */
  public boolean isAtLeast(int major, int minor, int patch, int build) {
    return compareTo(new Version(major, minor, patch, build)) >= 0;
  }

  @Override
  public int compareTo(@Nonnull Version other) {
    if (major != other.major) {
      return Integer.compare(major, other.major);
    }
    if (minor != other.minor) {
      return Integer.compare(minor, other.minor);
    }
    if (patch != other.patch) {
      return Integer.compare(patch, other.patch);
    }
    return Integer.compare(build, other.build);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Version version = (Version) o;
    return major == version.major
        && minor == version.minor
        && patch == version.patch
        && build == version.build;
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, patch, build);
  }

  @Override
  public String toString() {
    if (build == 0) {
      return String.format("%d.%d.%d", major, minor, patch);
    } else {
      return String.format("%d.%d.%d.%d", major, minor, patch, build);
    }
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getPatch() {
    return patch;
  }

  public int getBuild() {
    return build;
  }
}
